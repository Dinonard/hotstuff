use consensus::{Block, EngineProxyMessage};
use mempool::MempoolMessage;
use store::Store;

use reth::providers::HeaderProvider;
use reth_basic_payload_builder::{BuildArguments, PayloadConfig};
use reth_chainspec::{ChainSpec, ChainSpecProvider, EthereumHardforks};
use reth_engine_primitives::BeaconEngineMessage;
use reth_ethereum_engine_primitives::EthPayloadAttributes;
use reth_ethereum_payload_builder::default_ethereum_payload;
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes};
use reth_node_builder::{BlockBody, EngineTypes, PayloadBuilderAttributes};
use reth_payload_builder::EthPayloadBuilderAttributes;
use reth_primitives::{Header, SealedBlock, TransactionSigned, TransactionSignedEcRecovered};
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_rpc_types_compat::engine::payload::block_to_payload_v3;
use reth_tasks::TokioTaskExecutor;
use reth_transaction_pool::blobstore::InMemoryBlobStore;
use reth_transaction_pool::{
    EthPooledTransaction, EthTransactionPool, TransactionPool, TransactionValidationTaskExecutor,
};

use alloy::{
    primitives::{Address, B256},
    rlp::{Decodable, Encodable},
    rpc::types::engine::{CancunPayloadFields, ExecutionPayloadSidecar, ForkchoiceState},
};

use std::sync::Arc;
use tokio::sync::{
    mpsc::{Receiver, UnboundedSender},
    oneshot,
};

pub struct EngineApiProxy<T, C, EC>
where
    T: EngineTypes,
{
    client: C,
    evm_config: EC,
    store: Store,
    rx_engine_proxy: Receiver<EngineProxyMessage>,
    tx_consensus: UnboundedSender<BeaconEngineMessage<T>>,
}

impl<T, C, EC> EngineApiProxy<T, C, EC>
where
    T: EngineTypes,
    C: StateProviderFactory
        + BlockReaderIdExt
        + ChainSpecProvider<ChainSpec = ChainSpec>
        + HeaderProvider<Header = Header>
        + Clone
        + 'static,
    <C as ChainSpecProvider>::ChainSpec: EthereumHardforks,
    EC: ConfigureEvm<Header = Header, Transaction = TransactionSigned> + Clone,
{
    pub fn new(
        client: C,
        evm_config: EC,
        store: Store,
        rx_engine_proxy: Receiver<EngineProxyMessage>,
        tx_consensus: UnboundedSender<BeaconEngineMessage<T>>,
    ) -> Self {
        Self {
            client,
            evm_config,
            store,
            rx_engine_proxy,
            tx_consensus,
        }
    }

    pub async fn run(&mut self) {
        // REUSE FROM ERMAL'S APPROACH
        // TODO: Revisit this part later.
        let ready_pool = EthTransactionPool::eth_pool(
            TransactionValidationTaskExecutor::eth(
                self.client.clone(),
                self.client.chain_spec().into(),
                InMemoryBlobStore::default(),
                TokioTaskExecutor::default(),
            ),
            InMemoryBlobStore::default(),
            Default::default(),
        );
        // END REUSE

        while let Some(message) = self.rx_engine_proxy.recv().await {
            match message {
                EngineProxyMessage::NewPayload(block) => {
                    let author = block.author;
                    // TODO: check how does reth decide on the block number? Is it automatically inferred from the parent block hash?
                    let round = block.round;

                    let mut transactions = Vec::<EthPooledTransaction>::new();

                    // Each `digest` refers to a batch of transactions.
                    // For some reason, each block contains multiple batches.
                    // TODO: Low prio - simplify this to a single 'batch' per block?
                    for digest in block.payload.iter() {
                        let serialized = match self.store.read(digest.to_vec()).await {
                            Ok(Some(serialized)) => serialized,
                            Ok(None) => panic!("Digest {:?} not found in DB but it was included in the block payload", digest),
                            Err(e) => panic!("Error reading digest {:?} from DB: {}", digest, e),
                        };

                        let batch = match bincode::deserialize(&serialized) {
                            Ok(MempoolMessage::Batch(batch)) => batch,
                            Ok(other) => {
                                panic!("Expected MempoolMessage::Batch but got {:?}", other)
                            }
                            Err(e) => panic!(
                                "Error deserializing MempoolMessage for digest {:?}: {}",
                                digest, e
                            ),
                        };

                        transactions.extend(batch.into_iter().map(|tx| {
                            // Hacky, but hopefully it works
                            let recovered_tx: TransactionSignedEcRecovered =
                                TransactionSignedEcRecovered::decode(&mut &tx[..])
                                    .expect("Unexpected Ethereum transaction format.");

                            let encode_len = recovered_tx.length();
                            EthPooledTransaction::new(recovered_tx, encode_len)
                        }));
                    }

                    // REUSE FROM ERMAL'S APPROACH
                    ready_pool.add_external_transactions(transactions).await;

                    let parent_header = self
                        .client
                        .latest_header()
                        .expect("header exists")
                        .expect("header exists");

                    let payload_attributes = EthPayloadAttributes {
                        // TODO: Verify later if it's fine to use round as dummy timestamp
                        timestamp: round,
                        prev_randao: B256::ZERO.into(),
                        suggested_fee_recipient: Address::from_word(author.0.into()),
                        withdrawals: Default::default(),
                        parent_beacon_block_root: Some(B256::ZERO.into()),
                        max_blobs_per_block: None,
                        target_blobs_per_block: None,
                    };

                    let attributes = EthPayloadBuilderAttributes::new(
                        parent_header.hash(),
                        payload_attributes.clone(),
                    );

                    let config = PayloadConfig {
                        attributes: attributes.clone(),
                        parent_header: Arc::new(parent_header.clone()),
                        extra_data: Default::default(),
                    };

                    let args = BuildArguments::new(
                        self.client.clone(),
                        ready_pool.clone(),
                        Default::default(),
                        config,
                        Default::default(),
                        None,
                    );

                    let next_attr = NextBlockEnvAttributes {
                        timestamp: args.config.attributes.timestamp(),
                        suggested_fee_recipient: args.config.attributes.suggested_fee_recipient,
                        prev_randao: args.config.attributes.prev_randao,
                    };

                    let (cfg_env, block_env) = self
                        .evm_config
                        .next_cfg_and_block_env(&parent_header, next_attr)
                        .unwrap();

                    let payload = default_ethereum_payload(
                        self.evm_config.clone(),
                        args,
                        cfg_env,
                        block_env,
                        |attributes| ready_pool.best_transactions_with_attributes(attributes),
                    )
                    .unwrap()
                    .into_payload()
                    .unwrap();

                    let block = payload.block();
                    let payload = block_to_payload_v3(block.clone());

                    // END REUSE

                    // Finally send the payload to the consensus engine.
                    let (tx, rx) = oneshot::channel();
                    let _ = self.tx_consensus.send(BeaconEngineMessage::NewPayload {
                        payload: payload.into(),
                        sidecar: ExecutionPayloadSidecar::v3(CancunPayloadFields {
                            parent_beacon_block_root: block
                                .parent_beacon_block_root
                                .unwrap_or_default(),
                            versioned_hashes: block.blob_versioned_hashes_iter().copied().collect(),
                        }),
                        tx,
                    });

                    // REUSE FROM ERMAL'S APPROACH
                    // TODO: Revisit this part later.
                    ready_pool.remove_transactions(
                        block.body.transactions().iter().map(|x| x.hash()).collect(),
                    );
                    let res = rx.await.unwrap().unwrap();

                    if !res.is_valid() {
                        log::error!("Invalid payload");
                    }

                    // END REUSE
                }
                EngineProxyMessage::ForkchoiceUpdated(block) => {}
            }
        }
    }
}
