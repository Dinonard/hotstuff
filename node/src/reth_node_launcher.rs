//! Abstraction for launching a node.
//!
//! This is based on the `DefaultNodeLauncher` from `reth` repository, but with the modification to allow injection
//! of the consensus tx & rx channels. This is useful since it allows the _consensus_ part of the program to use the **tx**
//! channel and send _consensus_ related messages.
//!
//! For a proper solution, it would be best if the `Node` exposes ETH Engine API directly, without the need for approaches like this.

use reth_node_builder::common::{Attached, LaunchContextWith, WithConfigs};

use std::sync::Arc;

use futures::{future::Either, stream, stream_select, StreamExt};
use reth_beacon_consensus::{
    hooks::{EngineHooks, PruneHook, StaticFileHook},
    BeaconConsensusEngine,
};
use reth_blockchain_tree::{
    externals::TreeNodeTypes, noop::NoopBlockchainTree, BlockchainTree, BlockchainTreeConfig,
    ShareableBlockchainTree, TreeExternals,
};
use reth_chainspec::EthChainSpec;
use reth_consensus_debug_client::{DebugConsensusClient, EtherscanBlockProvider, RpcBlockProvider};
use reth_engine_primitives::BeaconEngineMessage;
use reth_engine_util::EngineMessageStreamExt;
use reth_exex::ExExManagerHandle;
use reth_network::BlockDownloaderProvider;
use reth_node_api::{AddOnsContext, FullNodeTypes, NodeTypesWithEngine};
use reth_node_builder::{
    components::{NodeComponents, NodeComponentsBuilder},
    hooks::NodeHooks,
    node::FullNode,
    rpc::{RethRpcAddOns, RpcHandle},
    AddOns, ExExLauncher, LaunchContext, LaunchNode, NodeAdapter, NodeBuilderWithComponents,
    NodeHandle, NodeTypesAdapter,
};
use reth_node_core::{
    dirs::{ChainPath, DataDirPath},
    exit::NodeExitFuture,
};
use reth_node_events::{cl::ConsensusLayerHealthEvents, node};
use reth_provider::providers::{BlockchainProvider, ProviderNodeTypes};
use reth_tasks::TaskExecutor;
use reth_tracing::tracing::{debug, info};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// The default launcher for a node.
#[derive(Debug)]
pub struct DefaultNodeLauncher<Types: ProviderNodeTypes + NodeTypesWithEngine + TreeNodeTypes> {
    /// The task executor for the node.
    pub ctx: LaunchContext,
    pub tx_rx_consensus: (
        UnboundedSender<BeaconEngineMessage<Types::Engine>>,
        UnboundedReceiver<BeaconEngineMessage<Types::Engine>>,
    ),
}

impl<Types> DefaultNodeLauncher<Types>
where
    Types: ProviderNodeTypes + NodeTypesWithEngine + TreeNodeTypes,
{
    /// Create a new instance of the default node launcher.
    pub const fn new(
        task_executor: TaskExecutor,
        data_dir: ChainPath<DataDirPath>,
        tx_rx_consensus: (
            UnboundedSender<BeaconEngineMessage<Types::Engine>>,
            UnboundedReceiver<BeaconEngineMessage<Types::Engine>>,
        ),
    ) -> Self {
        Self {
            ctx: LaunchContext::new(task_executor, data_dir),
            tx_rx_consensus,
        }
    }
}

impl<Types, T, CB, AO> LaunchNode<NodeBuilderWithComponents<T, CB, AO>>
    for DefaultNodeLauncher<Types>
where
    Types: ProviderNodeTypes + NodeTypesWithEngine + TreeNodeTypes,
    T: FullNodeTypes<Provider = BlockchainProvider<Types>, Types = Types>,
    CB: NodeComponentsBuilder<T>,
    AO: RethRpcAddOns<NodeAdapter<T, CB::Components>>,
{
    type Node = NodeHandle<NodeAdapter<T, CB::Components>, AO>;

    async fn launch_node(
        self,
        target: NodeBuilderWithComponents<T, CB, AO>,
    ) -> eyre::Result<Self::Node> {
        let Self {
            ctx,
            tx_rx_consensus,
        } = self;
        let NodeBuilderWithComponents {
            adapter: NodeTypesAdapter { database },
            components_builder,
            add_ons:
                AddOns {
                    hooks,
                    exexs: installed_exex,
                    add_ons,
                },
            config,
        } = target;
        let NodeHooks {
            on_component_initialized,
            on_node_started,
            ..
        } = hooks;

        // TODO: remove tree and move tree_config and canon_state_notification_sender
        // initialization to with_blockchain_db once the engine revamp is done
        // https://github.com/paradigmxyz/reth/issues/8742
        let tree_config = BlockchainTreeConfig::default();

        // NOTE: This is a temporary workaround to provide the canon state notification sender to the components builder because there's a cyclic dependency between the blockchain provider and the tree component. This will be removed once the Blockchain provider no longer depends on an instance of the tree: <https://github.com/paradigmxyz/reth/issues/7154>
        let (canon_state_notification_sender, _receiver) =
            tokio::sync::broadcast::channel(tree_config.max_reorg_depth() as usize * 2);

        let tree = Arc::new(NoopBlockchainTree::with_canon_state_notifications(
            canon_state_notification_sender.clone(),
        ));

        // setup the launch context
        let mut ctx = ctx
            .with_configured_globals()
            // load the toml config
            .with_loaded_toml_config(config)?
            // add resolved peers
            .with_resolved_peers().await?
            // attach the database
            .attach(database.clone())
            // ensure certain settings take effect
            .with_adjusted_configs()
            // Create the provider factory
            .with_provider_factory().await?
            .inspect(|_| {
                info!(target: "reth::cli", "Database opened");
            })
            .with_prometheus_server().await?
            .inspect(|this| {
                debug!(target: "reth::cli", chain=%this.chain_id(), genesis=?this.genesis_hash(), "Initializing genesis");
            })
            .with_genesis()?
            .inspect(|this: &LaunchContextWith<Attached<WithConfigs<Types::ChainSpec>, _>>| {
                info!(target: "reth::cli", "\n{}", this.chain_spec().display_hardforks());
            })
            .with_metrics_task()
            // passing FullNodeTypes as type parameter here so that we can build
            // later the components.
            .with_blockchain_db::<T, _>(move |provider_factory| {
                Ok(BlockchainProvider::new(provider_factory, tree)?)
            })?
            .with_components(components_builder, on_component_initialized).await?;

        let consensus = Arc::new(ctx.components().consensus().clone());

        let tree_externals = TreeExternals::new(
            ctx.provider_factory().clone(),
            consensus.clone(),
            ctx.components().block_executor().clone(),
        );
        let tree = BlockchainTree::new(tree_externals, tree_config)?
            .with_sync_metrics_tx(ctx.sync_metrics_tx())
            // Note: This is required because we need to ensure that both the components and the
            // tree are using the same channel for canon state notifications. This will be removed
            // once the Blockchain provider no longer depends on an instance of the tree
            .with_canon_state_notification_sender(canon_state_notification_sender);

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

        ctx.node_adapter_mut().provider = ctx.blockchain_db().clone().with_tree(blockchain_tree);

        debug!(target: "reth::cli", "configured blockchain tree");

        // spawn exexs
        let exex_manager_handle = ExExLauncher::new(
            ctx.head(),
            ctx.node_adapter().clone(),
            installed_exex,
            ctx.configs().clone(),
        )
        .launch()
        .await?;

        // create pipeline
        let network_client = ctx.components().network().fetch_client().await?;
        let (consensus_engine_tx, consensus_engine_rx) = tx_rx_consensus;

        let node_config = ctx.node_config();
        let consensus_engine_stream = UnboundedReceiverStream::from(consensus_engine_rx)
            .maybe_skip_fcu(node_config.debug.skip_fcu)
            .maybe_skip_new_payload(node_config.debug.skip_new_payload)
            .maybe_reorg(
                ctx.blockchain_db().clone(),
                ctx.components().evm_config().clone(),
                reth_payload_validator::ExecutionPayloadValidator::new(ctx.chain_spec()),
                node_config.debug.reorg_frequency,
                node_config.debug.reorg_depth,
            )
            // Store messages _after_ skipping so that `replay-engine` command
            // would replay only the messages that were observed by the engine
            // during this run.
            .maybe_store_messages(node_config.debug.engine_api_store.clone());

        let max_block = ctx.max_block(network_client.clone()).await?;
        let mut hooks = EngineHooks::new();

        let static_file_producer = ctx.static_file_producer();
        let static_file_producer_events = static_file_producer.lock().events();
        hooks.add(StaticFileHook::new(
            static_file_producer.clone(),
            Box::new(ctx.task_executor().clone()),
        ));
        info!(target: "reth::cli", "StaticFileProducer initialized");

        // Configure the pipeline
        let pipeline_exex_handle = exex_manager_handle
            .clone()
            .unwrap_or_else(ExExManagerHandle::empty);
        let (pipeline, client) = if ctx.is_dev() {
            eyre::bail!("Dev mode is not supported for legacy engine")
        } else {
            let pipeline = reth_node_builder::setup::build_networked_pipeline(
                &ctx.toml_config().stages,
                network_client.clone(),
                consensus.clone(),
                ctx.provider_factory().clone(),
                ctx.task_executor(),
                ctx.sync_metrics_tx(),
                ctx.prune_config(),
                max_block,
                static_file_producer,
                ctx.components().block_executor().clone(),
                pipeline_exex_handle,
            )?;

            (pipeline, network_client.clone())
        };

        let pipeline_events = pipeline.events();

        let initial_target = ctx.node_config().debug.tip;

        let mut pruner_builder = ctx.pruner_builder();
        if let Some(exex_manager_handle) = &exex_manager_handle {
            pruner_builder =
                pruner_builder.finished_exex_height(exex_manager_handle.finished_height());
        }
        let pruner = pruner_builder.build_with_provider_factory(ctx.provider_factory().clone());

        let pruner_events = pruner.events();
        info!(target: "reth::cli", prune_config=?ctx.prune_config().unwrap_or_default(), "Pruner initialized");
        hooks.add(PruneHook::new(
            pruner,
            Box::new(ctx.task_executor().clone()),
        ));

        // Configure the consensus engine
        let (beacon_consensus_engine, beacon_engine_handle) = BeaconConsensusEngine::with_channel(
            client,
            pipeline,
            ctx.blockchain_db().clone(),
            Box::new(ctx.task_executor().clone()),
            Box::new(ctx.components().network().clone()),
            max_block,
            ctx.components().payload_builder().clone(),
            initial_target,
            reth_beacon_consensus::MIN_BLOCKS_FOR_PIPELINE_RUN,
            consensus_engine_tx,
            Box::pin(consensus_engine_stream),
            hooks,
        )?;
        info!(target: "reth::cli", "Consensus engine initialized");

        let events = stream_select!(
            pipeline_events.map(Into::into),
            if ctx.node_config().debug.tip.is_none() && !ctx.is_dev() {
                Either::Left(
                    ConsensusLayerHealthEvents::new(Box::new(ctx.blockchain_db().clone()))
                        .map(Into::into),
                )
            } else {
                Either::Right(stream::empty())
            },
            pruner_events.map(Into::into),
            static_file_producer_events.map(Into::into),
        );
        ctx.task_executor().spawn_critical(
            "events task",
            node::handle_events(
                Some(Box::new(ctx.components().network().clone())),
                Some(ctx.head().number),
                events,
            ),
        );

        // extract the jwt secret from the args if possible
        let jwt_secret = ctx.auth_jwt_secret()?;

        let add_ons_ctx = AddOnsContext {
            node: ctx.node_adapter().clone(),
            config: ctx.node_config(),
            beacon_engine_handle,
            jwt_secret,
        };

        let RpcHandle {
            rpc_server_handles,
            rpc_registry,
        } = add_ons.launch_add_ons(add_ons_ctx).await?;

        // Run consensus engine to completion
        let (tx, rx) = oneshot::channel();
        info!(target: "reth::cli", "Starting consensus engine");
        ctx.task_executor()
            .spawn_critical_blocking("consensus engine", async move {
                let res = beacon_consensus_engine.await;
                let _ = tx.send(res);
            });

        if let Some(maybe_custom_etherscan_url) = ctx.node_config().debug.etherscan.clone() {
            info!(target: "reth::cli", "Using etherscan as consensus client");

            let chain = ctx.node_config().chain.chain();
            let etherscan_url = maybe_custom_etherscan_url.map(Ok).unwrap_or_else(|| {
                // If URL isn't provided, use default Etherscan URL for the chain if it is known
                chain
                    .etherscan_urls()
                    .map(|urls| urls.0.to_string())
                    .ok_or_else(|| eyre::eyre!("failed to get etherscan url for chain: {chain}"))
            })?;

            let block_provider = EtherscanBlockProvider::new(
                etherscan_url,
                chain.etherscan_api_key().ok_or_else(|| {
                    eyre::eyre!(
                        "etherscan api key not found for rpc consensus client for chain: {chain}"
                    )
                })?,
            );
            let rpc_consensus_client = DebugConsensusClient::new(
                rpc_server_handles.auth.clone(),
                Arc::new(block_provider),
            );
            ctx.task_executor()
                .spawn_critical("etherscan consensus client", async move {
                    rpc_consensus_client.run::<Types::Engine>().await
                });
        }

        if let Some(rpc_ws_url) = ctx.node_config().debug.rpc_consensus_ws.clone() {
            info!(target: "reth::cli", "Using rpc provider as consensus client");

            let block_provider = RpcBlockProvider::new(rpc_ws_url);
            let rpc_consensus_client = DebugConsensusClient::new(
                rpc_server_handles.auth.clone(),
                Arc::new(block_provider),
            );
            ctx.task_executor()
                .spawn_critical("rpc consensus client", async move {
                    rpc_consensus_client.run::<Types::Engine>().await
                });
        }

        let full_node = FullNode {
            evm_config: ctx.components().evm_config().clone(),
            block_executor: ctx.components().block_executor().clone(),
            pool: ctx.components().pool().clone(),
            network: ctx.components().network().clone(),
            provider: ctx.node_adapter().provider.clone(),
            payload_builder: ctx.components().payload_builder().clone(),
            task_executor: ctx.task_executor().clone(),
            config: ctx.node_config().clone(),
            data_dir: ctx.data_dir().clone(),
            add_ons_handle: RpcHandle {
                rpc_server_handles,
                rpc_registry,
            },
        };
        // Notify on node started
        on_node_started.on_event(FullNode::clone(&full_node))?;

        let handle = NodeHandle {
            node_exit_future: NodeExitFuture::new(
                async { Ok(rx.await??) },
                full_node.config.debug.terminate,
            ),
            node: full_node,
        };

        Ok(handle)
    }
}
