//! This example shows how to configure custom components for a reth node.

// TODO: revisit this later, get rid of unused imports
// #![cfg_attr(not(test), warn(unused_crate_dependencies))]

use reth::cli::Cli;
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};

use tokio::sync::mpsc::{channel, unbounded_channel};

mod config;
mod consensus_node;
mod engine_api_proxy;
mod reth_node_launcher;

use consensus_node::JolteonNode;
use engine_api_proxy::EngineApiProxy;
use reth_node_launcher::DefaultNodeLauncher;

// TODO: get rid of this later
const DB_PATH: &str = "./db";

fn main() {
    Cli::parse_args()
        .run(|builder, _| async move {
            // 0. To be used by the Jolteon consensus part.
            let (consensus_tx, consensus_rx) = unbounded_channel();

            // 1. Build & start the reth node handle
            let handle = builder
                // use the default ethereum node types
                .with_types::<EthereumNode>()
                // Configure the components of the node
                // use default ethereum components
                .with_components(EthereumNode::components())
                .with_add_ons(EthereumAddOns::default())
                .launch_with_fn(|builder| {
                    let launcher = DefaultNodeLauncher::new(
                        builder.task_executor().clone(),
                        builder.config().datadir(),
                        (consensus_tx.clone(), consensus_rx),
                    );

                    builder.launch_with(launcher)
                })
                .await?;

            // 2. Build & start the Jolten consensus node handle
            let (tx_engine_proxy, rx_engine_proxy) = channel(1000);
            let consensus_node = JolteonNode::new(&"", &"", DB_PATH, None, tx_engine_proxy)
                .await
                .map_err(|e| {
                    log::error!("Failed to create consensus node: {:?}", e);
                    e
                })
                .unwrap();
            log::info!("Node started");

            // 3. Build & start the Engine API proxy
            let provider = handle.node.provider.clone();
            let evm_config = handle.node.evm_config.clone();
            handle
                .node
                .task_executor
                .spawn_critical("engine API proxy", async move {
                    EngineApiProxy::new(
                        provider,
                        evm_config,
                        consensus_node.store,
                        rx_engine_proxy,
                        consensus_tx,
                    )
                    .run()
                    .await;
                });

            // TODO: use tx stream to send transactions to the consensus
            // let _task_executor = handle.node.task_executor.clone();
            // let _pool = handle.node.pool.clone();

            handle.wait_for_node_exit().await
        })
        .unwrap();
}
