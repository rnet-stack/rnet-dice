use std::{env, sync::Arc, time::Duration};

use anyhow::Result;
use rnet_p2p::{
    identity::{
        events::{FloodsubMsgType, GlobalEvent},
        multiaddr::Multiaddr,
        traits::{core::INode, protocols::INodeFloodsubAPI},
    },
    node::{inner::NodeInner, node::Node, protocol::InnerProtocolOpt},
    protocols::FLOODSUB,
};
use tokio::sync::mpsc::Receiver;
use tracing::{debug, info};

use crate::common::{MpcMsgType, set_bootstrap_node};

pub struct MPCNode {
    pub host_mpsc_tx: Arc<Node>,
    pub mode: String,
    pub listen_addr: Multiaddr,
}

impl MPCNode {
    pub async fn new(mode: &str) -> Arc<MPCNode> {
        let mut listen_addr = Multiaddr::new("ip4/127.0.0.1/tcp/0").unwrap();
        let (host_mpsc_tx, global_rx) = NodeInner::new(
            &mut listen_addr,
            vec![InnerProtocolOpt::Floodsub, InnerProtocolOpt::Ping],
        )
        .await
        .unwrap();

        let mpc_node = Arc::new(MPCNode {
            host_mpsc_tx,
            mode: mode.to_string(),
            listen_addr,
        });
        let handler_mcp = mpc_node.clone();

        tokio::spawn(async move {
            handler_mcp.p2p_handler(global_rx).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(2000)).await;

        mpc_node.initiate().await.unwrap();

        mpc_node
    }

    pub async fn initiate(&self) -> Result<()> {
        match self.mode.as_ref() {
            "bootstrap" => {
                set_bootstrap_node(&self.listen_addr.to_string()).unwrap();
            }
            "general" => {
                // CONNECT TO BOOTSTRAP NODE
                info!("Connection to BOOTSTRAP node...");
                let bootstrap_node = Multiaddr::new(&env::var("BOOTSTRAP_NODE").unwrap()).unwrap();
                self.host_mpsc_tx
                    .new_stream(&bootstrap_node.to_string(), vec![FLOODSUB.to_string()])
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(2000)).await;
            }
            _ => {}
        }

        self.host_mpsc_tx
            .floodsub_subscribe("mpc-common".to_string())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2000)).await;

        Ok(())
    }

    pub async fn p2p_handler(&self, mut global_event_rx: Receiver<Vec<u8>>) -> Result<()> {
        loop {
            let notification = global_event_rx.recv().await.unwrap();
            let decoded = bincode::deserialize::<GlobalEvent>(&notification).unwrap();
            match decoded {
                GlobalEvent::Floodsub(event) => match event.msg_type {
                    FloodsubMsgType::Publish => {
                        let topic = event.topic;
                        let source = event.source.unwrap();
                        let decoded_msg =
                            bincode::deserialize::<MpcMsgType>(&event.msg.unwrap()).unwrap();

                        match decoded_msg {
                            MpcMsgType::General(msg) => {
                                debug!("FloodsubEvent: {topic} - {source}: {msg}")
                            }

                            MpcMsgType::Advertize(adv) => {
                                info!("Session starting at topic: {}", adv);
                            }

                            MpcMsgType::Session(_payload) => {}
                            MpcMsgType::Bootmesh(_mesh) => {}
                        }
                    }
                    FloodsubMsgType::Subscribe => {
                        debug!("FloodsubEvent: SUBSCRIBED - {}", event.topic);
                    }
                    FloodsubMsgType::Unsubscribe => {
                        debug!("FloodsubEvent: UNSUBSCRIBED - {}", event.topic);
                    }
                },
                GlobalEvent::Ping(event) => debug!("{:?}", event),
            }
        }
    }
}
