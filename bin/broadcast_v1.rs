use std::{collections::HashSet, io, sync::Arc};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};
use tokio::sync::{Mutex, OnceCell};

#[derive(Default)]
struct BroadcastApp {
    neighbours: OnceCell<Vec<String>>,
    messages: Mutex<HashSet<i64>>,
}

#[async_trait]
impl App for BroadcastApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> std::io::Result<()> {
        match &request.body.msg_type {
            MessageType::Topology { topology } => {
                // set neighbours of the current node
                let neighbours = topology.get(maelstrom.node_id()).unwrap().to_owned();
                let _ = self.neighbours.set(neighbours);

                let body = MessageBody::with_type(MessageType::TopologyOk);
                maelstrom.reply(request, body)?;
            }
            MessageType::Broadcast { message } => {
                // acquire lock to access local state
                let mut data = self.messages.lock().await;

                if !data.contains(message) {
                    data.insert(*message);
                    // release the lock
                    drop(data);

                    let neighbours = self.neighbours.get().unwrap().to_owned();
                    let body = MessageBody::with_type(MessageType::Broadcast { message: *message });
                    // broadcast message to all neighbours except src
                    for neighbour in neighbours {
                        if neighbour.eq(&request.src) {
                            continue;
                        }
                        maelstrom.spawn_rpc(neighbour, body.clone(), true);
                    }
                }

                let body = MessageBody::with_type(MessageType::BroadcastOk);
                maelstrom.reply(request, body)?;
            }
            #[allow(unused_variables)]
            MessageType::Read { key } => {
                let messages = self.messages.lock().await.clone();
                let body = MessageBody::with_type(MessageType::ReadOk {
                    messages: Some(messages),
                    value: None,
                });
                maelstrom.reply(request, body)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let app = Arc::new(BroadcastApp::default());
    Maelstrom::new().run_with_app(app).await
}
