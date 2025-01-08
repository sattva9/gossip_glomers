use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};
use tokio::sync::{Mutex, OnceCell};

#[derive(Default)]
struct BroadcastApp {
    neighbours: OnceCell<Vec<String>>,
    // holds all messages the app received through broadcast
    messages: Mutex<HashSet<i64>>,
    // holds pending messages that need to be broadcasted
    neighbours_meta: OnceCell<HashMap<String, Mutex<HashSet<i64>>>>,
}

#[async_trait]
impl App for BroadcastApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> std::io::Result<()> {
        match &request.body.msg_type {
            MessageType::Topology { topology } => {
                let neighbours = topology.get(maelstrom.node_id()).unwrap().to_owned();

                let mut neighbours_meta = HashMap::new();
                for neighbour in neighbours.iter() {
                    neighbours_meta.insert(neighbour.to_owned(), Default::default());
                }

                let _ = self.neighbours.set(neighbours);
                let _ = self.neighbours_meta.set(neighbours_meta);

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

                    let neighbours = self.neighbours.get().unwrap();
                    let neighbours_meta = self.neighbours_meta.get().unwrap();

                    // add the new message to pending messages that need to be broadcasted to each neighbour
                    for neighbour in neighbours {
                        if neighbour.ne(&request.src) {
                            neighbours_meta
                                .get(neighbour)
                                .unwrap()
                                .lock()
                                .await
                                .insert(*message);
                        }
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
            MessageType::BroadcastMany { messages } => {
                let mut new_messages = HashSet::new();
                let mut data = self.messages.lock().await;

                // add the new messages received through broadcast to local state
                for m in messages.iter() {
                    if !data.contains(m) {
                        data.insert(*m);
                        new_messages.insert(*m);
                    }
                }
                drop(data);

                let neighbours = self.neighbours.get().unwrap();
                let neighbours_meta = self.neighbours_meta.get().unwrap();
                // add them to pending messages for each neighbour
                for neighbour in neighbours {
                    if neighbour.ne(&request.src) {
                        neighbours_meta
                            .get(neighbour)
                            .unwrap()
                            .lock()
                            .await
                            .extend(new_messages.to_owned());
                    }
                }

                let body = MessageBody::with_type(MessageType::BroadcastManyOk);
                maelstrom.reply(request, body)?;
            }
            _ => {}
        }
        Ok(())
    }
}

async fn gossip_broadcast(maelstrom: Arc<Maelstrom>, app: Arc<BroadcastApp>) {
    loop {
        std::thread::sleep(std::time::Duration::from_millis(500));
        let neighbours_meta = app.neighbours_meta.get().unwrap();

        // get pending messages that need to be broacasted to each neighbour
        for (dest, meta) in neighbours_meta.iter() {
            // acquire lock to access data
            let mut data = meta.lock().await;
            let messages = data.clone();
            data.clear();
            // release lock
            drop(data);

            if messages.is_empty() {
                continue;
            }

            // broadcast messages if not empty
            let body = MessageBody::with_type(MessageType::BroadcastMany { messages });
            maelstrom.spawn_rpc(dest.to_owned(), body, true);
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let app = Arc::new(BroadcastApp::default());
    let maelstrom = Arc::new(Maelstrom::new());

    // periodically broadcast data of the current node
    tokio::spawn(gossip_broadcast(maelstrom.clone(), app.clone()));

    maelstrom.run_with_app(app).await
}
