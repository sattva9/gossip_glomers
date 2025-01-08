use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};
use tokio::sync::OnceCell;

#[derive(Default)]
struct GrowOnlyCounterApp {
    counters: OnceCell<HashMap<String, AtomicI64>>,
}

#[async_trait]
impl App for GrowOnlyCounterApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> io::Result<()> {
        // get counters for each node in the network
        let counters = self
            .counters
            .get_or_init(|| async {
                let mut counters = HashMap::new();
                for node_id in maelstrom.node_ids() {
                    counters.insert(node_id, AtomicI64::new(0));
                }
                counters
            })
            .await;

        match &request.body.msg_type {
            MessageType::Add { delta } => {
                // update counter of the current node
                let old = counters
                    .get(&request.dest)
                    .unwrap()
                    .fetch_add(*delta, Ordering::Relaxed);
                let message = old + *delta;

                maelstrom.reply(request, MessageBody::with_type(MessageType::AddOk))?;

                // broadcast current node value to other nodes in the network
                let body = MessageBody::with_type(MessageType::Broadcast { message });
                for dest in maelstrom.node_ids() {
                    if dest.ne(maelstrom.node_id()) {
                        let _ = maelstrom.send(dest.to_owned(), body.clone());
                    }
                }
            }
            #[allow(unused_variables)]
            MessageType::Read { key } => {
                // read and add counter values of all nodes
                let value = counters.values().map(|a| a.load(Ordering::Relaxed)).sum();
                let body = MessageBody::with_type(MessageType::ReadOk {
                    messages: None,
                    value: Some(Value::Int(value)),
                });

                maelstrom.reply(request, body)?;
            }
            MessageType::Broadcast { message } => {
                // update counter of the node which sent this broadcast
                counters
                    .get(&request.src)
                    .unwrap()
                    .fetch_max(*message, Ordering::Relaxed);
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let app = Arc::new(GrowOnlyCounterApp::default());
    Maelstrom::new().run_with_app(app).await
}
