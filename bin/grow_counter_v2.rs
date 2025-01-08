use std::{io, sync::Arc};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct GrowOnlyCounterApp {
    lock: Mutex<()>,
}

impl GrowOnlyCounterApp {
    // read from lin-kv store
    #[allow(unused_variables)]
    async fn read(&self, maelstrom: &Maelstrom, key: String) -> io::Result<Value> {
        let body = MessageBody::with_type(MessageType::Read { key: Some(key) });
        let response = maelstrom.rpc("seq-kv".to_owned(), body, false).await?;

        let value = match response.body.msg_type {
            MessageType::ReadOk { messages, value } => value.unwrap(),
            _ => Value::None,
        };
        Ok(value)
    }

    // write to lin-kv store
    async fn write(&self, maelstrom: &Maelstrom, key: String, value: Value) -> io::Result<()> {
        let body = MessageBody::with_type(MessageType::Write {
            key: key.to_owned(),
            value,
        });
        maelstrom.rpc("seq-kv".to_owned(), body, false).await?;
        Ok(())
    }
}

#[async_trait]
impl App for GrowOnlyCounterApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> io::Result<()> {
        let _lock_gaurd = self.lock.lock().await;

        match &request.body.msg_type {
            MessageType::Add { delta } => {
                let key = maelstrom.node_id();
                let value = self
                    .read(&maelstrom, key.to_owned())
                    .await?
                    .as_int()
                    .unwrap_or_default();
                let _ = self
                    .write(&maelstrom, key.to_owned(), Value::Int(value + *delta))
                    .await;

                maelstrom.reply(request, MessageBody::with_type(MessageType::AddOk))?;
            }
            #[allow(unused_variables)]
            MessageType::Read { key } => {
                // read and add counter values of all nodes
                let mut sum = 0;
                for node_id in maelstrom.node_ids() {
                    sum += self
                        .read(&maelstrom, node_id)
                        .await?
                        .as_int()
                        .unwrap_or_default();
                }

                let body = MessageBody::with_type(MessageType::ReadOk {
                    messages: None,
                    value: Some(Value::Int(sum)),
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
    let app = Arc::new(GrowOnlyCounterApp::default());
    Maelstrom::new().run_with_app(app).await
}
