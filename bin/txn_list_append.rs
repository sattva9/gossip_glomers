use std::{collections::HashMap, io, sync::Arc};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct TxnKVStoreApp {
    lock: Mutex<()>,
}

impl TxnKVStoreApp {
    #[allow(unused_variables)]
    async fn transaction_handler(
        &self,
        maelstrom: &Maelstrom,
        mut txn: Vec<Transaction>,
    ) -> io::Result<Vec<Transaction>> {
        let _lock_gaurd = self.lock.lock().await;

        // storing whole database as a value of `root` key in lin-kv store
        let body = MessageBody::with_type(MessageType::Read {
            key: Some("root".to_string()),
        });
        let response = maelstrom.rpc("lin-kv".to_owned(), body, false).await?;
        let old_data = match response.body.msg_type {
            MessageType::ReadOk { messages, value } => value.unwrap(),
            _ => Value::None,
        };
        let mut data = match old_data.to_owned() {
            Value::Map(v) => v,
            _ => HashMap::new(),
        };

        for t in txn.iter_mut() {
            match t {
                Transaction::Read { key, val } => {
                    *val = Value::Vec(data.get(&key.to_string()).map(|v| v.to_owned()).unwrap());
                }
                Transaction::Append { key, value } => {
                    let entry = data.entry(key.to_string()).or_insert(vec![]);
                    entry.push(*value);
                }
                _ => {}
            }
        }

        let body = MessageBody::with_type(MessageType::Cas {
            key: "root".to_string(),
            from: old_data,
            to: Value::Map(data),
            create_if_not_exists: Some(true),
        });
        let response = maelstrom.rpc("lin-kv".to_owned(), body, false).await?;
        match response.body.msg_type {
            MessageType::Error { code, text } => {
                return Err(io::Error::new(io::ErrorKind::Other, "failed cas"));
            }
            _ => {}
        };
        Ok(txn)
    }
}

#[async_trait]
impl App for TxnKVStoreApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> io::Result<()> {
        match &request.body.msg_type {
            MessageType::Txn { txn } => {
                let body = match self.transaction_handler(&maelstrom, txn.to_owned()).await {
                    Ok(txn) => MessageBody::with_type(MessageType::TxnOk { txn }),
                    Err(_) => MessageBody::with_type(MessageType::Error {
                        code: 30,
                        text: "The requested transaction has been aborted because of a conflict."
                            .to_string(),
                    }),
                };

                maelstrom.reply(request, body)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let app = Arc::new(TxnKVStoreApp::default());
    Maelstrom::new().run_with_app(app).await
}
