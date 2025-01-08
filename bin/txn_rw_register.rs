use std::{io, sync::Arc};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct KVStoreApp {
    lock: Mutex<()>,
}

impl KVStoreApp {
    async fn distributed_lock(&self, maelstrom: &Maelstrom, acquire: bool) -> io::Result<()> {
        let (from, to) = if acquire {
            (Value::None, Value::String(maelstrom.node_id().to_string()))
        } else {
            (Value::String(maelstrom.node_id().to_string()), Value::None)
        };

        let body = MessageBody::with_type(MessageType::Cas {
            key: "lock".to_string(),
            from: from.to_owned(),
            to: to.to_owned(),
            create_if_not_exists: Some(true),
        });
        loop {
            let response = maelstrom
                .rpc("lin-kv".to_owned(), body.to_owned(), false)
                .await?;
            match response.body.msg_type {
                MessageType::CasOk => break,
                _ => {}
            };
        }

        Ok(())
    }

    #[allow(unused_variables)]
    async fn transaction_handler(
        &self,
        maelstrom: &Maelstrom,
        mut txn: Vec<Transaction>,
    ) -> io::Result<Vec<Transaction>> {
        for t in txn.iter_mut() {
            match t {
                Transaction::Read { key, val } => {
                    let body = MessageBody::with_type(MessageType::Read {
                        key: Some(key.to_string()),
                    });
                    let response = maelstrom.rpc("lin-kv".to_owned(), body, false).await?;

                    let value = match response.body.msg_type {
                        MessageType::ReadOk { messages, value } => value.unwrap(),
                        _ => Value::None,
                    };

                    *val = value;
                }
                Transaction::Write { key, value } => {
                    let body = MessageBody::with_type(MessageType::Write {
                        key: key.to_string(),
                        value: Value::Int(*value),
                    });
                    let response = maelstrom.rpc("lin-kv".to_owned(), body, false).await?;
                }
                _ => {}
            }
        }

        Ok(txn)
    }
}

#[async_trait]
impl App for KVStoreApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> io::Result<()> {
        match &request.body.msg_type {
            MessageType::Txn { txn } => {
                let _lock_gaurd = self.lock.lock().await;

                // acquire distributed lock
                self.distributed_lock(&maelstrom, true).await?;

                // process transaction
                if let Ok(txn) = self.transaction_handler(&maelstrom, txn.to_owned()).await {
                    let body = MessageBody::with_type(MessageType::TxnOk { txn });
                    let _ = maelstrom.reply(request, body);
                }

                // release distributed lock
                self.distributed_lock(&maelstrom, false).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let app = Arc::new(KVStoreApp::default());
    Maelstrom::new().run_with_app(app).await
}
