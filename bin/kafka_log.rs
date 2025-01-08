use std::{collections::HashMap, io, sync::Arc};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct KafkaLogApp {
    lock: Mutex<()>,
}

impl KafkaLogApp {
    async fn distributed_lock(&self, maelstrom: &Maelstrom, lock: bool) -> io::Result<()> {
        let (from, to) = if lock {
            (Value::None, Value::String(maelstrom.node_id().to_string()))
        } else {
            (Value::String(maelstrom.node_id().to_string()), Value::None)
        };

        loop {
            let body = MessageBody::with_type(MessageType::Cas {
                key: "lock".to_string(),
                from: from.to_owned(),
                to: to.to_owned(),
                create_if_not_exists: Some(true),
            });
            let response = maelstrom.rpc("lin-kv".to_owned(), body, false).await?;
            match response.body.msg_type {
                MessageType::CasOk => break,
                _ => {}
            };
        }

        Ok(())
    }

    // read from lin-kv store
    #[allow(unused_variables)]
    async fn read(&self, maelstrom: &Maelstrom, key: &str) -> io::Result<Value> {
        let body = MessageBody::with_type(MessageType::Read {
            key: Some(key.to_owned()),
        });
        let response = maelstrom.rpc("lin-kv".to_owned(), body, false).await?;

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
        maelstrom.rpc("lin-kv".to_owned(), body, false).await?;
        Ok(())
    }
}

#[async_trait]
impl App for KafkaLogApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> io::Result<()> {
        let _lock = self.lock.lock().await;

        // we acquire distributed lock only if we have write to lin-kv store
        match &request.body.msg_type {
            MessageType::Send { key, msg } => {
                // acquire distributed lock
                self.distributed_lock(&maelstrom, true).await?;

                // read data for key from lin-kv, append new msg to key and write back to lin-kv store
                // offset will be index of new msg in the list
                let mut data = self
                    .read(&maelstrom, key)
                    .await?
                    .as_vec()
                    .unwrap_or_default();
                let offset = data.len() as i64;
                data.push(*msg as i64);
                self.write(&maelstrom, key.to_owned(), Value::Vec(data))
                    .await?;

                let body = MessageBody::with_type(MessageType::SendOk { offset });
                let _ = maelstrom.reply(request, body);

                // release distributed lock
                self.distributed_lock(&maelstrom, false).await?;
            }
            MessageType::Poll { offsets } => {
                let mut msgs = HashMap::new();

                // read data for each key from lin-kv store and convert the data to required format
                for (key, offset) in offsets {
                    if let Some(data) = self.read(&maelstrom, key).await?.as_vec() {
                        let data: Vec<[i64; 2]> = data
                            .into_iter()
                            .enumerate()
                            .filter(|(idx, _)| *idx as i64 >= *offset)
                            .map(|(idx, value)| [idx as i64, value])
                            .collect();

                        msgs.insert(key.to_owned(), data);
                    }
                }

                let body = MessageBody::with_type(MessageType::PollOk { msgs });
                maelstrom.reply(request, body)?;
            }
            MessageType::CommitOffsets { offsets } => {
                // acquire distributed lock
                self.distributed_lock(&maelstrom, true).await?;

                // read commited offset for each key from lin-kv and update if the new offset is greater
                for (key, offset) in offsets {
                    let key = format!("{key}-commited");
                    let last_comitted_offset =
                        self.read(&maelstrom, &key).await?.as_int().unwrap_or(-1);

                    if last_comitted_offset < *offset {
                        self.write(&maelstrom, key.to_owned(), Value::Int(*offset))
                            .await?;
                    }
                }

                maelstrom.reply(
                    request,
                    MessageBody::with_type(MessageType::CommitOffsetsOk),
                )?;

                // release distributed lock
                self.distributed_lock(&maelstrom, false).await?;
            }
            MessageType::ListCommittedOffsets { keys } => {
                let mut offsets = HashMap::new();

                // read commited offset for each key from lin-kv store
                for key in keys {
                    let key = format!("{key}-commited");
                    if let Some(offset) = self.read(&maelstrom, &key).await?.as_int() {
                        offsets.insert(key.to_owned(), offset);
                    }
                }

                let body = MessageBody::with_type(MessageType::ListCommittedOffsetsOk { offsets });
                maelstrom.reply(request, body)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let app = Arc::new(KafkaLogApp::default());
    Maelstrom::new().run_with_app(app).await
}
