use std::{
    io,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};

#[derive(Default)]
struct UniqueIdsApp {
    id: AtomicU64,
}

#[async_trait]
impl App for UniqueIdsApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> std::io::Result<()> {
        match &request.body.msg_type {
            MessageType::Generate => {
                let id = self.id.fetch_add(1, Ordering::Relaxed);
                let id = format!("{}-{}", maelstrom.node_id(), id);
                let body = MessageBody::with_type(MessageType::GenerateOk { id });
                maelstrom.reply_with_id(request, body)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let app = Arc::new(UniqueIdsApp::default());
    Maelstrom::new().run_with_app(app).await
}
