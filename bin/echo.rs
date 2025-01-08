use std::{io, sync::Arc};

use async_trait::async_trait;
use maelstrom_client::{
    maelstrom::{App, Maelstrom},
    message::*,
};

#[derive(Default)]
struct EchoApp;

#[async_trait]
impl App for EchoApp {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> std::io::Result<()> {
        match &request.body.msg_type {
            MessageType::Echo { echo } => {
                let body = MessageBody::with_type(MessageType::EchoOk {
                    echo: echo.to_owned(),
                });
                maelstrom.reply_with_id(request, body)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let app = Arc::new(EchoApp::default());
    Maelstrom::new().run_with_app(app).await
}
