use std::{
    collections::HashMap,
    future::Future,
    io::{self, BufRead, Error},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    sync::{
        oneshot::{self, Sender},
        Mutex, OnceCell,
    },
    task::JoinHandle,
    time::interval,
};
use tokio_util::task::TaskTracker;

use crate::message::{Message, MessageBody, MessageType};

#[derive(Clone)]
pub struct Maelstrom {
    inner: Arc<MaelstromInner>,
}

pub struct MaelstromInner {
    node: OnceCell<NodeMeta>,
    rpc: Mutex<HashMap<u64, Sender<Message>>>,
    next_msg_id: AtomicU64,
    task_tracker: TaskTracker,
}

#[derive(Debug)]
pub struct NodeMeta {
    node_id: String,
    node_ids: Vec<String>,
}

impl Maelstrom {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MaelstromInner {
                node: Default::default(),
                rpc: Default::default(),
                next_msg_id: AtomicU64::new(0),
                task_tracker: TaskTracker::new(),
            }),
        }
    }

    pub fn log(&self, message: String) {
        eprintln!("{message}");
    }

    pub fn set_node_meta(&self, node: NodeMeta) -> io::Result<()> {
        self.inner
            .node
            .set(node)
            .map_err(|e| Error::new(io::ErrorKind::Other, e))
    }

    pub fn node_id(&self) -> &str {
        if let Some(node) = self.inner.node.get() {
            return node.node_id.as_str();
        }
        ""
    }

    pub fn node_ids(&self) -> Vec<String> {
        if let Some(node) = self.inner.node.get() {
            return node.node_ids.to_owned();
        }
        vec![]
    }

    fn next_msg_id(&self) -> u64 {
        self.inner.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn send(&self, dest: String, body: MessageBody) -> io::Result<()> {
        let message = Message {
            src: self.node_id().to_owned(),
            dest,
            body,
        };
        let message = serde_json::to_value(message)?;

        println!("{message}");
        self.log(format!("sent {}", message.to_string()));

        Ok(())
    }

    pub fn send_with_id(&self, dest: String, mut body: MessageBody) -> io::Result<()> {
        body.msg_id = Some(self.inner.next_msg_id.fetch_add(1, Ordering::Relaxed));
        self.send(dest, body)
    }

    pub fn reply(&self, request: Message, mut body: MessageBody) -> io::Result<()> {
        body.in_reply_to = request.body.msg_id;
        self.send(request.src, body)
    }

    pub fn reply_with_id(&self, request: Message, mut body: MessageBody) -> io::Result<()> {
        body.msg_id = Some(self.next_msg_id());
        body.in_reply_to = request.body.msg_id;
        self.send(request.src, body)
    }

    pub async fn rpc(
        &self,
        dest: String,
        mut body: MessageBody,
        retry: bool,
    ) -> io::Result<Message> {
        let msg_id = self.next_msg_id();
        body.msg_id = Some(msg_id);

        let (sender, mut receiver) = oneshot::channel::<Message>();
        let mut interval = interval(Duration::from_millis(500));
        self.inner.rpc.lock().await.insert(msg_id, sender);

        self.send(dest.to_owned(), body.to_owned())?;
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if retry {
                        self.send(dest.to_owned(), body.to_owned())?;
                    } else {
                        return Err(Error::new(io::ErrorKind::TimedOut, "rpc timed out"));
                    }
                },
                msg = &mut receiver => {
                    return Ok(msg.unwrap());
                }
            }
        }
    }

    pub fn spawn_rpc(
        &self,
        dest: String,
        body: MessageBody,
        retry: bool,
    ) -> JoinHandle<io::Result<Message>> {
        let m = self.clone();
        self.spawn(async move { m.rpc(dest, body, retry).await })
    }

    pub async fn process_response(maelstrom: Self, request: Message, in_reply_to: u64) {
        let sender = maelstrom.inner.rpc.lock().await.remove(&in_reply_to);
        if let Some(sender) = sender {
            sender.send(request).unwrap();
        }
    }

    pub async fn run_with_app(&self, app: Arc<dyn App + 'static>) -> io::Result<()> {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line?;
            self.log(format!("received {line}"));

            let request = serde_json::from_str::<Message>(&line)?;

            if let Some(in_reply_to) = request.body.in_reply_to {
                self.spawn(Self::process_response(self.clone(), request, in_reply_to));
                continue;
            }

            match &request.body.msg_type {
                MessageType::Init { node_id, node_ids } => {
                    let node_meta = NodeMeta {
                        node_id: node_id.to_owned(),
                        node_ids: node_ids.to_owned(),
                    };
                    self.set_node_meta(node_meta)?;
                    self.reply_with_id(request, MessageBody::with_type(MessageType::InitOk))?;
                }
                _ => {
                    // let _ = app.handler(self.clone(), request).await;
                    let maelstrom = self.clone();
                    let app = app.clone();
                    self.spawn(async move {
                        if let Err(e) = app.handler(maelstrom.clone(), request).await {
                            maelstrom.log(format!("Error: {e}"));
                        }
                    });
                }
            }
        }

        self.graceful_shutdown().await;
        Ok(())
    }

    async fn graceful_shutdown(&self) {
        self.inner.task_tracker.close();
        self.inner.task_tracker.wait().await;
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.task_tracker.spawn(future)
    }
}

#[async_trait]
pub trait App: Sync + Send {
    async fn handler(&self, maelstrom: Maelstrom, request: Message) -> io::Result<()>;
}
