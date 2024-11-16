mod backend;
mod cli;
use anyhow::Result;
use backend::DataFusionBackEnd;
pub use cli::DatasetConn;
pub use cli::ReplCommand;
use cli::{
    connect, describe, head, list, schema, sql, ConnectOpts, DescribeOpts, HeadOpts, ListOpts,
    SchemaOpts, SqlOpts,
};
use crossbeam_channel as mpsc;

use datafusion::prelude::DataFrame;
use enum_dispatch::enum_dispatch;

use reedline_repl_rs::{CallBackMap, Error};
use std::{ops::Deref, process, thread};
use tokio::runtime::Runtime;
pub type ReplCallBacks = CallBackMap<ReplContext, Error>;

pub fn get_callbacks() -> ReplCallBacks {
    let mut callbacks = ReplCallBacks::new();
    callbacks.insert("connect".to_string(), connect);
    callbacks.insert("list".to_string(), list);
    callbacks.insert("schema".to_string(), schema);
    callbacks.insert("describe".to_string(), describe);
    callbacks.insert("head".to_string(), head);
    callbacks.insert("sql".to_string(), sql);
    callbacks
}
pub struct ReplContext {
    pub tx: mpsc::Sender<ReplMsg>,
}

pub struct ReplMsg {
    pub cmd: ReplCommand,
    pub tx: oneshot::Sender<String>,
}

#[enum_dispatch]
trait CmdExcutor {
    async fn execute<T: BackEnd>(self, backend: &mut T) -> Result<String>;
}

trait BackEnd {
    type DataFrame: ReplDisplay;
    async fn connect(&mut self, opts: &ConnectOpts) -> Result<()>;
    async fn list(&mut self) -> Result<Vec<String>>;
    async fn schema(&self, name: &str) -> Result<DataFrame>;
    async fn describe(&self, name: &str) -> Result<DataFrame>;
    async fn head(&self, opts: HeadOpts) -> Result<DataFrame>;
    async fn sql(&self, sql: &str) -> Result<DataFrame>;
}

trait ReplDisplay {
    async fn display(self) -> Result<String>;
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded::<ReplMsg>();
        let rt = Runtime::new().expect("Failed to create runtime");
        let mut ctx = DataFusionBackEnd::new();
        thread::Builder::new()
            .name("ReplContext".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    if let Err(e) = rt.block_on(async {
                        // 因为有了enum_dispatch宏，这里可以直接调用 ReplCommand对应的方法，如果 sql,head 的execute方法没有实现
                        //不用使用大量的match去实现
                        let ret = msg.cmd.execute(&mut ctx).await?;
                        if let Err(e) = msg.tx.send(ret) {
                            eprintln!("Fail to send result: {}", e);
                            process::exit(1);
                        };

                        anyhow::Result::<String>::Ok("".to_string())
                    }) {
                        eprintln!("Fail to process command: {}", e);
                        process::exit(1);
                    }
                }
            })
            .unwrap();

        Self { tx }
    }

    pub fn send(&self, cmd: ReplMsg, rx: oneshot::Receiver<String>) -> Option<String> {
        if let Err(e) = self.tx.send(cmd) {
            eprintln!("Send Error: {}", e);
            process::exit(1);
        }
        match rx.recv() {
            Ok(ret) => Some(ret),
            Err(e) => {
                eprintln!("Receive Error: {}", e);
                process::exit(1);
            }
        }
    }
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplMsg>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplMsg {
    pub fn new(cmd: impl Into<ReplCommand>) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cmd: cmd.into(),
                tx,
            },
            rx,
        )
    }
}
