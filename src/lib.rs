mod cli;

pub use cli::ReplCommand;
use cli::{connect, describe, head, list, sql};
use crossbeam_channel as mpsc;
use reedline_repl_rs::{CallBackMap, Error};
use std::{ops::Deref, process, thread};
pub type ReplCallBacks = CallBackMap<ReplContext, Error>;

pub fn get_callbacks() -> ReplCallBacks {
    let mut callbacks = ReplCallBacks::new();
    callbacks.insert("connect".to_string(), connect);
    callbacks.insert("list".to_string(), list);
    callbacks.insert("describe".to_string(), describe);
    callbacks.insert("head".to_string(), head);
    callbacks.insert("sql".to_string(), sql);

    callbacks
}
pub struct ReplContext {
    pub tx: mpsc::Sender<ReplCommand>,
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded();

        thread::Builder::new()
            .name("ReplContext".to_string())
            .spawn(move || {
                while let Ok(cmd) = rx.recv() {
                    match cmd {
                        ReplCommand::Connect(opts) => {
                            println!("Connect: {:?}", opts);
                        }
                        ReplCommand::List => {
                            println!("List");
                        }
                        ReplCommand::Describe(opts) => {
                            println!("Describe: {:?}", opts);
                        }
                        ReplCommand::Head(opts) => {
                            println!("Head: {:?}", opts);
                        }
                        ReplCommand::Sql(opts) => {
                            println!("Sql: {:?}", opts);
                        }
                    }
                }
            })
            .unwrap();

        Self { tx }
    }

    pub fn send(&self, cmd: ReplCommand) {
        if let Err(e) = self.tx.send(cmd) {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    }
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplCommand>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}
