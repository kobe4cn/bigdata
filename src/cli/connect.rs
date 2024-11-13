use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Parquet(String),
    Csv(String),
    Json(String),
}
#[derive(Debug, Parser)]
pub struct ConnectOpts {
    /// Input file path
    #[arg(value_parser=verify_conn_str, required=true, help="Connection string to the dataset, e.g. postgres, parquet, csv, json")]
    pub conn_str: DatasetConn,

    #[arg(
        short,
        long,
        help = "If is the database, the name of the tables should be provided"
    )]
    pub table: Option<String>,

    #[arg(short, long, help = "The name of the dataset")]
    pub name: String,
}

fn verify_conn_str(s: &str) -> Result<DatasetConn, String> {
    let conn_str = s.to_string();
    if conn_str.starts_with("postgres://") {
        Ok(DatasetConn::Postgres(conn_str))
    } else if conn_str.ends_with(".parquet") {
        Ok(DatasetConn::Parquet(conn_str))
    } else if conn_str.ends_with(".csv") {
        Ok(DatasetConn::Csv(conn_str))
    } else if conn_str.ends_with(".json") {
        Ok(DatasetConn::Json(conn_str))
    } else {
        Err(format!("Invalid connection string: {}", s))
    }
}

pub fn connect(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let conn_str = args
        .get_one::<DatasetConn>("conn_str")
        .expect("Connection string is required")
        .to_owned();
    let table = args.get_one::<String>("table").map(|s| s.to_string());
    let name = args
        .get_one::<String>("name")
        .expect("Dataset Name is required")
        .to_string();

    let cmd = ConnectOpts::new(conn_str, table, name).into();
    ctx.send(cmd);

    Ok(None)
}

impl From<ConnectOpts> for ReplCommand {
    fn from(opts: ConnectOpts) -> Self {
        ReplCommand::Connect(opts)
    }
}

impl ConnectOpts {
    pub fn new(conn_str: DatasetConn, table: Option<String>, name: String) -> Self {
        Self {
            conn_str,
            table,
            name,
        }
    }
}
