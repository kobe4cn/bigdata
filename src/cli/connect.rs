use super::ReplResult;
use crate::{CmdExcutor, ReplContext};
use clap::{ArgMatches, Parser};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Parquet(String),
    Csv(FileOpts),
    NdJson(FileOpts),
}

#[derive(Debug, Clone)]
pub struct FileOpts {
    pub filename: String,
    pub ext: String,
    pub compression: FileCompressionType,
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
    } else {
        //connect assets/users.ndjson -n users or connect assets/users.ndjson.gz -n users
        let conn_str = conn_str.rsplit('/').next().unwrap().to_string();
        let ext_split = conn_str.split('.');
        let count = ext_split.clone().count();
        let mut exts = ext_split.rev().take(2);
        let ext1 = exts.next();
        let ext2 = if count <= 2 { None } else { exts.next() };
        match (ext1, ext2) {
            (Some(ext1), Some(ext2)) => {
                let compression = match ext1 {
                    "gz" => FileCompressionType::GZIP,
                    "bz2" => FileCompressionType::BZIP2,
                    "xz" => FileCompressionType::XZ,
                    "zstd" => FileCompressionType::ZSTD,
                    v => return Err(format!("Invalid compression type: {}", v)),
                };
                let opts = FileOpts {
                    filename: s.to_string(),
                    ext: ext2.to_string(),
                    compression,
                };
                match ext2 {
                    "csv" => return Ok(DatasetConn::Csv(opts)),
                    "json" | "jsonl" | "ndjson" => return Ok(DatasetConn::NdJson(opts)),
                    v => return Err(format!("Invalid file type: {}", v)),
                }
            }
            (Some(ext1), None) => {
                let opts = FileOpts {
                    filename: s.to_string(),
                    ext: ext1.to_string(),
                    compression: FileCompressionType::UNCOMPRESSED,
                };
                match ext1 {
                    "csv" => return Ok(DatasetConn::Csv(opts)),

                    "json" | "jsonl" | "ndjson" => return Ok(DatasetConn::NdJson(opts)),
                    v => return Err(format!("Invalid file type: {}", v)),
                }
            }
            _ => Err(format!("Invalid connection string: {}", s)),
        }
    }

    // else {
    //     Err(format!("Invalid connection string: {}", s))
    // }
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

    let (msg, rx) = crate::ReplMsg::new(ConnectOpts::new(conn_str, table, name));

    Ok(ctx.send(msg, rx))
}

// impl From<ConnectOpts> for ReplCommand {
//     fn from(opts: ConnectOpts) -> Self {
//         ReplCommand::Connect(opts)
//     }
// }

impl ConnectOpts {
    pub fn new(conn_str: DatasetConn, table: Option<String>, name: String) -> Self {
        Self {
            conn_str,
            table,
            name,
        }
    }
}

impl CmdExcutor for ConnectOpts {
    async fn execute<T: crate::BackEnd>(self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(&self).await?;
        Ok(format!("Connected to dataset: {}", &self.name))
    }
}
