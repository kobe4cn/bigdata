mod connect;
mod describe;
mod head;
mod list;
mod sql;
pub use self::connect::connect;
pub use self::describe::describe;
pub use self::head::head;
pub use self::list::list;
pub use self::sql::sql;
use clap::Parser;
use connect::ConnectOpts;
use describe::DescribeOpts;
use head::HeadOpts;
use sql::SqlOpts;
type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;

#[derive(Debug, Parser)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "connect to a dataset (postgres, parquet, csv, json)"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "list all registered dataset")]
    List,
    #[command(name = "describe", about = "show basic information for dataset")]
    Describe(DescribeOpts),
    #[command(name = "head", about = "get first 10 items for the dataset")]
    Head(HeadOpts),
    #[command(name = "sql", about = "run sql query on the dataset")]
    Sql(SqlOpts),
}
