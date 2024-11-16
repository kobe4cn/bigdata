mod connect;
mod describe;
mod head;
mod list;
mod sql;
pub use self::connect::connect;
pub use self::describe::describe;
pub use self::head::head;
pub use self::list::list;
pub use self::schema::schema;
pub use self::sql::sql;
mod schema;
use clap::Parser;
pub use connect::*;
pub use describe::DescribeOpts;
use enum_dispatch::enum_dispatch;
pub use head::HeadOpts;
pub use list::ListOpts;
pub use schema::SchemaOpts;
pub use sql::SqlOpts;

type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;

#[derive(Debug, Parser)]
#[enum_dispatch(CmdExcutor)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "connect to a dataset (postgres, parquet, csv, json)"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "list all registered dataset")]
    List(ListOpts),
    #[command(name = "schema", about = "show basic information for dataset")]
    Schema(SchemaOpts),
    #[command(name = "describe", about = "show describe for dataset")]
    Describe(DescribeOpts),
    #[command(name = "head", about = "get first 10 items for the dataset")]
    Head(HeadOpts),
    #[command(name = "sql", about = "run sql query on the dataset")]
    Sql(SqlOpts),
}
