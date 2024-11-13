use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(long, short, help = "The sql query to run on the dataset")]
    pub query: String,
}

pub fn sql(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let query = args
        .get_one::<String>("query")
        .expect("SQL query string is required")
        .to_string();

    let cmd = SqlOpts::new(query).into();
    ctx.send(cmd);

    Ok(None)
}
impl SqlOpts {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}

impl From<SqlOpts> for ReplCommand {
    fn from(opts: SqlOpts) -> Self {
        ReplCommand::Sql(opts)
    }
}
