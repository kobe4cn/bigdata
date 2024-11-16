use clap::{ArgMatches, Parser};

use crate::{CmdExcutor, ReplContext, ReplDisplay};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(help = "The sql query to run on the dataset")]
    pub query: String,
}

pub fn sql(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let query = args
        .get_one::<String>("query")
        .expect("SQL query string is required")
        .to_string();

    let (msg, rx) = crate::ReplMsg::new(SqlOpts::new(query));
    Ok(ctx.send(msg, rx))
}
impl SqlOpts {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}
impl CmdExcutor for SqlOpts {
    async fn execute<T: crate::BackEnd>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.sql(&self.query).await?;
        df.display().await
    }
}

// impl From<SqlOpts> for ReplCommand {
//     fn from(opts: SqlOpts) -> Self {
//         ReplCommand::Sql(opts)
//     }
// }
