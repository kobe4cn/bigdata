use crate::{BackEnd, CmdExcutor, ReplContext, ReplDisplay};
use anyhow::Result;
use clap::{ArgMatches, Parser};

use super::{ReplCommand, ReplResult};
#[derive(Debug, Parser)]
pub struct ListOpts;

pub fn list(_args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let (msg, rx) = crate::ReplMsg::new(ReplCommand::List(ListOpts));
    Ok(ctx.send(msg, rx))
}

impl CmdExcutor for ListOpts {
    async fn execute<T: BackEnd>(self, backend: &mut T) -> Result<String> {
        let tables = backend.list().await?;
        tables.display().await
    }
}
// impl From<ListOpts> for ReplCommand {
//     fn from(_opts: ListOpts) -> Self {
//         ReplCommand::List(_opts)
//     }
// }
