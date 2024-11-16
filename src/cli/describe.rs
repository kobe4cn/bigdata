use clap::{ArgMatches, Parser};

use crate::{BackEnd, CmdExcutor, ReplContext, ReplDisplay};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(help = "The name of the dataset")]
    pub name: String,
}

pub fn describe(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("Dataset Name is required")
        .to_string();
    let (msg, rx) = crate::ReplMsg::new(DescribeOpts::new(name));
    Ok(ctx.send(msg, rx))
}
impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

// impl From<DescribeOpts> for ReplCommand {
//     fn from(opts: DescribeOpts) -> Self {
//         ReplCommand::Describe(opts)
//     }
// }

impl CmdExcutor for DescribeOpts {
    async fn execute<T: BackEnd>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.describe(&self.name).await?;
        df.display().await
    }
}
