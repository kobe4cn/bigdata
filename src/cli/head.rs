use clap::{ArgMatches, Parser};

use crate::{BackEnd, CmdExcutor, ReplContext, ReplDisplay};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(long, help = "List the first n rows of the dataset")]
    pub name: String,
    #[arg(long, help = "The number of rows to show", default_value = "10")]
    pub n: Option<usize>,
}

pub fn head(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("Dataset Name is required")
        .to_string();
    let n = args.get_one::<usize>("n").map(|n| n.to_owned());

    let (msg, rx) = crate::ReplMsg::new(HeadOpts::new(name, n));
    Ok(ctx.send(msg, rx))
}
impl HeadOpts {
    pub fn new(name: String, n: Option<usize>) -> Self {
        Self { name, n }
    }
}

// impl From<HeadOpts> for ReplCommand {
//     fn from(opts: HeadOpts) -> Self {
//         ReplCommand::Head(opts)
//     }
// }

impl CmdExcutor for HeadOpts {
    async fn execute<T: BackEnd>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.head(self).await?;
        df.display().await
    }
}
