use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

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

    let cmd = HeadOpts::new(name, n).into();
    ctx.send(cmd);

    Ok(None)
}
impl HeadOpts {
    pub fn new(name: String, n: Option<usize>) -> Self {
        Self { name, n }
    }
}

impl From<HeadOpts> for ReplCommand {
    fn from(opts: HeadOpts) -> Self {
        ReplCommand::Head(opts)
    }
}
