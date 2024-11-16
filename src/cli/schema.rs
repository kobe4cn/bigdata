use clap::{arg, ArgMatches, Parser};

use crate::{BackEnd, CmdExcutor, ReplContext, ReplDisplay};

use super::ReplResult;
#[derive(Debug, Parser)]
pub struct SchemaOpts {
    #[arg(help = "dataset name")]
    pub name: String,
}

impl SchemaOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

pub fn schema(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("Dataset Name is required")
        .to_string();

    let (msg, rx) = crate::ReplMsg::new(SchemaOpts::new(name));
    Ok(ctx.send(msg, rx))
}

impl CmdExcutor for SchemaOpts {
    async fn execute<T: BackEnd>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.schema(&self.name).await?;
        df.display().await
    }
}
