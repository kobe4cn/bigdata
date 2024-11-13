use anyhow::Result;
use bigdata::{get_callbacks, ReplCommand, ReplContext};
use reedline_repl_rs::Repl;
const HISTORY_SIZE: usize = 1024;
fn main() -> Result<()> {
    let ctx = ReplContext::new();
    let callbacks = get_callbacks();
    let history_file = dirs::home_dir()
        .expect("except home dir")
        .join(".polars_history");

    let mut repl = Repl::new(ctx)
        .with_history(history_file, HISTORY_SIZE)
        .with_banner("Welcome polars cli")
        .with_derived::<ReplCommand>(callbacks);

    repl.run()?;
    Ok(())
}
