use std::fs::File;

use anyhow::{Ok, Result};
use arrow::array::AsArray as _;
use datafusion::prelude::{col, ParquetReadOptions, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use polars::{prelude::*, sql::SQLContext};

const PQ_FILE: &str = "assets/sample.parquet";

#[tokio::main]
async fn main() -> Result<()> {
    read_with_parquet(PQ_FILE)?;
    read_with_datafusion(PQ_FILE).await?;
    read_with_polars(PQ_FILE)?;
    Ok(())
}

fn read_with_parquet(file: &str) -> Result<()> {
    let file = File::open(file)?;
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

    // let mut batches = Vec::new();
    for batch in parquet_reader {
        let batch = batch?;
        let emails = batch.column(0).as_string::<i32>();
        for _email in emails {

            // println!("{:?}",email);
        }
        // println!("{:?}",emails);
        // batches.push(batch);
    }

    // print_batches(&batches)?;
    Ok(())
}
async fn read_with_datafusion(file: &str) -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.read_parquet(file, ParquetReadOptions::new()).await?;
    // println!("{:?}",df.schema());
    let _df = df.select(vec![col("email")])?;
    // println!("{:?}",df.show().await?);

    // let result:Vec<RecordBatch>=df.collect().await?;
    // let pretty_results=arrow::util::pretty::print_batches(&result)?;
    // println!("{:?}",pretty_results);
    Ok(())
}
fn read_with_polars(file: &str) -> Result<()> {
    // let lf = LazyFrame::scan_parquet(file, Default::default())?;
    let lf = LazyFrame::scan_parquet(file, Default::default())?;
    let mut ctx = SQLContext::new();
    ctx.register("stats", lf);
    let df = ctx.execute("select * from stats")?.collect()?;

    println!("{:?}", df);

    Ok(())
}
