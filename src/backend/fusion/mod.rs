use std::ops::Deref;
mod describe;
mod df_describe;

use datafusion::prelude::{
    CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext,
};
use describe::DataFrameDescriber;

use crate::{
    cli::{ConnectOpts, HeadOpts},
    BackEnd, ReplDisplay,
};
use anyhow::Result;
pub struct DataFusionBackEnd(SessionContext);

impl Deref for DataFusionBackEnd {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DataFusionBackEnd {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;

        let ctx = SessionContext::new_with_config(config);
        Self(ctx)
    }
}
impl Default for DataFusionBackEnd {
    fn default() -> Self {
        Self::new()
    }
}

impl BackEnd for DataFusionBackEnd {
    async fn connect(&mut self, opts: &ConnectOpts) -> Result<()> {
        match &opts.conn_str {
            crate::DatasetConn::Postgres(_conn_str) => {
                println!("Postgres: {:?}", _conn_str);
            }
            crate::DatasetConn::Parquet(conn_str) => {
                self.register_parquet(&opts.name, conn_str, ParquetReadOptions::default())
                    .await?;
            }
            crate::DatasetConn::Csv(file_opts) => {
                let cvsopts = CsvReadOptions::default()
                    .file_compression_type(file_opts.compression)
                    .file_extension(&file_opts.ext);
                self.register_csv(&opts.name, &file_opts.filename, cvsopts)
                    .await?;
            }

            crate::DatasetConn::NdJson(file_opts) => {
                let jsonopts = NdJsonReadOptions::default()
                    .file_compression_type(file_opts.compression)
                    .file_extension(&file_opts.ext);
                self.register_json(&opts.name, &file_opts.filename, jsonopts)
                    .await?;
            }
        }
        // println!("Connect: {:?}", opts);
        Ok(())
    }

    async fn list(&mut self) -> Result<impl ReplDisplay> {
        // let catalog = self.0.catalog("datafusion").unwrap();
        // let schema = catalog.schema("public").unwrap();
        // let table_name = schema.table_names();
        let df = self.0.sql("select table_name,table_type from information_schema.tables where table_schema='public'").await?;
        Ok(df)
    }
    async fn schema(&self, name: &str) -> Result<impl ReplDisplay> {
        let df = self.0.sql(&format!("DESCRIBE {}", name)).await?;
        Ok(df)
    }
    async fn describe(&self, name: &str) -> Result<impl ReplDisplay> {
        // let df = self.0.sql(&format!("select * from {}", name)).await?;
        // let df = df.describe().await?;
        // Ok(df)
        let df = self.0.sql(&format!("select * from {}", name)).await?;
        // let df = df.describe().await?;
        // let ddf = DescribeDataFrame::new(df);
        // let record_batch = ddf.to_record_batch().await?;
        let ddf = DataFrameDescriber::try_new(df)?;
        let record_batch = ddf.describe().await?;
        println!("{:?}", record_batch);
        Ok(record_batch)
    }

    async fn head(&self, opts: HeadOpts) -> Result<impl ReplDisplay> {
        let df = self
            .0
            .sql(format!("select * from {} limit {}", opts.name, opts.n.unwrap_or(10)).as_str())
            .await?;
        Ok(df)
    }

    async fn sql(&self, sql: &str) -> Result<impl ReplDisplay> {
        let df = self.0.sql(sql).await?;
        Ok(df)
    }
}
