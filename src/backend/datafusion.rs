use std::ops::Deref;

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{
    CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions, SessionContext,
};

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
        Self(SessionContext::new())
    }
}
impl Default for DataFusionBackEnd {
    fn default() -> Self {
        Self::new()
    }
}
impl ReplDisplay for DataFrame {
    async fn display(self) -> Result<String> {
        let batch = self.collect().await?;
        let pretty_results = pretty_format_batches(&batch)?;

        Ok(pretty_results.to_string())
    }
}

impl BackEnd for DataFusionBackEnd {
    type DataFrame = datafusion::prelude::DataFrame;
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

    async fn list(&mut self) -> Result<Vec<String>> {
        let catalog = self.0.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table_name = schema.table_names();
        Ok(table_name)
    }
    async fn schema(&self, name: &str) -> Result<DataFrame> {
        let df = self.0.sql(&format!("DESCRIBE {}", name)).await?;
        Ok(df)
    }
    async fn describe(&self, name: &str) -> Result<Self::DataFrame> {
        let df = self.0.table(name).await?;
        let df = df.describe().await?;
        Ok(df)
    }

    async fn head(&self, opts: HeadOpts) -> Result<Self::DataFrame> {
        let df = self.0.table(opts.name).await?;
        Ok(df)
    }

    async fn sql(&self, sql: &str) -> Result<Self::DataFrame> {
        let df = self.0.sql(sql).await?;
        Ok(df)
    }
}

//parse the connection string(postgres://postgres:postgres@localhost:5432/state) to Hashmap
/*
HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("user".to_string(), "postgres".to_string()),
        ("db".to_string(), "postgres_db".to_string()),
        ("pass".to_string(), "password".to_string()),
        ("port".to_string(), "5432".to_string()),
        ("sslmode".to_string(), "disable".to_string()),
    ])
*/
