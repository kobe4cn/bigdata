use std::{ops::Deref, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::{cast, concat},
    datatypes::{DataType, Field, Schema, SchemaRef},
    util::pretty::pretty_format_batches,
};
use datafusion::{
    functions_aggregate::{
        count::count,
        expr_fn::avg,
        median::median,
        min_max::{max, min},
        stddev::stddev,
        sum::sum,
    },
    prelude::{
        case, col, is_null, lit, CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions,
        SessionConfig, SessionContext,
    },
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
impl ReplDisplay for DataFrame {
    async fn display(self) -> Result<String> {
        let batch = self.collect().await?;
        let pretty_results = pretty_format_batches(&batch)?;

        Ok(pretty_results.to_string())
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
        let df = self.0.sql(&format!("select * from {}", name)).await?;
        let df = df.describe().await?;
        Ok(df)
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

impl ReplDisplay for RecordBatch {
    async fn display(self) -> anyhow::Result<String> {
        let data = pretty_format_batches(&[self])?;
        Ok(data.to_string())
    }
}
#[allow(unused)]
pub struct DescribeDataFrame {
    df: DataFrame,
    functions: &'static [&'static str],
    schema: SchemaRef,
}
#[allow(unused)]
impl DescribeDataFrame {
    pub fn new(df: DataFrame) -> Self {
        let functions = &["count", "null_count", "mean", "std", "min", "max", "median"];

        let original_schema_fields = df.schema().fields().iter();

        //define describe column
        let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];
        describe_schemas.extend(original_schema_fields.clone().map(|field| {
            if field.data_type().is_numeric() {
                Field::new(field.name(), DataType::Float64, true)
            } else {
                Field::new(field.name(), DataType::Utf8, true)
            }
        }));
        Self {
            df,
            functions,
            schema: Arc::new(Schema::new(describe_schemas)),
        }
    }

    pub fn count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| count(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    pub fn null_count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| {
                    sum(case(is_null(col(f.name())))
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap())
                    .alias(f.name())
                })
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    pub fn mean(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| avg(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    pub fn stddev(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| stddev(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    pub fn min(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| min(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    pub fn max(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| max(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    pub fn medium(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| median(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    pub async fn to_record_batch(&self) -> anyhow::Result<RecordBatch> {
        let original_schema_fields = self.df.schema().fields().iter();

        let batches = vec![
            self.count(),
            self.null_count(),
            self.mean(),
            self.stddev(),
            self.min(),
            self.max(),
            self.medium(),
        ];

        // first column with function names
        let mut describe_col_vec: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            self.functions
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>(),
        ))];
        for field in original_schema_fields {
            let mut array_data = vec![];
            for result in batches.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batchs = df.clone().collect().await;
                        match batchs {
                            Ok(batchs)
                                if batchs.len() == 1
                                    && batchs[0].column_by_name(field.name()).is_some() =>
                            {
                                let column = batchs[0].column_by_name(field.name()).unwrap();
                                if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    //Handling error when only boolean/binary column, and in other cases
                    Err(err)
                        if err.to_string().contains(
                            "Error during planning: \
                                            Aggregate requires at least one grouping \
                                            or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(other_err) => {
                        panic!("{other_err}")
                    }
                };
                array_data.push(array_ref);
            }
            describe_col_vec.push(concat(
                array_data
                    .iter()
                    .map(|af| af.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?);
        }

        let batch = RecordBatch::try_new(self.schema.clone(), describe_col_vec)?;

        Ok(batch)
    }
}
