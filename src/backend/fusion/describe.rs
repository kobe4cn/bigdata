use core::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::{
    functions_aggregate::{
        count::count,
        expr_fn::{approx_percentile_cont, avg},
        median::median,
        min_max::{max, min},
        stddev::stddev,
        sum::sum,
    },
    prelude::{array_length, case, cast, col, is_null, length, lit, DataFrame},
};

pub struct DataFrameDescriber {
    original: DataFrame,
    transformed: DataFrame,
    methods: Vec<DescribeMethod>,
}

#[derive(Debug)]
pub enum DescribeMethod {
    Total,
    NullTotal,
    Min,
    Max,
    Mean,
    Median,
    Sttdev,
    Percentile(u8),
}
macro_rules! describe_method {
    ($name:ident,$method:ident) => {
        fn $name(df: DataFrame) -> anyhow::Result<DataFrame> {
            let schema_fields = df.schema().fields().iter();
            let ret = df.clone().aggregate(
                vec![],
                schema_fields
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| $method(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )?;
            Ok(ret)
        }
    };
}
describe_method!(total, count);
describe_method!(mean, avg);
describe_method!(st_ddev, stddev);
describe_method!(minimum, min);
describe_method!(maximum, max);
describe_method!(med, median);

impl DataFrameDescriber {
    pub fn try_new(df: DataFrame) -> anyhow::Result<Self> {
        let fields = df.schema().fields().iter();
        //change all temporal fields to float64
        let expressions = fields
            .map(|f| {
                let dt = f.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(f.name()), DataType::Float64),
                    dt if dt.is_numeric() => col(f.name()),
                    DataType::List(_) | DataType::LargeList(_) => array_length(col(f.name())),
                    _ => length(cast(col(f.name()), DataType::Utf8)),
                };
                expr.alias(f.name())
            })
            .collect();
        let transformed = df.clone().select(expressions)?;

        Ok(Self {
            original: df,
            transformed,
            methods: vec![
                DescribeMethod::Total,
                DescribeMethod::NullTotal,
                DescribeMethod::Min,
                DescribeMethod::Max,
                DescribeMethod::Mean,
                DescribeMethod::Median,
                DescribeMethod::Sttdev,
                DescribeMethod::Percentile(25),
                DescribeMethod::Percentile(75),
            ],
        })
    }

    pub async fn describe(&self) -> anyhow::Result<DataFrame> {
        let df = self.do_describe().await?;
        self.cast_back(df)
    }

    async fn do_describe(&self) -> anyhow::Result<DataFrame> {
        let df: Option<DataFrame> = self.methods.iter().fold(None, |acc, method| {
            let df = self.transformed.clone();
            let ret = match method {
                DescribeMethod::Total => total(df).unwrap(),
                DescribeMethod::NullTotal => null_total(df).unwrap(),
                DescribeMethod::Min => minimum(df).unwrap(),
                DescribeMethod::Max => maximum(df).unwrap(),
                DescribeMethod::Mean => mean(df).unwrap(),
                DescribeMethod::Median => med(df).unwrap(),
                DescribeMethod::Sttdev => st_ddev(df).unwrap(),
                DescribeMethod::Percentile(p) => percentile(df, *p).unwrap(),
            };
            let mut select_expr = vec![lit(method.to_string()).alias("describe")];
            select_expr.extend(ret.schema().fields().iter().map(|f| col(f.name())));
            let ret = ret.select(select_expr).unwrap();
            Some(match acc {
                Some(acc) => acc.union(ret).unwrap(),
                None => ret,
            })
        });

        df.ok_or_else(|| anyhow::anyhow!("No methods to describe"))
    }
    fn cast_back(&self, df: DataFrame) -> anyhow::Result<DataFrame> {
        // we need the describe column
        let describe = Arc::new(Field::new("describe", DataType::Utf8, false));
        let mut fields = vec![&describe];
        fields.extend(self.original.schema().fields().iter());
        let expressions = fields
            .into_iter()
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), dt.clone()),
                    DataType::List(_) | DataType::LargeList(_) => {
                        cast(col(field.name()), DataType::Int32)
                    }
                    _ => col(field.name()),
                };
                expr.alias(field.name())
            })
            .collect();

        Ok(df
            .select(expressions)?
            .sort(vec![col("describe").sort(true, false)])?)
    }
}

fn percentile(df: DataFrame, p: u8) -> anyhow::Result<DataFrame> {
    let schema_fields = df.schema().fields().iter();
    let ret = df.clone().aggregate(
        vec![],
        schema_fields
            .clone()
            .map(|f| {
                let expr = col(f.name());
                let p = p / 100;
                let percentile = approx_percentile_cont(expr, lit(p as f64), None);
                percentile.alias(f.name())
            })
            .collect::<Vec<_>>(),
    )?;
    Ok(ret)
}

fn null_total(df: DataFrame) -> anyhow::Result<DataFrame> {
    let schema_fields = df.schema().fields().iter();
    let ret = df.clone().aggregate(
        vec![],
        schema_fields
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

impl fmt::Display for DescribeMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DescribeMethod::Total => write!(f, "Total"),
            DescribeMethod::NullTotal => write!(f, "NullTotal"),
            DescribeMethod::Min => write!(f, "Min"),
            DescribeMethod::Max => write!(f, "Max"),
            DescribeMethod::Mean => write!(f, "Mean"),
            DescribeMethod::Median => write!(f, "Median"),
            DescribeMethod::Sttdev => write!(f, "Sttdev"),
            DescribeMethod::Percentile(p) => write!(f, "Percentile({})", p),
        }
    }
}
