use crate::api::models::CorrelateDataPoint;
use ndarray_stats::CorrelationExt;
use polars::prelude::*;
use std::collections::HashMap;

use crate::database::models::{AggregationPeriod, Dataset, DatasetMetadata};

pub fn transform_data(df: &DataFrame, time_increment: AggregationPeriod) -> DataFrame {
    // Group by aggregation period
    let q_df = match time_increment {
        AggregationPeriod::Quarterly => {
            let q_df = df
                .clone()
                .lazy()
                .with_column(
                    (col("Date").dt().year().cast(DataType::String)
                        + lit("Q")
                        + ((col("Date").dt().month() - lit(1)) / lit(3) + lit(1))
                            .cast(DataType::String))
                    .alias("Date"),
                )
                .group_by(vec![col("Date")])
                .agg(vec![col("Value").sum().alias("Value")])
                .sort("Date", Default::default())
                .collect()
                .unwrap();

            q_df
        }
        AggregationPeriod::Annually => {
            // Implement Annually logic
            // ...
            df.clone()
        }
    };

    q_df
}

pub fn create_dataframes(
    datasets: &Vec<Dataset>,
    metadata: &Vec<DatasetMetadata>,
) -> HashMap<String, DataFrame> {
    let mut dataframes: HashMap<String, DataFrame> = HashMap::new();

    for meta in metadata {
        let relevant_datasets: Vec<&Dataset> = datasets
            .iter()
            .filter(|d| d.metadata_id == meta.id)
            .collect();

        if relevant_datasets.is_empty() {
            continue;
        }
        // Transform relevant_datasets into a DataFrame
        let df = datasets_to_dataframe(relevant_datasets);

        // Insert into HashMap
        dataframes.insert(meta.internal_name.clone(), df);
    }

    dataframes
}

pub fn datasets_to_dataframe(datasets: Vec<&Dataset>) -> DataFrame {
    // Create Series for each column
    let date_series = Series::new(
        "Date",
        datasets
            .iter()
            .map(|d| d.date.timestamp() / 86_400)
            .collect::<Vec<i64>>(),
    );
    let value_series = Series::new(
        "Value",
        datasets.iter().map(|d| d.value).collect::<Vec<f64>>(),
    );

    // Create DataFrame
    DataFrame::new(vec![
        date_series.cast(&DataType::Date).unwrap(),
        value_series,
    ])
    .unwrap()
}

pub fn correlate(
    df1: &DataFrame,
    df2: &DataFrame,
    title: String,
    series_id: String,
    lag: i32,
) -> CorrelateDataPoint {
    // Joining df1 and df2 on "key"

    let combined_df = df1.inner_join(&df2, ["Date"], ["Date"]).unwrap();
    println!(
        "{} {} {}",
        df1.estimated_size(),
        df2.estimated_size(),
        combined_df.estimated_size(),
    );

    let dates: Vec<String> = combined_df
        .column("Date")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect();

    let nd_array = combined_df
        .select(vec!["Value", "Value_right"])
        .unwrap()
        .to_ndarray::<Float64Type>(IndexOrder::Fortran)
        .unwrap();

    let input_data: Vec<f64> = combined_df
        .column("Value")
        .unwrap()
        .f64()
        .unwrap()
        .iter()
        .map(|value| value.unwrap())
        .collect();
    let dataset_data: Vec<f64> = combined_df
        .column("Value_right")
        .unwrap()
        .f64()
        .unwrap()
        .iter()
        .map(|value| value.unwrap())
        .collect();

    let pearson_correlation = match nd_array.t().pearson_correlation() {
        Ok(correlation) => correlation[[0, 1]],
        Err(err) => {
            println!("{:?}", series_id);
            println!("{:?}", nd_array);
            println!("{}", err);
            0.0
        }
    };

    CorrelateDataPoint {
        title,
        internal_name: series_id,
        pearson_value: pearson_correlation,
        lag,
        input_data,
        dataset_data,
        dates,
    }
}

pub fn revenues_to_dataframe(revenues: HashMap<String, f64>) -> DataFrame {
    let (dates, values): (Vec<_>, Vec<_>) = revenues.into_iter().unzip();

    let date_series = Series::new("Date", dates);
    let value_series = Series::new("Value", values);

    // Create DataFrame
    let df = DataFrame::new(vec![
        date_series.cast(&DataType::Date).unwrap(),
        value_series,
    ])
    .unwrap();

    transform_data(&df, AggregationPeriod::Quarterly)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correlate() {
        // Arrange
        let df1 = DataFrame::new(vec![
            Series::new("Date", vec!["2022-01-01", "2022-01-02"]),
            Series::new("Value", vec![1.0, 2.0]),
        ]);
        let df2 = DataFrame::new(vec![
            Series::new("Date", vec!["2022-01-01", "2022-01-02"]),
            Series::new("Value", vec![3.0, 4.0]),
        ]);
        let series_id = "test_series".to_string();
        let lag = 0;

        // Act
        let result = correlate(
            &df1.unwrap(),
            &df2.unwrap(),
            String::from("title"),
            series_id,
            lag,
        );

        // Assert
        assert_eq!(result.title, "title");
        assert_eq!(result.internal_name, "test_series");
        assert_eq!(result.pearson_value, 1.0);
        assert_eq!(result.lag, 0);
        assert_eq!(result.input_data, vec![1.0, 2.0]);
        assert_eq!(result.dataset_data, vec![3.0, 4.0]);
        assert_eq!(result.dates, vec!["2022-01-01", "2022-01-02"]);
    }

    // Add more test functions for the other methods in this file
}
