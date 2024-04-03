use crate::api::models::{AggregationPeriod, CorrelateDataPoint, CorrelationMetric};
use ndarray_stats::CorrelationExt;
use polars::prelude::*;
use std::collections::HashMap;

use crate::database::models::{Dataset, DatasetMetadata};

pub fn transform_data(
    df: &DataFrame,
    time_increment: AggregationPeriod,
    fiscal_year_end: i8,
    correlation_metric: CorrelationMetric,
) -> DataFrame {
    // Group by aggregation period
    if df.shape().0 == 0 {
        return df.clone();
    }

    let mut q_df = match time_increment {
        AggregationPeriod::Quarterly => {
            let q_df = df
                .clone()
                .lazy()
                .with_column(
                    (when(col("Date").dt().month().gt(fiscal_year_end))
                        .then((col("Date").dt().year() + lit(1)).cast(DataType::String))
                        .otherwise(col("Date").dt().year().cast(DataType::String))
                        + lit("Q")
                        + (((col("Date").dt().month() - lit(1) - lit(fiscal_year_end)) % lit(12))
                            / lit(3)
                            + lit(1))
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

    if matches!(correlation_metric, CorrelationMetric::YoyGrowth) {
        q_df = q_df
            .clone()
            .lazy()
            .with_column((col("Value") / col("Value").shift(lit(4)) - lit(1)).alias("Value"))
            .collect()
            .unwrap();

        let mask = q_df
            .column("Value")
            .expect("Value column should exist")
            .is_not_null();
        q_df = q_df.filter(&mask).unwrap();
    }

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
    input_df: &DataFrame,
    dataset_df: &DataFrame,
    title: String,
    series_id: String,
    lag: usize,
) -> Vec<CorrelateDataPoint> {
    // Joining df1 and df2 on "key"
    let combined_df = dataset_df
        .inner_join(&input_df, ["Date"], ["Date"])
        .unwrap();

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

    let input_data: Vec<f64> = nd_array.column(1).to_vec();
    let dataset_data: Vec<f64> = nd_array.column(0).to_vec();
    let mut correlate_data_points = Vec::new();

    for i in 0..(lag + 1) {
        let input_data_shifted: Vec<f64> = if i < input_data.len() {
            input_data[i..].to_vec()
        } else {
            break;
        };

        let dataset_data_shifted: Vec<f64> = if i < dataset_data.len() {
            dataset_data[..dataset_data.len() - i].to_vec()
        } else {
            break;
        };

        let correlation_matrix = ndarray::Array::from_shape_vec(
            (input_data_shifted.len(), 2),
            input_data_shifted
                .iter()
                .cloned()
                .chain(dataset_data_shifted.iter().cloned())
                .collect(),
        )
        .unwrap()
        .t()
        .pearson_correlation()
        .unwrap();

        let pearson_correlation = correlation_matrix[[0, 1]];

        correlate_data_points.push(CorrelateDataPoint {
            title: title.clone(),
            internal_name: series_id.clone(),
            pearson_value: pearson_correlation,
            lag: i,
            input_data: input_data_shifted,
            dataset_data: dataset_data_shifted,
            dates: dates.clone(),
        });
    }

    correlate_data_points
}

pub fn revenues_to_dataframe(revenues: HashMap<String, f64>) -> DataFrame {
    let (dates, values): (Vec<_>, Vec<_>) = revenues.into_iter().unzip();

    let date_series = Series::new("Date", dates);
    let value_series = Series::new("Value", values);

    // Create DataFrame
    let df = DataFrame::new(vec![date_series, value_series]).unwrap();
    df
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;
    use chrono::NaiveDate;

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
        let results = correlate(
            &df1.unwrap(),
            &df2.unwrap(),
            String::from("title"),
            series_id,
            lag,
        );
        let result = &results[0];

        // Assert
        assert_eq!(result.title, "title");
        assert_eq!(result.internal_name, "test_series");
        assert_eq!(result.pearson_value, 1.0);
        assert_eq!(result.lag, 0);
        assert_eq!(result.input_data, vec![1.0, 2.0]);
        assert_eq!(result.dataset_data, vec![3.0, 4.0]);
        assert_eq!(result.dates, vec!["2022-01-01", "2022-01-02"]);
    }

    #[test]
    fn test_transform_data() {
        // Create a sample DataFrame
        let df = DataFrame::new(vec![
            Series::new(
                "Date",
                vec![
                    NaiveDate::from_ymd_opt(2020, 1, 1),
                    NaiveDate::from_ymd_opt(2020, 2, 1),
                    NaiveDate::from_ymd_opt(2020, 3, 1),
                    NaiveDate::from_ymd_opt(2020, 4, 1),
                    NaiveDate::from_ymd_opt(2020, 5, 1),
                ],
            ),
            Series::new("Value", &[10, 20, 30, 40, 50]),
        ])
        .unwrap();

        // Call the transform_data function
        let result = transform_data(
            &df,
            AggregationPeriod::Quarterly,
            12,
            CorrelationMetric::RawValue,
        );

        // Define the expected output DataFrame
        let expected = DataFrame::new(vec![
            Series::new("Date", &["2020Q1", "2020Q2"]),
            Series::new("Value", &[60, 90]),
        ])
        .unwrap();

        // Assert that the result matches the expected output
        assert_eq!(result, expected);
    }

    #[test]
    fn test_transform_data_fiscal_year_end() {
        // Create a sample DataFrame
        let df = DataFrame::new(vec![
            Series::new(
                "Date",
                vec![
                    NaiveDate::from_ymd_opt(2020, 1, 1),
                    NaiveDate::from_ymd_opt(2020, 2, 1),
                    NaiveDate::from_ymd_opt(2020, 3, 1),
                    NaiveDate::from_ymd_opt(2020, 4, 1),
                    NaiveDate::from_ymd_opt(2020, 5, 1),
                    NaiveDate::from_ymd_opt(2020, 6, 1),
                ],
            ),
            Series::new("Value", &[10, 20, 30, 40, 50, 60]),
        ])
        .unwrap();

        // Call the transform_data function
        let result = transform_data(
            &df,
            AggregationPeriod::Quarterly,
            3,
            CorrelationMetric::RawValue,
        );

        // Define the expected output DataFrame
        let expected = DataFrame::new(vec![
            Series::new("Date", &["2020Q4", "2021Q1"]),
            Series::new("Value", &[60, 150]),
        ])
        .unwrap();

        // Assert that the result matches the expected output
        assert_eq!(result, expected);
    }

    #[test]
    fn test_transform_data_correlation_metric() {
        // Create a sample DataFrame
        let df = DataFrame::new(vec![
            Series::new(
                "Date",
                vec![
                    NaiveDate::from_ymd_opt(2020, 1, 1),
                    NaiveDate::from_ymd_opt(2020, 2, 1),
                    NaiveDate::from_ymd_opt(2020, 3, 1),
                    NaiveDate::from_ymd_opt(2020, 4, 1),
                    NaiveDate::from_ymd_opt(2020, 5, 1),
                    NaiveDate::from_ymd_opt(2020, 6, 1),
                    NaiveDate::from_ymd_opt(2020, 7, 1),
                    NaiveDate::from_ymd_opt(2020, 8, 1),
                    NaiveDate::from_ymd_opt(2020, 9, 1),
                    NaiveDate::from_ymd_opt(2020, 10, 1),
                    NaiveDate::from_ymd_opt(2020, 11, 1),
                    NaiveDate::from_ymd_opt(2020, 12, 1),
                    NaiveDate::from_ymd_opt(2021, 1, 1),
                    NaiveDate::from_ymd_opt(2021, 2, 1),
                    NaiveDate::from_ymd_opt(2021, 3, 1),
                    NaiveDate::from_ymd_opt(2021, 4, 1),
                    NaiveDate::from_ymd_opt(2021, 5, 1),
                    NaiveDate::from_ymd_opt(2021, 6, 1),
                    NaiveDate::from_ymd_opt(2021, 7, 1),
                    NaiveDate::from_ymd_opt(2021, 8, 1),
                    NaiveDate::from_ymd_opt(2021, 9, 1),
                    NaiveDate::from_ymd_opt(2021, 10, 1),
                    NaiveDate::from_ymd_opt(2021, 11, 1),
                    NaiveDate::from_ymd_opt(2021, 12, 1),
                ],
            ),
            Series::new("Value", &(1..25).map(|x| x as f64).collect::<Vec<f64>>()),
        ])
        .unwrap();

        // Call the transform_data function
        let result = transform_data(
            &df,
            AggregationPeriod::Quarterly,
            12,
            CorrelationMetric::YoyGrowth,
        );

        // Define the expected output DataFrame

        let expected = DataFrame::new(vec![
            Series::new("Date", &["2021Q1", "2021Q2", "2021Q3", "2021Q4"]),
            Series::new("Value", &[6.0, 2.4, 1.5, 1.09090909]),
        ])
        .unwrap();

        assert_eq!(
            result.column("Date").unwrap(),
            expected.column("Date").unwrap()
        );

        let result_values = result.column("Value").unwrap();
        let expected_values = expected.column("Value").unwrap();
        for (result_value, expected_value) in result_values
            .f64()
            .unwrap()
            .iter()
            .zip(expected_values.f64().unwrap().iter())
        {
            if let (Some(result_value), Some(expected_value)) = (result_value, expected_value) {
                assert_abs_diff_eq!(result_value, expected_value, epsilon = 1e-6);
            }
        }
    }
}
