use crate::api::models::{
    AggregationPeriod, CorrelateDataPoint, CorrelationMetric, ManualDataInput,
};
use chrono::NaiveDate;
use ndarray_stats::CorrelationExt;
use polars::prelude::*;
use std::collections::HashMap;

use crate::database::models::{Dataset, DatasetMetadata};

pub fn transform_correlation_metric(
    df: DataFrame,
    correlation_metric: CorrelationMetric,
) -> DataFrame {
    let mut updated_df = df;
    if matches!(correlation_metric, CorrelationMetric::YoyGrowth) {
        updated_df = updated_df
            .clone()
            .lazy()
            .with_column((col("Value") / col("Value").shift(lit(4)) - lit(1)).alias("Value"))
            .collect()
            .unwrap();

        let mask = updated_df
            .column("Value")
            .expect("Value column should exist")
            .is_not_null();
        updated_df = updated_df.filter(&mask).unwrap();
    }
    updated_df
}

pub fn transform_data(
    df: &DataFrame,
    time_increment: AggregationPeriod,
    fiscal_year_end: i8,
    correlation_metric: CorrelationMetric,
    end_date: NaiveDate,
) -> DataFrame {
    // Group by aggregation period
    if df.shape().0 == 0 {
        return df.clone();
    }

    let lazy_df = df.clone().lazy().filter(col("Date").lt(lit(end_date)));

    let updated_df = match time_increment {
        AggregationPeriod::Quarterly => {
            let q_df = lazy_df
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
                .sort("Date", Default::default());

            q_df
        }
        AggregationPeriod::Annually => {
            // Implement Annually logic
            // ...
            lazy_df
        }
    }
    .collect()
    .unwrap();

    transform_correlation_metric(updated_df, correlation_metric)
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
        .unwrap()
        .sort(["Date"], false, false)
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
        let mut input_data_shifted: Vec<f64> = if i < input_data.len() {
            input_data[i..].to_vec()
        } else {
            break;
        };

        let mut dataset_data_shifted: Vec<f64> = if i < dataset_data.len() {
            dataset_data[..dataset_data.len() - i].to_vec()
        } else {
            break;
        };

        let mut correlation_matrix = ndarray::Array::zeros((0, input_data_shifted.len()));
        correlation_matrix
            .push_row(ndarray::ArrayView::from(&dataset_data_shifted))
            .unwrap();
        correlation_matrix
            .push_row(ndarray::ArrayView::from(&input_data_shifted))
            .unwrap();

        let pearson_correlation = correlation_matrix.pearson_correlation().unwrap()[[0, 1]];
        let mut lag_padding_vec = vec![0.0; i];
        let mut lag_start_vec = vec![0.0; i];

        // Append zeroes to the end of input_data and start of dataset_data to graph them correctly
        input_data_shifted.append(&mut lag_padding_vec);
        lag_start_vec.append(&mut dataset_data_shifted);

        correlate_data_points.push(CorrelateDataPoint {
            title: title.clone(),
            internal_name: series_id.clone(),
            pearson_value: pearson_correlation,
            lag: i,
            input_data: input_data_shifted,
            dataset_data: lag_start_vec,
            dates: dates.clone(),
        });
    }

    correlate_data_points
}

pub fn revenues_to_dataframe(
    revenues: HashMap<String, f64>,
    correlation_metric: CorrelationMetric,
) -> DataFrame {
    let (dates, values): (Vec<_>, Vec<_>) = revenues.into_iter().unzip();

    let date_series = Series::new("Date", dates);
    let value_series = Series::new("Value", values);

    // Create DataFrame
    let df = DataFrame::new(vec![date_series, value_series]).unwrap();
    let sorted_df = df.sort(["Date"], false, false).unwrap();

    transform_correlation_metric(sorted_df, correlation_metric)
}

pub fn manual_input_to_dataframe(manual_input: Vec<ManualDataInput>) -> DataFrame {
    let date_series: Vec<_> = manual_input
        .iter()
        .map(|inp: &ManualDataInput| -> String { inp.date.clone() })
        .collect::<Vec<String>>();

    let value_series: Vec<_> = manual_input
        .iter()
        .map(|inp: &ManualDataInput| -> f64 { inp.value })
        .collect::<Vec<f64>>();

    DataFrame::new(vec![
        Series::new("Date", date_series),
        Series::new("Value", value_series),
    ])
    .unwrap()
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
            Series::new(
                "Date",
                vec![
                    "2022-01-01",
                    "2022-02-01",
                    "2022-03-01",
                    "2022-04-01",
                    "2022-05-01",
                    "2022-06-01",
                ],
            ),
            Series::new("Value", vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
        ]);
        let df2 = DataFrame::new(vec![
            Series::new(
                "Date",
                vec![
                    "2022-01-01",
                    "2022-02-01",
                    "2022-03-01",
                    "2022-04-01",
                    "2022-05-01",
                    "2022-06-01",
                ],
            ),
            Series::new("Value", vec![9.0, 11.0, 10.0, 9.0, 8.0, 7.0]),
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
        assert_abs_diff_eq!(result.pearson_value, -0.755928946, epsilon = 1e-6);
        assert_eq!(result.lag, 0);
        assert_eq!(result.input_data, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        assert_eq!(result.dataset_data, vec![9.0, 11.0, 10.0, 9.0, 8.0, 7.0]);
        assert_eq!(
            result.dates,
            vec![
                "2022-01-01",
                "2022-02-01",
                "2022-03-01",
                "2022-04-01",
                "2022-05-01",
                "2022-06-01",
            ],
        );
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
            chrono::offset::Utc::now().date_naive(),
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
            chrono::offset::Utc::now().date_naive(),
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
    fn test_transform_data_end_year() {
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
            NaiveDate::from_ymd_opt(2020, 3, 2).unwrap(),
        );

        // Define the expected output DataFrame
        let expected = DataFrame::new(vec![
            Series::new("Date", &["2020Q4"]),
            Series::new("Value", &[60]),
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
            chrono::offset::Utc::now().date_naive(),
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

    #[test]
    fn test_revenues_to_dataframe() {
        // Create a vector of revenues
        let mut revenues = HashMap::new();

        // Randomly insert year values
        revenues.insert("2020Q2".into(), 1.0);
        revenues.insert("2020Q1".into(), 1.0);
        revenues.insert("2020Q4".into(), 1.0);
        revenues.insert("2020Q3".into(), 1.0);
        revenues.insert("2021Q3".into(), 4.0);
        revenues.insert("2021Q1".into(), 2.0);
        revenues.insert("2021Q2".into(), 3.0);
        revenues.insert("2021Q4".into(), 5.0);

        // Call revenues_to_dataframe with different values of CorrelationMetric
        let df_yoy_growth = revenues_to_dataframe(revenues.clone(), CorrelationMetric::YoyGrowth);
        let df_raw_value = revenues_to_dataframe(revenues, CorrelationMetric::RawValue);

        // Check that the returned DataFrames have the expected properties
        assert_eq!(df_raw_value.shape(), (8, 2));

        // yoy growth the first 4 values are dropped
        assert_eq!(df_yoy_growth.shape(), (4, 2));

        let expected_raw_value = DataFrame::new(vec![
            Series::new(
                "Date",
                &[
                    "2020Q1", "2020Q2", "2020Q3", "2020Q4", "2021Q1", "2021Q2", "2021Q3", "2021Q4",
                ],
            ),
            Series::new("Value", &[1.0, 1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0]),
        ])
        .unwrap();
        let expected_yoy_growth = DataFrame::new(vec![
            Series::new("Date", &["2021Q1", "2021Q2", "2021Q3", "2021Q4"]),
            Series::new("Value", &[1.0, 2.0, 3.0, 4.0]),
        ])
        .unwrap();

        assert_eq!(expected_raw_value, df_raw_value);
        assert_eq!(expected_yoy_growth, df_yoy_growth);
    }
}
