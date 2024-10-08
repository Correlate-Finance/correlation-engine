use crate::api::models::{
    AggregationPeriod, CorrelateDataPoint, CorrelationMetric, ManualDataInput,
};
use chrono::NaiveDate;
use ndarray_stats::CorrelationExt;
use polars::{lazy::dsl::col, prelude::*};
use std::collections::HashMap;

use crate::database::models::{Dataset, DatasetMetadata};

const MIN_SAMPLES: usize = 8;

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

    let lazy_df = df.clone().lazy().filter(col("Date").lt_eq(lit(end_date)));

    let updated_df = match time_increment {
        AggregationPeriod::Quarterly => lazy_df
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
            .agg(vec![
                col("Value").sum().alias("Value"),
                col("Value").count().alias("Count"),
            ])
            // Remove any periods with less than 3 data points
            .filter(col("Count").gt_eq(lit(3)))
            // Remove the Count column
            .select(&[col("Date"), col("Value")])
            .sort("Date", Default::default()),
        AggregationPeriod::Annually => lazy_df
            .with_column(col("Date").dt().year().cast(DataType::String).alias("Date"))
            .group_by(vec![col("Date")])
            .agg(vec![col("Value").sum().alias("Value")])
            .sort("Date", Default::default()),
    }
    .collect()
    .unwrap();

    transform_correlation_metric(updated_df, correlation_metric)
}

pub fn create_dataframes(
    datasets: &[Dataset],
    metadata: &[DatasetMetadata],
) -> HashMap<String, DataFrame> {
    let dataset_vectors: HashMap<i64, Vec<&Dataset>> =
        datasets.iter().fold(HashMap::new(), |mut acc, dataset| {
            let metadata_id = dataset.metadata_id;
            let dataset_vector = acc.entry(metadata_id).or_default();
            dataset_vector.push(dataset);
            acc
        });
    let mut dataframes: HashMap<String, DataFrame> = HashMap::new();

    for meta in metadata {
        let dataset_vec = dataset_vectors.get(&meta.id);

        if let Some(dataset_vec) = dataset_vec {
            // Transform relevant_datasets into a DataFrame
            let df = datasets_to_dataframe(dataset_vec);

            // Insert into HashMap
            dataframes.insert(meta.internal_name.clone(), df);
        }
    }

    dataframes
}

pub fn datasets_to_dataframe(datasets: &[&Dataset]) -> DataFrame {
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
    lag: usize,
    metadata: &DatasetMetadata,
    include_data: bool,
) -> Vec<CorrelateDataPoint> {
    // Joining df1 and df2 on "key"
    let combined_df = dataset_df
        .inner_join(input_df, ["Date"], ["Date"])
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

        if dataset_data_shifted.len() < MIN_SAMPLES {
            break;
        }

        let mut correlation_matrix = ndarray::Array::zeros((0, input_data_shifted.len()));
        correlation_matrix
            .push_row(ndarray::ArrayView::from(&dataset_data_shifted))
            .unwrap();
        correlation_matrix
            .push_row(ndarray::ArrayView::from(&input_data_shifted))
            .unwrap();

        let pearson_correlation = correlation_matrix.pearson_correlation().unwrap()[[0, 1]];

        // If the std_dev of the dataset is 0, the correlation is not finite
        if !pearson_correlation.is_finite() {
            break;
        }

        let mut lag_padding_vec = vec![0.0; i];
        let mut lag_start_vec = vec![0.0; i];

        if include_data {
            // Append zeroes to the end of input_data and start of dataset_data to graph them correctly
            dataset_data_shifted.append(&mut lag_padding_vec);
            lag_start_vec.append(&mut input_data_shifted);
        } else {
            // Reset to empty vectors if we don't want to include data
            lag_start_vec = vec![];
            dataset_data_shifted = vec![];
        }

        correlate_data_points.push(CorrelateDataPoint {
            title: match &metadata.external_name {
                Some(title) => title.clone(),
                _ => metadata.internal_name.clone(),
            },
            internal_name: metadata.internal_name.clone(),
            source: match &metadata.source {
                Some(source) => source.clone(),
                _ => String::from(""),
            },
            url: match &metadata.url {
                Some(url) => url.clone(),
                _ => String::from(""),
            },
            units: match &metadata.units {
                Some(units) => units.clone(),
                _ => String::from(""),
            },
            release: match &metadata.release {
                Some(release) => release.clone(),
                _ => String::from(""),
            },
            pearson_value: pearson_correlation,
            lag: i,
            input_data: lag_start_vec,
            dataset_data: dataset_data_shifted,
            dates: dates.clone(),
            categories: match &metadata.categories {
                Some(categories) => categories
                    .iter()
                    .filter_map(|c| c.as_ref())
                    .cloned()
                    .collect(),
                _ => Vec::new(),
            },
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
    fn test_correlate_not_enough_data() {
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
        let lag = 0;
        let dataset_metadata = DatasetMetadata {
            id: 1,
            internal_name: "test_series".to_string(),
            external_name: Some("title".to_string()),
            description: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            category: None,
            high_level: false,
            source: None,
            group_popularity: None,
            popularity: None,
            hidden: false,
            sub_source: None,
            units: None,
            units_short: None,
            release: None,
            url: None,
            categories: None,
        };
        // Act
        let results = correlate(&df1.unwrap(), &df2.unwrap(), lag, &dataset_metadata, true);
        assert_eq!(results.len(), 0);
    }

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
                    "2022-07-01",
                    "2022-08-01",
                ],
            ),
            Series::new("Value", vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
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
                    "2022-07-01",
                    "2022-08-01",
                ],
            ),
            Series::new("Value", vec![9.0, 11.0, 10.0, 9.0, 8.0, 7.0, 11.0, 13.0]),
        ]);
        let lag = 0;

        let dataset_metadata = DatasetMetadata {
            id: 1,
            internal_name: "test_series".to_string(),
            external_name: Some("title".to_string()),
            description: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            category: None,
            high_level: false,
            source: None,
            group_popularity: None,
            popularity: None,
            hidden: false,
            sub_source: None,
            units: None,
            units_short: None,
            release: None,
            url: None,
            categories: None,
        };
        // Act
        let results = correlate(&df1.unwrap(), &df2.unwrap(), lag, &dataset_metadata, true);
        let result = &results[0];

        // Assert
        assert_eq!(result.title, "title");
        assert_eq!(result.internal_name, "test_series");
        assert_abs_diff_eq!(result.pearson_value, 0.27500954910, epsilon = 1e-6);
        assert_eq!(result.lag, 0);
        assert_eq!(
            result.input_data,
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        );
        assert_eq!(
            result.dataset_data,
            vec![9.0, 11.0, 10.0, 9.0, 8.0, 7.0, 11.0, 13.0]
        );
        assert_eq!(
            result.dates,
            vec![
                "2022-01-01",
                "2022-02-01",
                "2022-03-01",
                "2022-04-01",
                "2022-05-01",
                "2022-06-01",
                "2022-07-01",
                "2022-08-01",
            ],
        );
    }

    #[test]
    fn test_correlate_lag_1() {
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
                    "2022-07-01",
                    "2022-08-01",
                    "2022-09-01",
                ],
            ),
            Series::new("Value", vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
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
                    "2022-07-01",
                    "2022-08-01",
                    "2022-09-01",
                ],
            ),
            Series::new(
                "Value",
                vec![9.0, 11.0, 10.0, 9.0, 8.0, 7.0, 11.0, 13.0, 15.0],
            ),
        ]);
        let lag = 1;

        let dataset_metadata = DatasetMetadata {
            id: 1,
            internal_name: "test_series".to_string(),
            external_name: Some("title".to_string()),
            description: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            category: None,
            high_level: false,
            source: None,
            group_popularity: None,
            popularity: None,
            hidden: false,
            sub_source: None,
            units: None,
            units_short: None,
            release: None,
            url: None,
            categories: None,
        };

        // Act
        let results = correlate(&df1.unwrap(), &df2.unwrap(), lag, &dataset_metadata, true);

        assert_eq!(results.len(), 2);
        let result = &results[1];

        // Assert
        assert_eq!(result.title, "title");
        assert_eq!(result.internal_name, "test_series");
        assert_abs_diff_eq!(result.pearson_value, 0.2750095491, epsilon = 1e-6);
        assert_eq!(result.lag, 1);
        assert_eq!(
            result.input_data,
            vec![0.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        );
        assert_eq!(
            result.dataset_data,
            vec![9.0, 11.0, 10.0, 9.0, 8.0, 7.0, 11.0, 13.0, 0.0]
        );
        assert_eq!(
            result.dates,
            vec![
                "2022-01-01",
                "2022-02-01",
                "2022-03-01",
                "2022-04-01",
                "2022-05-01",
                "2022-06-01",
                "2022-07-01",
                "2022-08-01",
                "2022-09-01",
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
            12,
            CorrelationMetric::RawValue,
            chrono::offset::Utc::now().date_naive(),
        );

        // Define the expected output DataFrame
        let expected = DataFrame::new(vec![
            Series::new("Date", &["2020Q1", "2020Q2"]),
            Series::new("Value", &[60, 150]),
        ])
        .unwrap();

        // Assert that the result matches the expected output
        assert_eq!(result, expected);
    }

    #[test]
    fn test_transform_data_missing_point_in_quarter() {
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

        let expected = DataFrame::new(vec![
            Series::new("Date", &["2020Q1"]),
            Series::new("Value", &[60]),
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
