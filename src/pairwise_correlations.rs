use chrono::NaiveDate;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[macro_use]
extern crate diesel;

mod api;
mod core_logic;
mod database;

use api::models::{AggregationPeriod, CorrelateDataPoint};
use core_logic::data_processing::correlate;
use database::models::DatasetMetadata;

use api::models::CorrelationMetric;
use polars::frame::DataFrame;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

async fn pre_process_datasets() -> (
    Arc<HashMap<std::string::String, DatasetMetadata>>,
    Arc<HashMap<std::string::String, polars::prelude::DataFrame>>,
) {
    let now = SystemTime::now();
    let datasets = database::queries::fetch_datasets::fetch_datasets().unwrap();
    let dataset_metadatas = database::queries::fetch_datasets::fetch_dataset_metadata().unwrap();

    let dataframes = Arc::new(core_logic::data_processing::create_dataframes(
        &datasets,
        &dataset_metadatas,
    ));
    let dataset_metadatas_map: Arc<HashMap<String, DatasetMetadata>> = Arc::new(
        dataset_metadatas
            .par_iter()
            .map(|metadata| (metadata.internal_name.clone(), metadata.to_owned()))
            .collect(),
    );

    println! {"Elapsed time fetching data {}", now.elapsed().unwrap().as_secs()}
    (dataset_metadatas_map, dataframes)
}

fn run_correlations(
    input_df: &DataFrame,
    transformed_dataframes: Arc<HashMap<String, DataFrame>>,
    dataset_metadatas_map: Arc<HashMap<std::string::String, DatasetMetadata>>,
) -> Vec<CorrelateDataPoint> {
    let mut correlations: Vec<CorrelateDataPoint> = transformed_dataframes
        .iter()
        .map(|(name2, df2)| {
            let metadata: &DatasetMetadata = dataset_metadatas_map.get(name2).unwrap();
            let title = match &metadata.external_name {
                Some(title) => title.clone(),
                _ => String::from(""),
            };
            let series_id = metadata.internal_name.clone();
            correlate(&input_df, df2, 0, metadata)
        })
        .flatten()
        .collect();

    correlations.retain(|x| x.pearson_value > 0.99);
    correlations.sort_by(|a, b: &CorrelateDataPoint| {
        b.pearson_value
            .partial_cmp(&a.pearson_value)
            .unwrap_or(Ordering::Equal)
    });
    correlations
}

#[tokio::main]
async fn main() {
    let (dataset_metadatas_map, dataframes) = pre_process_datasets().await;

    let pool = ThreadPoolBuilder::new()
        .stack_size(32 * 1024 * 1024) // 32 MB
        .build()
        .unwrap();

    let dataset_metadatas_map = dataset_metadatas_map;
    let dataframes = dataframes;

    println!("Finished pre-processing data");

    let transformed_dataframes: HashMap<String, DataFrame> = pool.install(|| {
        dataframes
            .par_iter()
            .map(|(name, df)| {
                let transformed_df = core_logic::data_processing::transform_data(
                    df,
                    AggregationPeriod::Quarterly,
                    12 as i8,
                    CorrelationMetric::RawValue,
                    NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
                );
                (name.clone(), transformed_df)
            })
            .collect()
    });

    println!("Finished transforming data");

    let transformed_dataframes = Arc::new(transformed_dataframes);
    let correlated_datasets: Arc<Mutex<HashMap<String, Vec<CorrelateDataPoint>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let mut file = Arc::new(Mutex::new(
        OpenOptions::new()
            .write(true)
            .append(true)
            .open("correlated_datasets.csv")
            .unwrap(),
    ));

    pool.install(|| {
        transformed_dataframes.par_iter().for_each(|(name, df)| {
            let correlations = run_correlations(
                &df,
                transformed_dataframes.clone(),
                dataset_metadatas_map.clone(),
            );

            if correlations.len() > 0 {
                let internal_names: Vec<String> = correlations
                    .iter()
                    .map(|dp| dp.internal_name.clone())
                    .collect();

                correlated_datasets
                    .lock()
                    .unwrap()
                    .insert(name.clone(), correlations);

                let record: Vec<String> = std::iter::once(name.clone())
                    .chain(internal_names)
                    .collect();

                writeln!(file.lock().unwrap(), "{}", record.join(",")).unwrap();

                println!(
                    "Finished correlations total {}",
                    correlated_datasets.lock().unwrap().len()
                );
            }
        })
    });

    println!("Finished correlations");

    // After the parallel processing
    let correlated_datasets = Arc::try_unwrap(correlated_datasets)
        .unwrap()
        .into_inner()
        .unwrap();

    // for (key, data_points) in correlated_datasets {
    //     let internal_names: Vec<String> = data_points
    //         .iter()
    //         .map(|dp| dp.internal_name.clone())
    //         .collect();
    //     let record: Vec<String> = std::iter::once(key).chain(internal_names).collect();
    //     wtr.lock().unwrap().write_record(&record).unwrap();
    // }

    // wtr.lock().unwrap().flush().unwrap();
}
