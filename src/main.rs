#[macro_use]
extern crate diesel;

mod adapters;
mod api;
mod core_logic;
mod database;

use adapters::discounting_cash_flows::fetch_stock_revenues;
use adapters::discounting_cash_flows::InternalServerError;
use api::lib::{correlation_metric_from_str, month_index};
use api::models::AggregationPeriod;
use api::models::{CorrelateDataPoint, CorrelationData};
use core_logic::data_processing::correlate;
use core_logic::data_processing::revenues_to_dataframe;
use database::models::DatasetMetadata;

use futures::future::FutureExt;
use futures::future::Shared;
use polars::frame::DataFrame;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::SystemTime;
use warp::Filter;

#[derive(Deserialize, Clone)]
struct RevenueParameters {
    stock: String,
    start_year: i32,
    aggregation_period: String,
    lag_periods: usize,
    correlation_metric: String,
}

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

async fn correlate_view(
    pre_process_future: Shared<
        impl std::future::Future<
            Output = (
                Arc<HashMap<std::string::String, DatasetMetadata>>,
                Arc<HashMap<std::string::String, polars::prelude::DataFrame>>,
            ),
        >,
    >,
    revenues: HashMap<String, f64>,
    fiscal_year_end: Option<u32>,
    params: RevenueParameters,
) -> warp::reply::Json {
    let correlation_metric = correlation_metric_from_str(params.correlation_metric.clone());
    let (dataset_metadatas_map, dataframes) = pre_process_future.await;
    let dataframes = Arc::clone(&dataframes);
    let dataset_metadatas_map = Arc::clone(&dataset_metadatas_map);

    let pool = ThreadPoolBuilder::new()
        .stack_size(32 * 1024 * 1024) // 32 MB
        .build()
        .unwrap();

    let correlations: Vec<_> = pool.install(|| {
        // Fetch the revenues for the stock
        let now = SystemTime::now();

        let transformed_dataframes: Arc<HashMap<String, DataFrame>> =
            Arc::new(pool.install(|| {
                let transformed_dataframes = dataframes
                    .par_iter()
                    .map(|(name, df)| {
                        let transformed_df = core_logic::data_processing::transform_data(
                            df,
                            AggregationPeriod::Quarterly,
                            fiscal_year_end.unwrap_or(12) as i8,
                            correlation_metric,
                        );
                        (name.clone(), transformed_df)
                    })
                    .collect();
                transformed_dataframes
            }));

        println! {"Elapsed time {}", now.elapsed().unwrap().as_secs()}

        let df1 = revenues_to_dataframe(revenues, correlation_metric);
        let mut correlations: Vec<CorrelateDataPoint> = transformed_dataframes
            .par_iter()
            .map(|(name2, df2)| {
                let metadata: &DatasetMetadata = dataset_metadatas_map.get(name2).unwrap();
                let title = match &metadata.external_name {
                    Some(title) => title.clone(),
                    _ => String::from(""),
                };
                let series_id = metadata.internal_name.clone();
                let correlations = correlate(&df1, df2, title, series_id, params.lag_periods);
                correlations
            })
            .flatten()
            .collect();

        correlations.sort_by(|a, b: &CorrelateDataPoint| {
            b.pearson_value
                .partial_cmp(&a.pearson_value)
                .unwrap_or(Ordering::Equal)
        });

        correlations.truncate(100);
        correlations
    });

    warp::reply::json(&CorrelationData {
        data: correlations,
        aggregation_period: params.aggregation_period,
        correlation_metric: params.correlation_metric,
    })
}

#[tokio::main]
async fn main() {
    println!("Starting set up");

    let pre_process_future = pre_process_datasets();
    let shared_future = pre_process_future.shared();

    // Run the future in the background
    tokio::spawn(shared_future.clone());

    let revenue_route = warp::path("revenue")
        .and(warp::query::<RevenueParameters>())
        .and_then(|params: RevenueParameters| async move {
            let aggregation_period = match params.aggregation_period.as_str() {
                "Quarterly" => AggregationPeriod::Quarterly,
                _ => AggregationPeriod::Annually,
            };

            let result =
                fetch_stock_revenues(&params.stock, params.start_year, aggregation_period).await;
            match result {
                Ok((revenues, _)) => Ok(warp::reply::json(&revenues)),
                Err(_) => Err(warp::reject::custom(InternalServerError)),
            }
        });

    let correlate_route = warp::path("correlate")
        .and(warp::query::<RevenueParameters>())
        .and_then(move |params: RevenueParameters| async move {
            let aggregation_period = match params.aggregation_period.as_str() {
                "Quarterly" => AggregationPeriod::Quarterly,
                _ => AggregationPeriod::Annually,
            };

            let result =
                fetch_stock_revenues(&params.stock, params.start_year, aggregation_period).await;

            // TODO: Use fiscal year end when we implement it
            match result {
                Ok(value) => Ok((value.0, value.1, params)),
                Err(_) => return Err(warp::reject::custom(InternalServerError)),
            }
        })
        .then(move |(revenues, fiscal_year_end, params)| {
            correlate_view(shared_future.clone(), revenues, fiscal_year_end, params)
        });

    // Start the webserver
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "8001".to_string())
        .parse()
        .expect("PORT must be a number");

    println!("Starting web server! on {}", port);
    warp::serve(revenue_route.or(correlate_route))
        .run(([0, 0, 0, 0], port))
        .await;
}
