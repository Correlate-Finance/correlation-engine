#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[macro_use]
extern crate diesel;

mod adapters;
mod api;
mod core_logic;
mod database;

use adapters::discounting_cash_flows::fetch_stock_revenues;
use adapters::discounting_cash_flows::InternalServerError;
use api::lib::correlation_metric_from_str;
use api::models::{
    AggregationPeriod, CorrelateAutomaticRequestParameters, CorrelateDataPoint, CorrelateInputBody,
    CorrelateRequestParameters, CorrelationData, RevenueRequestParameters,
};
use core_logic::data_processing::{correlate, manual_input_to_dataframe, revenues_to_dataframe};
use database::models::DatasetMetadata;

use chrono::Datelike;
use futures::future::FutureExt;
use futures::future::Shared;
use polars::frame::DataFrame;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::SystemTime;
use warp::Filter;

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
    revenues: DataFrame,
    params: CorrelateRequestParameters,
    selected_datasets: Vec<String>,
) -> warp::reply::Json {
    let correlation_metric = correlation_metric_from_str(params.correlation_metric.clone());
    let (dataset_metadatas_map, dataframes) = pre_process_future.await;
    let dataframes = Arc::clone(&dataframes);
    let dataset_metadatas_map = Arc::clone(&dataset_metadatas_map);

    let end_date: chrono::NaiveDate = match params.end_year {
        Some(year) => chrono::NaiveDate::from_ymd_opt(year, 12, 31).unwrap(),
        None => chrono::offset::Utc::now().date_naive(),
    };

    let pool = ThreadPoolBuilder::new()
        .stack_size(32 * 1024 * 1024) // 32 MB
        .build()
        .unwrap();

    let correlations: Vec<_> = pool.install(|| {
        // Fetch the revenues for the stock
        let now = SystemTime::now();

        let filtered_dataframes: Arc<HashMap<String, DataFrame>> = if !selected_datasets.is_empty()
        {
            Arc::new(
                dataframes
                    .iter()
                    .filter(|(name, _)| selected_datasets.contains(name))
                    .map(|(name, df)| (name.clone(), df.clone()))
                    .collect::<HashMap<_, _>>(),
            )
        } else {
            dataframes
        };

        let transformed_dataframes: Arc<HashMap<String, DataFrame>> =
            Arc::new(pool.install(|| {
                filtered_dataframes
                    .par_iter()
                    .map(|(name, df)| {
                        let transformed_df = core_logic::data_processing::transform_data(
                            df,
                            AggregationPeriod::Quarterly,
                            params.fiscal_year_end.unwrap_or(12) as i8,
                            correlation_metric,
                            end_date,
                        );
                        (name.clone(), transformed_df)
                    })
                    .collect()
            }));

        println! {"Elapsed time {}", now.elapsed().unwrap().as_secs()}

        let mut correlations: Vec<CorrelateDataPoint> = transformed_dataframes
            .par_iter()
            .map(|(name2, df2)| {
                let metadata: &DatasetMetadata = dataset_metadatas_map.get(name2).unwrap();
                let title = match &metadata.external_name {
                    Some(title) => title.clone(),
                    _ => String::from(""),
                };
                let series_id = metadata.internal_name.clone();
                correlate(&revenues, df2, title, series_id, params.lag_periods)
            })
            .flatten()
            .collect();

        correlations.sort_by(|a, b: &CorrelateDataPoint| {
            b.pearson_value
                .partial_cmp(&a.pearson_value)
                .unwrap_or(Ordering::Equal)
        });

        correlations.truncate(1000);
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

    let shared1 = shared_future.clone();
    let shared2 = shared_future.clone();

    // Run the future in the background
    tokio::spawn(shared_future.clone());

    let revenue_route = warp::path("revenue")
        .and(warp::query::<RevenueRequestParameters>())
        .and_then(|params: RevenueRequestParameters| async move {
            let aggregation_period = match params.aggregation_period.as_str() {
                "Quarterly" => AggregationPeriod::Quarterly,
                _ => AggregationPeriod::Annually,
            };
            let end_year: i32 = match params.end_year {
                Some(year) => year,
                None => chrono::Utc::now().date_naive().year(),
            };

            let result = fetch_stock_revenues(
                &params.stock,
                params.start_year,
                end_year,
                aggregation_period,
            )
            .await;
            match result {
                Ok((revenues, _)) => Ok(warp::reply::json(&revenues)),
                Err(_) => Err(warp::reject::custom(InternalServerError)),
            }
        });

    let correlate_route = warp::path("correlate")
        .and(warp::query::<CorrelateAutomaticRequestParameters>())
        .and_then(
            move |params: CorrelateAutomaticRequestParameters| async move {
                let aggregation_period = match params.aggregation_period.as_str() {
                    "Quarterly" => AggregationPeriod::Quarterly,
                    _ => AggregationPeriod::Annually,
                };
                let end_year: i32 = match params.end_year {
                    Some(year) => year,
                    None => chrono::Utc::now().date_naive().year(),
                };

                let result = fetch_stock_revenues(
                    &params.stock,
                    params.start_year,
                    end_year,
                    aggregation_period,
                )
                .await;

                // TODO: Use fiscal year end when we implement it
                match result {
                    Ok(value) => Ok((value.0, value.1, params)),
                    Err(_) => Err(warp::reject::custom(InternalServerError)),
                }
            },
        )
        .then(
            move |(revenues, fiscal_year_end, params): (
                HashMap<String, f64>,
                Option<u32>,
                CorrelateAutomaticRequestParameters,
            )| {
                let correlation_metric =
                    correlation_metric_from_str(params.correlation_metric.clone());
                let revenue_df = revenues_to_dataframe(revenues, correlation_metric);
                let correlate_params = CorrelateRequestParameters {
                    start_year: params.start_year,
                    end_year: params.end_year,
                    aggregation_period: params.aggregation_period,
                    lag_periods: params.lag_periods,
                    correlation_metric: params.correlation_metric,
                    fiscal_year_end,
                };
                correlate_view(shared1.clone(), revenue_df, correlate_params, vec![])
            },
        );

    let correlate_input_route = warp::post()
        .and(warp::path("correlate_input"))
        .and(warp::body::json())
        .and(warp::query::<CorrelateRequestParameters>())
        .then(
            move |body: CorrelateInputBody, params: CorrelateRequestParameters| {
                // You can now use api_parameters in your handler
                // ...
                let df = manual_input_to_dataframe(body.manual_input_dataset);
                correlate_view(shared2.clone(), df, params, body.selected_datasets)
            },
        );

    // Start the webserver
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "8001".to_string())
        .parse()
        .expect("PORT must be a number");

    println!("Starting web server! on {}", port);
    warp::serve(revenue_route.or(correlate_route).or(correlate_input_route))
        .run(([0, 0, 0, 0], port))
        .await;
}
