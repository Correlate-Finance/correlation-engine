use crate::{core_logic::data_processing::correlate, database::models::AggregationPeriod};

#[macro_use]
extern crate diesel;

mod adapters;
mod api;
mod core_logic;
mod database;

use adapters::discounting_cash_flows::fetch_stock_revenues;
use adapters::discounting_cash_flows::InternalServerError;
use api::models::{CorrelateDataPoint, CorrelationData};
use core_logic::data_processing::revenues_to_dataframe;
use database::models::DatasetMetadata;

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
}

#[tokio::main]
async fn main() {
    println!("Starting set up");

    let pool = ThreadPoolBuilder::new()
        .stack_size(32 * 1024 * 1024) // 32 MB
        .build()
        .unwrap();

    let now = SystemTime::now();
    let datasets = database::queries::fetch_datasets::fetch_datasets().unwrap();
    let dataset_metadatas = database::queries::fetch_datasets::fetch_dataset_metadata().unwrap();

    let dataframes = core_logic::data_processing::create_dataframes(&datasets, &dataset_metadatas);
    let dataset_metadatas_map: Arc<HashMap<String, DatasetMetadata>> = Arc::new(
        dataset_metadatas
            .par_iter()
            .map(|metadata| (metadata.internal_name.clone(), metadata.to_owned()))
            .collect(),
    );

    println! {"Elapsed time fetching data {}", now.elapsed().unwrap().as_secs()}

    let transformed_dataframes: Arc<HashMap<String, DataFrame>> = Arc::new(pool.install(|| {
        let transformed_dataframes = dataframes
            .par_iter()
            .map(|(name, df)| {
                let transformed_df =
                    core_logic::data_processing::transform_data(df, AggregationPeriod::Quarterly);
                (name.clone(), transformed_df)
            })
            .collect();
        transformed_dataframes
    }));

    println! {"Elapsed time {}", now.elapsed().unwrap().as_secs()}

    // let correlations_route = warp::path("correlations").map({
    //     let transformed_dataframes = Arc::clone(&transformed_dataframes);
    //     move || {
    //         let (name, df1) = transformed_dataframes.iter().next().unwrap();
    //         let pool = ThreadPoolBuilder::new()
    //             .stack_size(32 * 1024 * 1024) // 32 MB
    //             .build()
    //             .unwrap();

    //         let correlations: Vec<_> = pool.install(|| {
    //             transformed_dataframes
    //                 .par_iter()
    //                 .filter(|(name2, _)| *name != **name2)
    //                 .map(|(name2, df2)| {
    //                     let correlation = correlate(df1, df2);
    //                     (name.clone(), name2.clone(), correlation)
    //                 })
    //                 .collect()
    //         });
    //         warp::reply::json(&correlations)
    //     }
    // });

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
                Ok(value) => Ok(value.0),
                Err(_) => return Err(warp::reject::custom(InternalServerError)),
            }
        })
        .map({
            let transformed_dataframes = Arc::clone(&transformed_dataframes);
            let dataset_metadatas_map = Arc::clone(&dataset_metadatas_map);
            move |revenues| {
                let pool = ThreadPoolBuilder::new()
                    .stack_size(32 * 1024 * 1024) // 32 MB
                    .build()
                    .unwrap();

                let correlations: Vec<_> = pool.install(|| {
                    // Fetch the revenues for the stock
                    let df1 = revenues_to_dataframe(revenues);
                    let mut correlations: Vec<CorrelateDataPoint> = transformed_dataframes
                        .par_iter()
                        .map(|(name2, df2)| {
                            let metadata: &DatasetMetadata =
                                dataset_metadatas_map.get(name2).unwrap();
                            let title = match &metadata.external_name {
                                Some(title) => title.clone(),
                                _ => String::from(""),
                            };
                            let series_id = metadata.internal_name.clone();
                            let correlation_dp = correlate(&df1, df2, title, series_id, 0);
                            correlation_dp
                        })
                        .collect();

                    correlations.sort_by(|a, b| {
                        b.pearson_value
                            .partial_cmp(&a.pearson_value)
                            .unwrap_or(Ordering::Equal)
                    });

                    correlations.truncate(100);
                    correlations
                });

                warp::reply::json(&CorrelationData {
                    data: correlations,
                    aggregation_period: "Quarterly".to_string(),
                    correlation_metric: "RAW_VALUE".to_string(),
                })
            }
        });

    // Start the webserver
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "8001".to_string())
        .parse()
        .expect("PORT must be a number");

    println!("Starting web server! on {}", port);
    warp::serve(revenue_route.or(correlate_route))
        .run(([127, 0, 0, 1], port))
        .await;
}
