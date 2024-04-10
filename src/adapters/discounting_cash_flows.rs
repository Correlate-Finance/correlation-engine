use crate::api::models::AggregationPeriod;
use chrono::Datelike;
use chrono::NaiveDate;
use reqwest::Error;
use serde_json::Value;
use std::collections::HashMap;
use warp::reject;

#[derive(Debug)]
pub struct InternalServerError;

impl reject::Reject for InternalServerError {}

pub async fn fetch_stock_revenues(
    stock: &str,
    start_year: i32,
    end_year: i32,
    aggregation_period: AggregationPeriod,
) -> Result<(HashMap<String, f64>, Option<u32>), Error> {
    match aggregation_period {
        AggregationPeriod::Annually => {
            let url = format!(
                "https://discountingcashflows.com/api/income-statement/{}/",
                stock
            );
            let response = reqwest::get(&url).await?;
            let response_json: Value = response.json().await?;
            let report = response_json["report"].as_array().unwrap();

            if report.is_empty() {
                return Ok((HashMap::new(), None));
            }

            let reporting_date_month =
                NaiveDate::parse_from_str(report[0]["date"].as_str().unwrap(), "%Y-%m-%d")
                    .unwrap()
                    .month();

            let mut revenues = HashMap::new();

            for item in report {
                let year = item["calendarYear"]
                    .as_str()
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                let date = format!("{}-01-01", year);
                if year < start_year || year > end_year {
                    continue;
                }
                revenues.insert(date, item["revenue"].as_f64().unwrap());
            }

            Ok((revenues, Some(reporting_date_month)))
        }
        AggregationPeriod::Quarterly => {
            let url = format!("https://discountingcashflows.com/api/income-statement/quarterly/{}/?key=e787734f-59d8-4809-8955-1502cb22ba36", stock);
            let response = reqwest::get(&url).await?;
            let response_json: Value = response.json().await?;
            let report = response_json["report"].as_array().unwrap();

            if report.is_empty() {
                return Ok((HashMap::new(), None));
            }

            let period = report[0]["period"].as_str().unwrap();
            let reporting_date_month =
                NaiveDate::parse_from_str(report[0]["date"].as_str().unwrap(), "%Y-%m-%d")
                    .unwrap()
                    .month();

            let delta = match period {
                "Q1" => 9,
                "Q2" => 6,
                "Q3" => 3,
                _ => 0,
            };

            let mut updated_month: u32 = reporting_date_month as u32 + delta;
            if updated_month > 12 {
                updated_month = ((updated_month - 1) % 12) + 1;
            }

            let mut revenues = HashMap::new();

            for item in report {
                let year = item["calendarYear"].as_str().unwrap();
                let date = format!("{}{}", year, item["period"].as_str().unwrap());
                if year.parse::<i32>().unwrap() < start_year {
                    continue;
                }
                revenues.insert(date, item["revenue"].as_f64().unwrap());
            }

            Ok((revenues, Some(updated_month)))
        }
    }
}
