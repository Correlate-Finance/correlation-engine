use crate::api::models::CorrelationMetric;
use chrono::Month;

pub fn correlation_metric_from_str(metric: String) -> CorrelationMetric {
    if metric == "RAW_VALUE" {
        CorrelationMetric::RawValue
    } else if metric == "YOY_GROWTH" {
        CorrelationMetric::YoyGrowth
    } else {
        panic!("Unknown Metric")
    }
}

pub fn month_index(month: String) -> i8 {
    month
        .parse::<Month>()
        .expect("Fiscal year end needs to be a valid month")
        .number_from_month() as i8
        + 1
}
