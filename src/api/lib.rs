use crate::api::models::CorrelationMetric;

pub fn correlation_metric_from_str(metric: String) -> CorrelationMetric {
    if metric == "RAW_VALUE" {
        CorrelationMetric::RawValue
    } else if metric == "YOY_GROWTH" {
        CorrelationMetric::YoyGrowth
    } else {
        panic!("Unknown Metric {}", metric)
    }
}
