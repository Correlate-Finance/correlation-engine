use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CorrelateDataPoint {
    pub title: String,
    pub internal_name: String,
    pub pearson_value: f64,
    pub lag: usize,
    pub input_data: Vec<f64>,
    pub dataset_data: Vec<f64>,
    pub dates: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CorrelationData {
    pub data: Vec<CorrelateDataPoint>,
    pub aggregation_period: String,
    pub correlation_metric: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
pub enum CorrelationMetric {
    RawValue,
    YoyGrowth,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
pub enum AggregationPeriod {
    Quarterly,
    Annually,
}
