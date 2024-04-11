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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CorrelateAutomaticRequestParameters {
    pub stock: String,
    pub start_year: i32,
    pub end_year: Option<i32>,
    pub aggregation_period: String,
    pub lag_periods: usize,
    pub correlation_metric: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CorrelateRequestParameters {
    pub start_year: i32,
    pub end_year: Option<i32>,
    pub aggregation_period: String,
    pub lag_periods: usize,
    pub correlation_metric: String,
    pub fiscal_year_end: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RevenueRequestParameters {
    pub stock: String,
    pub start_year: i32,
    pub end_year: Option<i32>,
    pub aggregation_period: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ManualDataInput {
    pub date: String,
    pub value: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CorrelateInputBody {
    pub manual_input_dataset: Vec<ManualDataInput>,
    pub selected_datasets: Vec<String>,
}
