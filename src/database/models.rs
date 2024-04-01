use serde::{Deserialize, Serialize};

#[derive(Queryable, Deserialize, Serialize, Debug, Clone)]
#[diesel(table_name = datasets_datasetmetadata)]
pub struct DatasetMetadata {
    pub id: i64,                       // Int8 in Diesel corresponds to i64 in Rust
    pub internal_name: String,         // Varchar corresponds to String
    pub external_name: Option<String>, // Nullable<Varchar> corresponds to Option<String>
    pub description: Option<String>,   // Nullable<Text> corresponds to Option<String>
    pub created_at: chrono::DateTime<chrono::Utc>, // Timestamptz corresponds to NaiveDateTime
    pub updated_at: chrono::DateTime<chrono::Utc>, // Timestamptz corresponds to NaiveDateTime
    pub category: Option<String>,      // Nullable<Varchar> corresponds to Option<String>
    pub high_level: bool,              // Bool corresponds to bool
    pub source: Option<String>,        // Nullable<Varchar> corresponds to Option<String>
}

#[derive(Queryable, Deserialize, Serialize, Debug, Clone)]
#[diesel(table_name = datasets_dataset)]
pub struct Dataset {
    pub id: i32,
    pub date: chrono::DateTime<chrono::Utc>,
    pub value: f64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metadata_id: i64,
}

pub enum AggregationPeriod {
    Quarterly,
    Annually,
}
