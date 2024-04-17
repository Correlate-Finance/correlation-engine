use crate::database::schema::datasets_dataset;
use crate::database::schema::datasets_datasetmetadata;
use serde::{Deserialize, Serialize};

#[derive(Queryable, Selectable, Deserialize, Serialize, Debug, Clone)]
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
    pub group_popularity: Option<i32>, // Nullable<Int4> corresponds to Option<i32>
    pub popularity: Option<i32>,       // Nullable<Int4> corresponds to Option<i32>
    pub hidden: bool,                  // Bool corresponds to bool
    pub sub_source: Option<String>,    // Nullable<Varchar> corresponds to Option<String>
    pub units: Option<String>,         // Nullable<Varchar> corresponds to Option<String>
    pub units_short: Option<String>,   // Nullable<Varchar> corresponds to Option<String>
    pub release: Option<String>,       // Nullable<Varchar> corresponds to Option<String>
    pub url: Option<String>,           // Nullable<Varchar> corresponds to Option<String>
    pub categories: Option<Vec<Option<String>>>, // Nullable<Array<Nullable<Varchar>>> corresponds to Option<Vec<Option<String>>>
}

#[derive(Queryable, Selectable, Deserialize, Serialize, Debug, Clone)]
#[diesel(table_name = datasets_dataset)]
pub struct Dataset {
    pub id: i32,
    pub date: chrono::DateTime<chrono::Utc>,
    pub value: f64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metadata_id: i64,
}
