use crate::database::models::Dataset; // Your Dataset struct
use diesel::prelude::*; // Your Dataset table's schema

use crate::database::lib::establish_connection;
use crate::database::models::DatasetMetadata;
use crate::database::schema::datasets_dataset::dsl::*;
use crate::database::schema::datasets_datasetmetadata::dsl::*;

pub fn fetch_datasets() -> QueryResult<Vec<Dataset>> {
    let conn = &mut establish_connection();
    datasets_dataset.load::<Dataset>(conn)
}

pub fn fetch_dataset_metadata() -> QueryResult<Vec<DatasetMetadata>> {
    let conn = &mut establish_connection();

    datasets_datasetmetadata.load::<DatasetMetadata>(conn)
}
