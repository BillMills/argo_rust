use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Serialize, Deserialize};
use serde_json::json;
use mongodb::{Client, options::ClientOptions};
use futures::stream::StreamExt;
use std::env;
use std::collections::HashMap;
use bson::doc;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GeoJSONPoint {
    #[serde(rename = "type")]
    location_type: String,
    coordinates: [f64; 2],
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DataInfo {
    DATA_MODE: String,
    UNITS: String,
    LONG_NAME: String,
    PROFILE_PARAMETER_QC: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DataSchema {
    _id: String,
    geolocation: GeoJSONPoint,
    metadata: Vec<String>,
    CYCLE_NUMBER: i32,
    DIRECTION: String,
    DATA_STATE_INDICATOR: String,
    DATA_MODE: String,
    DATE_CREATION: String,
    DATE_UPDATE: String,
    DC_REFERENCE: String,
    JULD: f64,
    JULD_QC: String,
    JULD_LOCATION: f64,
    POSITION_QC: String,
    VERTICAL_SAMPLING_SCHEME: String,
    CONFIG_MISSION_NUMBER: i32,
    realtime_data: Option<HashMap<String, Vec<f64>>>,
    adjusted_data: Option<HashMap<String, Vec<f64>>>,
    data_info: Option<HashMap<String, DataInfo>>,
    level_qc: Option<HashMap<String, Vec<String>>>,
    adjusted_level_qc: Option<HashMap<String, Vec<String>>>,
}

#[get("/query_params")]
async fn get_query_params(query_params: web::Query<serde_json::Value>) -> impl Responder {
    let params = query_params.into_inner();
    HttpResponse::Ok().json(params)
}

#[get("/search")]
async fn search_data_schema(query_params: web::Query<serde_json::Value>) -> impl Responder {
    let id = query_params.get("id").unwrap().as_str().unwrap();

    // Connect to MongoDB
    let client_options = ClientOptions::parse(env::var("MONGODB_URI").unwrap()).await.unwrap();
    let client = Client::with_options(client_options).unwrap();
    let db = client.database("argo");
    let collection = db.collection::<DataSchema>("argoX");

    // Search for documents with matching _id
    let filter = mongodb::bson::doc! { "_id": id };
    let mut cursor = collection.find(filter, None).await.unwrap();
    let mut results = Vec::new();

    while let Some(result) = cursor.next().await {
        match result {
            Ok(document) => {
                results.push(document);
            },
            Err(e) => {
                eprintln!("Error: {}", e);
                return HttpResponse::InternalServerError().finish();
            }
        }
    }

    HttpResponse::Ok().json(results)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(get_query_params)
            .service(search_data_schema)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
