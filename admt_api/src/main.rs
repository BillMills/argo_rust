use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use mongodb::{bson::doc, options::FindOptions, Client};

#[get("/argo")]
async fn get_argo_documents(data: web::Data<Client>, web::Query(query): web::Query<QueryParams>) -> impl Responder {
    let collection = data.database("argo").collection("argoX");

    let filter = doc! { "_id": query.id };
    let options = FindOptions::default();

    match collection.find(filter, options).await {
        Ok(cursor) => {
            let documents: Vec<_> = cursor.map(|doc| doc.unwrap()).collect();
            HttpResponse::Ok().json(documents)
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[derive(serde::Deserialize)]
struct QueryParams {
    id: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let client = Client::with_uri_str("mongodb://localhost:27017").await.unwrap();

    HttpServer::new(move || {
        App::new()
            .data(client.clone())
            .service(get_argo_documents)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
