use arrow_format::flight::service::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

mod service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let service = service::FlightServiceImpl {};

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    println!("Datafusion server running on port 50051.");

    Ok(())
}
