use ballista_core::serde::protobuf;
use ballista_core::serde::AsLogicalPlan;
use ballista_core::serde::DefaultLogicalExtensionCodec;
use datafusion::prelude::*;

use datafusion_wasi_proto::protobuf::greeter_client::GreeterClient;
use datafusion_wasi_proto::protobuf::HelloRequest;

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GreeterClient::connect("http://[::1]:50051").await?;

    let mut ctx = ExecutionContext::new();
    ctx.register_parquet("example", "parquet-testing/data/alltypes_plain.parquet")
        .await?;

    // create a plan to run a SQL query
    let df = ctx
        .sql("SELECT bool_col, MIN(int_col) FROM example GROUP BY bool_col LIMIT 100")
        .await?;

    let proto_plan = protobuf::LogicalPlanNode::try_from_logical_plan(
        &df.to_logical_plan(),
        &DefaultLogicalExtensionCodec {},
    )?;

    let request = tonic::Request::new(HelloRequest {
        name: "Tonic".into(),
    });

    let response = client.say_hello(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn it_works() {
        crate::main().await.unwrap();
    }
}
