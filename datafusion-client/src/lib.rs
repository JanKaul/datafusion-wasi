use std::collections::HashMap;
use std::sync::Arc;

use arrow_format::flight::data::Ticket;
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use ballista_core::serde::protobuf;
use ballista_core::serde::AsLogicalPlan;
use ballista_core::serde::DefaultLogicalExtensionCodec;
use datafusion::arrow;
use datafusion::arrow::io::flight::deserialize_schemas;
use datafusion::field_util::SchemaExt;
use datafusion::prelude::*;
use datafusion::record_batch::RecordBatch;

use prost::Message;
use tonic::Request;

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightServiceClient::connect("http://[::1]:50051").await?;

    let mut ctx = ExecutionContext::new();
    // ctx.register_parquet("example", "parquet-testing/data/alltypes_plain.parquet")
    //     .await?;

    // create a plan to run a SQL query
    let df = ctx
        .sql("SELECT bool_col, MIN(int_col) FROM example GROUP BY bool_col LIMIT 100")
        .await?;

    let proto_plan = protobuf::LogicalPlanNode::try_from_logical_plan(
        &df.to_logical_plan(),
        &DefaultLogicalExtensionCodec {},
    )?;

    let mut buf: Vec<u8> = Vec::with_capacity(proto_plan.encoded_len());

    proto_plan.try_encode(&mut buf)?;

    let request = Request::new(Ticket { ticket: buf });

    let mut stream = client.do_get(request).await?.into_inner();

    let flight_data = stream.message().await?.unwrap();
    // convert FlightData to a stream
    let (schema, ipc_schema) = deserialize_schemas(flight_data.data_body.as_slice()).unwrap();
    let schema = Arc::new(schema);
    println!("Schema: {:?}", schema);

    // all the remaining stream messages should be dictionary and record batches
    let mut results = vec![];
    let dictionaries_by_field = HashMap::new();
    while let Some(flight_data) = stream.message().await? {
        let chunk = arrow::io::flight::deserialize_batch(
            &flight_data,
            schema.fields(),
            &ipc_schema,
            &dictionaries_by_field,
        )?;
        results.push(RecordBatch::new_with_chunk(&schema, chunk));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn it_works() {
        crate::main().await.unwrap();
    }
}
