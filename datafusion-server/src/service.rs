use std::{pin::Pin, sync::Arc};

use arrow_format::flight::{
    data::{
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
        HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
    },
    service::flight_service_server::FlightService,
};

use ballista_core::serde::AsLogicalPlan;
use ballista_core::serde::{protobuf, DefaultLogicalExtensionCodec};

use datafusion::{
    arrow::{self, io::ipc::write::WriteOptions},
    datasource::{
        file_format::parquet::ParquetFormat, listing::ListingOptions,
        object_store::local::LocalFileSystem,
    },
    execution::dataframe_impl::DataFrameImpl,
    prelude::*,
    record_batch::RecordBatch,
};
use futures::{Stream, StreamExt};

use tonic::{Request, Response, Status, Streaming};

pub(crate) struct FlightServiceImpl {}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type DoActionStream = BoxedFlightStream<arrow_format::flight::data::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let mut ctx = ExecutionContext::new();

        ctx.register_parquet("example", "parquet-testing/data/alltypes_plain.parquet")
            .await
            .map_err(|e| from_datafusion_err(&e))?;

        let plan = protobuf::LogicalPlanNode::try_decode(&ticket.ticket)
            .map_err(|e| from_ballista_err(&e))?
            .try_into_logical_plan(&ctx, &DefaultLogicalExtensionCodec {})
            .map_err(|e| from_ballista_err(&e))?;

        let df = DataFrameImpl::new(ctx.state, &plan);

        let results: Vec<RecordBatch> = df.collect().await.map_err(|e| from_datafusion_err(&e))?;
        // Arrow IPC reader does not implement Sync + Send so we need to use a channel
        // to communicate
        if results.is_empty() {
            return Err(Status::internal("There were no results from ticket"));
        }

        // add an initial FlightData message that sends schema
        let options = WriteOptions::default();
        let schema_flight_data =
            arrow::io::flight::serialize_schema(&df.schema().clone().into(), None);

        let mut flights: Vec<Result<FlightData, Status>> = vec![Ok(schema_flight_data)];

        let mut batches: Vec<Result<FlightData, Status>> = results
            .into_iter()
            .flat_map(|batch| {
                let chunk = arrow::chunk::Chunk::new(batch.columns().to_vec());
                let (flight_dictionaries, flight_batch) =
                    arrow::io::flight::serialize_batch(&chunk, &[], &options);
                flight_dictionaries
                    .into_iter()
                    .chain(std::iter::once(flight_batch))
                    .map(Ok)
            })
            .collect();

        // append batch vector to schema vector, so that the first message sent is the schema
        flights.append(&mut batches);

        let output = futures::stream::iter(flights);

        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let request = request.into_inner();

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));

        let schema = listing_options
            .infer_schema(Arc::new(LocalFileSystem {}), &request.path[0])
            .await
            .unwrap();

        let schema_result = arrow::io::flight::serialize_schema_to_result(schema.as_ref(), None);

        Ok(Response::new(schema_result))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();

        while let Some(data) = request.next().await {
            let _data = data?;
        }

        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

fn from_ballista_err(e: &ballista_core::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {:?}", e))
}

fn from_datafusion_err(e: &datafusion::error::DataFusionError) -> Status {
    Status::internal(format!("Ballista Error: {:?}", e))
}
