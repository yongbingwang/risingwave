#![allow(dead_code)]

use risingwave_pb::catalog::catalog_service_server::CatalogService;
use risingwave_pb::catalog::{CreateDatabaseRequest, CreateDatabaseResponse, CreateMaterializedSourceRequest, CreateMaterializedSourceResponse, CreateMaterializedViewRequest, CreateMaterializedViewResponse, CreateSchemaRequest, CreateSchemaResponse, CreateSourceRequest, CreateSourceResponse, DropDatabaseRequest, DropDatabaseResponse, DropMaterializedSourceRequest, DropMaterializedSourceResponse, DropMaterializedViewRequest, DropMaterializedViewResponse, DropSchemaRequest, DropSchemaResponse, DropSourceRequest, DropSourceResponse, GetCatalogRequest, GetCatalogResponse, Source, StreamSourceInfo};
use tonic::{Request, Response, Status};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::source::Info::StreamSource;

use crate::manager::{CatalogManagerRef, IdGeneratorManagerRef, MetaSrvEnv};
use crate::rpc::service::stream_service::TonicResponse;
use crate::storage::MetaStore;
use crate::stream::{CreateSourceContext, SourceManagerRef, StreamManagerRef};

#[derive(Clone)]
pub struct CatalogServiceImpl<S>
    where
        S: MetaStore,
{
    id_gen_manager: IdGeneratorManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    stream_manager: StreamManagerRef<S>,
    source_manager: SourceManagerRef<S>,
}

impl<S> CatalogServiceImpl<S>
    where
        S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        catalog_manager: CatalogManagerRef<S>,
        stream_manager: StreamManagerRef<S>,
        source_manager: SourceManagerRef<S>,
    ) -> Self {
        Self {
            id_gen_manager: env.id_gen_manager_ref(),
            catalog_manager,
            stream_manager,
            source_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> CatalogService for CatalogServiceImpl<S>
    where
        S: MetaStore,
{
    async fn get_catalog(
        &self,
        _request: Request<GetCatalogRequest>,
    ) -> Result<Response<GetCatalogResponse>, Status> {
        todo!()
    }

    async fn create_database(
        &self,
        _request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        todo!()
    }

    async fn create_schema(
        &self,
        _request: Request<CreateSchemaRequest>,
    ) -> Result<Response<CreateSchemaResponse>, Status> {
        todo!()
    }

    async fn create_source(
        &self,
        _request: Request<CreateSourceRequest>,
    ) -> TonicResponse<CreateSourceResponse> {
        let req = _request.into_inner();
        let source = req.source.ok_or(Status::invalid_argument("source"))?;


        let _source_info = match source.clone().info {
            Some(StreamSource(s)) => { s }
            _ => return Err(Status::invalid_argument("source info illegal")),
        };

        // let _resp = self.source_manager.create_source(CreateSourceContext {
        //     name: "".to_string(),
        //     table_id: Default::default(),
        //     discovery_new_split: false,
        //     properties: Default::default(),
        // }).await;


        match self.catalog_manager.create_source(&source).await {
            Ok(x) => {
                Ok(Response::new(CreateSourceResponse {
                    status: None,
                    source_id: 0,
                    version: 0,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn create_materialized_source(
        &self,
        _request: Request<CreateMaterializedSourceRequest>,
    ) -> Result<Response<CreateMaterializedSourceResponse>, Status> {
        todo!()
    }

    async fn create_materialized_view(
        &self,
        _request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        todo!()
    }

    async fn drop_database(
        &self,
        _request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        todo!()
    }

    async fn drop_schema(
        &self,
        _request: Request<DropSchemaRequest>,
    ) -> Result<Response<DropSchemaResponse>, Status> {
        todo!()
    }

    async fn drop_source(
        &self,
        _request: Request<DropSourceRequest>,
    ) -> Result<Response<DropSourceResponse>, Status> {
        todo!()
    }

    async fn drop_materialized_source(
        &self,
        _request: Request<DropMaterializedSourceRequest>,
    ) -> Result<Response<DropMaterializedSourceResponse>, Status> {
        todo!()
    }

    async fn drop_materialized_view(
        &self,
        _request: Request<DropMaterializedViewRequest>,
    ) -> Result<Response<DropMaterializedViewResponse>, Status> {
        todo!()
    }
}
