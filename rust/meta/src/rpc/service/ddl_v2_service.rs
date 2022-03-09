use risingwave_pb::meta::ddl_v2_service_server::DdlV2Service;
use risingwave_pb::meta::{
    DdlV2CreateMaterializedSourceRequest, DdlV2CreateSourceRequest, DdlV2CreateTableRequest,
    DdlV2Response,
};
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManagerRef;
use crate::manager::{
    EpochGeneratorRef, IdGeneratorManagerRef, MetaSrvEnv, StoredCatalogManagerRef,
};
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, StreamManagerRef};

type TonicResult<T> = std::result::Result<Response<T>, Status>;

#[derive(Clone)]
pub struct DdlV2ServiceImpl<S> {
    env: MetaSrvEnv<S>,

    stored_catalog_manager: StoredCatalogManagerRef<S>,
    stream_manager: StreamManagerRef<S>,
    fragment_manager_ref: FragmentManagerRef<S>,
    cluster_manager: StoredClusterManagerRef<S>,
}

impl<S> DdlV2ServiceImpl<S> {
    pub fn new(
        env: MetaSrvEnv<S>,
        stored_catalog_manager: StoredCatalogManagerRef<S>,
        stream_manager: StreamManagerRef<S>,
        fragment_manager_ref: FragmentManagerRef<S>,
        cluster_manager: StoredClusterManagerRef<S>,
    ) -> Self {
        Self {
            env,
            stored_catalog_manager,
            stream_manager,
            fragment_manager_ref,
            cluster_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> DdlV2Service for DdlV2ServiceImpl<S>
where
    S: MetaStore,
{
    async fn create_table(
        &self,
        request: Request<DdlV2CreateTableRequest>,
    ) -> TonicResult<DdlV2Response> {
        todo!()
    }

    async fn create_materialized_source(
        &self,
        request: Request<DdlV2CreateMaterializedSourceRequest>,
    ) -> TonicResult<DdlV2Response> {
        todo!()
    }

    async fn create_source(
        &self,
        request: Request<DdlV2CreateSourceRequest>,
    ) -> TonicResult<DdlV2Response> {
        todo!()
    }
}
