// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_connector::{extract_split_enumerator, SourceSplit, SplitEnumeratorImpl, SplitImpl};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::meta::table_fragments::Fragment;
use tokio::sync::Mutex;

use crate::barrier::BarrierManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv, SourceId, TableId};
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

pub type SourceSplitID = String;

#[allow(dead_code)]
pub struct SourceManager<S: MetaStore> {
    core: Mutex<SourceManagerCore<S>>,
    barrier_manager_ref: BarrierManagerRef<S>,
    catalog_manager_ref: CatalogManagerRef<S>,
}

pub struct SourceManagerCore<S: MetaStore> {
    source_jobs: HashMap<SourceId, DiscoveryJob>,
    materialized_views: HashMap<TableId, HashMap<SourceId, Vec<Fragment>>>,
    meta_srv_env: MetaSrvEnv<S>,
}

#[derive(Copy, Clone, Debug)]
pub enum SourceState {
    Ready,
    Running,
    Disabled,
}

pub struct CreateSourceContext {
    pub materialized_view_id: TableId,
    pub fragments: HashMap<SourceId, Vec<Fragment>>,
    pub init_state: SourceState,
}

impl<S> SourceManagerCore<S>
where
    S: MetaStore,
{
    fn new() -> Self {
        todo!()
    }
}

pub struct DiscoveryJob {
    enumerator: SplitEnumeratorImpl,
    last_splits: Mutex<Vec<SplitImpl>>,
}

impl DiscoveryJob {
    fn new(source: StreamSourceInfo) -> Result<Self> {
        let enumerator = extract_split_enumerator(source.get_properties()).to_rw_result()?;
        Ok(Self {
            enumerator,
            last_splits: Mutex::new(Vec::new()),
        })
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            // TODO(Peng): error handler
            let splits = self.enumerator.list_splits().await.unwrap();

            {
                let mut last_splits = self.last_splits.lock().await;
                *last_splits = splits;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

impl<S> SourceManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        _meta_srv_env: MetaSrvEnv<S>,
        barrier_manager_ref: BarrierManagerRef<S>,
        catalog_manager_ref: CatalogManagerRef<S>,
    ) -> Result<Self> {
        Ok(Self {
            core: Mutex::new(SourceManagerCore::new()),
            // meta_srv_env,
            barrier_manager_ref,
            catalog_manager_ref,
            // last_assignment: Default::default(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        // todo: fill me
        Ok(())
    }

    async fn create_source(&self, mut ctx: CreateSourceContext) -> Result<()> {
        let mut core = self.core.lock().await;

        if core
            .materialized_views
            .contains_key(ctx.materialized_view_id.borrow())
        {
            return Err(RwError::from(InternalError(format!(
                "mv {} already registered in source manager",
                ctx.materialized_view_id
            ))));
        }

        let mut infos = HashMap::with_capacity(ctx.fragments.len());

        for source_id in ctx.fragments.keys() {
            let source = self
                .catalog_manager_ref
                .get_source(*source_id)
                .await?
                .ok_or(RwError::from(InternalError(format!(
                    "source {} does not exists",
                    source_id,
                ))))?;

            let source_info = match source.get_info()?.clone() {
                Info::StreamSource(s) => s,
                _ => {
                    return Err(RwError::from(InternalError(
                        "only stream source is support".to_string(),
                    )));
                }
            };

            infos.insert(source_id, source_info);
        }

        for (source_id, info) in infos {
            core.source_jobs
                .entry(*source_id)
                .or_insert_with(|| DiscoveryJob::new(info.clone()).unwrap());
        }

        core.materialized_views
            .insert(ctx.materialized_view_id, std::mem::take(&mut ctx.fragments));

        Ok(())
    }

    async fn split_discovery_job(&self) {}

    // async fn assign_split_to_actors(properties: HashMap<String, String>, actors: Vec<ActorId>) ->
    // Result<HashMap<ActorId, Vec<SplitImpl>>> {     let mut enumerator =
    // extract_split_enumerator(&properties).to_rw_result()?;     let splits =
    // enumerator.list_splits().await?;     let mut result =
    // HashMap::with_capacity(actors.len());     splits.into_iter().enumerate().for_each(|(i,
    // split)| {         result[&actors[i % actors.len()]] = split
    //     });
    //     Ok(result)
    // }
}
