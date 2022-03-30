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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use log::warn;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_connector::{extract_split_enumerator, SourceSplit, SplitImpl};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source, StreamSourceInfo};
use risingwave_pb::meta::table_fragments::Fragment;
use tokio::sync::Mutex;

use crate::barrier::BarrierManagerRef;
use crate::manager::{CatalogManagerRef, SourceId};
use crate::model::{ActorId, FragmentId, TableFragments};
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

pub type SourceSplitID = String;

#[allow(dead_code)]
pub struct SourceManager<S> {
    core: Mutex<SourceManagerCore<S>>,
    meta_store_ref: Arc<S>,
    barrier_manager_ref: BarrierManagerRef<S>,
    catalog_manager_ref: CatalogManagerRef<S>,
    last_assignment: HashMap<ActorId, Vec<String>>,
}


impl<S> SourceManager<S>
where
    S: MetaStore,
{


    pub async fn new(
        meta_store_ref: Arc<S>,
        barrier_manager_ref: BarrierManagerRef<S>,
        catalog_manager_ref: CatalogManagerRef<S>,
    ) -> Result<Self> {
    }

    pub async fn run(&self) -> Result<()> {
        // todo: fill me
        Ok(())
    }

    async fn start_source(&self, source_id: SourceId, fragment: &Fragment) -> Result<()> {
        let mut core = self.core.lock().await;

        if !core.jobs.contains_key(&source_id) {
            let source = self
                .catalog_manager_ref
                .get_source(source_id)
                .await?
                .ok_or(RwError::from(InternalError(format!(
                    "source {} does not exists",
                    source_id
                ))))?;

            let source_info = match source.get_info()? {
                Info::StreamSource(s) => s,
                _ => {
                    return Err(RwError::from(InternalError(
                        "only stream source is support".to_string(),
                    )))
                }
            };

            let job = SourceJob::new(source_info.clone())?;

            core.jobs.insert(source_id, job);
        }

        let job = core.jobs.get_mut(&fragment.fragment_id).unwrap();

        let _ = job.add(fragment.clone()).await?;

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
