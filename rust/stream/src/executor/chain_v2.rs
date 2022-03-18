use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use super::{Executor, Message, PkIndicesRef};
use crate::executor::ExecutorBuilder;
use crate::task::{ExecutorParams, StreamManagerCore};

#[derive(Debug)]
enum ChainState {
    Init,
    ReadingSnapshot,
    ReadingMView,
}

/// [`ChainV2Executor`] is an executor that enables synchronization between the existing stream and
/// newly appended executors. Currently, [`ChainV2Executor`] is mainly used to implement MV on MV
/// feature. It pipes new data of existing MVs to newly created MV only all of the old data in the
/// existing MVs are dispatched.
#[derive(Debug)]
pub struct ChainV2Executor {
    snapshot: Box<dyn Executor>,
    mview: Box<dyn Executor>,
    state: ChainState,
    schema: Schema,
    column_idxs: Vec<usize>,
    /// Logical Operator Info
    op_info: String,
}

pub struct ChainV2ExecutorBuilder;

impl ExecutorBuilder for ChainV2ExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        _stream: &mut StreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::ChainNode)?;
        let snapshot = params.input.remove(1);
        let mview = params.input.remove(0);

        // TODO(MrCroxx): Use column_descs to get idx after mv planner can generate stable
        // column_ids. Now simply treat column_id as column_idx.
        // TODO(bugen): how can we know the way of mapping?
        let column_idxs: Vec<usize> = node.column_ids.iter().map(|id| *id as usize).collect();

        // The batch query executor scans on a mapped adhoc mview table, thus we should directly use
        // its schema.
        let schema = snapshot.schema().clone();
        Ok(Box::new(ChainV2Executor::new(
            snapshot,
            mview,
            schema,
            column_idxs,
            params.op_info,
        )))
    }
}

impl ChainV2Executor {
    pub fn new(
        snapshot: Box<dyn Executor>,
        mview: Box<dyn Executor>,
        schema: Schema,
        column_idxs: Vec<usize>,
        op_info: String,
    ) -> Self {
        Self {
            snapshot,
            mview,
            state: ChainState::Init,
            schema,
            column_idxs,
            op_info,
        }
    }
}

#[async_trait]
impl Executor for ChainV2Executor {
    async fn next(&mut self) -> Result<Message> {
        todo!()
    }

    fn schema(&self) -> &Schema {
        todo!()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        todo!()
    }

    fn identity(&self) -> &str {
        todo!()
    }

    fn logical_operator_info(&self) -> &str {
        todo!()
    }
}
