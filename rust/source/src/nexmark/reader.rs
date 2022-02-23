use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};

use crate::common::SourceChunkBuilder;
use crate::generator::NEXMarkEventGenerator;
use crate::nexmark::config::NEXMarkSourceConfig;
use crate::{BatchSourceReader, Source, SourceColumnDesc, StreamSourceReader};

static PRESERVED: AtomicUsize = AtomicUsize::new(0);

/// `NEXMarkSource` is an source that generate test data for `NEXMark` benchmark. It contains three
/// tables: Person, Auction, Bid.
#[derive(Clone, Debug)]
pub struct NEXMarkSource {
    pub config: Arc<NEXMarkSourceConfig>,
    pub column_descs: Arc<Vec<SourceColumnDesc>>,
}

/// `NEXMarkSourceReaderContext` is used to provide additional information to
/// `NEXMarkSource` when generating the Reader, such as `query_id` for consumer group
/// generation and `bound_timestamp_ms` for synchronizing end bounds.
pub struct NEXMarkSourceReaderContext;

/// `NEXMarkSourceStreamReader` is used to generate `StreamChunk` messages, when there are
/// messages will be stacked up to `DEFAULT_CHUNK_BUFFER_SIZE` messages and generate `StreamChunk`,
/// when there is no message will stop here
pub struct NEXMarkSourceStreamReader {
    generator: NEXMarkEventGenerator,
    columns: Arc<Vec<SourceColumnDesc>>,
}

/// `NEXMarkSourceBatchReader` is used to generate `DataChunk` messages, when there are
/// messages, it will stack up to `DEFAULT_CHUNK_BUFFER_SIZE` messages and generate `DataChunk`,
/// when there are no messages coming in, it will regularly get the metadata to check the overall
/// consumption status, when it is sure that the consumer group has consumed the predefined bounds,
/// it will return None to end the generation.
pub struct NEXMarkSourceBatchReader;

impl NEXMarkSource {
    pub fn new(config: NEXMarkSourceConfig, column_descs: Arc<Vec<SourceColumnDesc>>) -> Self {
        NEXMarkSource {
            config: Arc::new(config),
            column_descs,
        }
    }
}

#[async_trait]
impl Source for NEXMarkSource {
    type ReaderContext = NEXMarkSourceReaderContext;
    type BatchReader = NEXMarkSourceBatchReader;
    type StreamReader = NEXMarkSourceStreamReader;

    fn batch_reader(
        &self,
        _context: NEXMarkSourceReaderContext,
        _column_ids: Vec<i32>,
    ) -> Result<Self::BatchReader> {
        Ok(NEXMarkSourceBatchReader {})
    }

    fn stream_reader(
        &self,
        _context: NEXMarkSourceReaderContext,
        column_ids: Vec<i32>,
    ) -> Result<Self::StreamReader> {
        let columns = column_ids
            .iter()
            .map(|id| {
                self.column_descs
                    .iter()
                    .find(|c| c.column_id == *id)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "Failed to find column id: {} in source: {:?}",
                            id, self
                        )))
                    })
                    .map(|col| col.clone())
            })
            .collect::<Result<Vec<SourceColumnDesc>>>();

        let reader_id = PRESERVED.fetch_add(1, Ordering::SeqCst);

        Ok(NEXMarkSourceStreamReader {
            columns: Arc::new(columns?),
            generator: NEXMarkEventGenerator {
                events_so_far: 0,
                config: self.config.clone(),
                wall_clock_base_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as usize,
                reader_id,
            },
        })
    }
}

impl SourceChunkBuilder for NEXMarkSourceStreamReader {}

impl SourceChunkBuilder for NEXMarkSourceBatchReader {}

#[async_trait]
impl StreamSourceReader for NEXMarkSourceStreamReader {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        let events = self.generator.next(&self.columns).await?;

        let mut ops = vec![];

        for _ in 0..events.len() {
            ops.push(Op::Insert);
        }

        let columns = Self::build_columns(&self.columns, events.as_ref())?;

        assert!(
            ops.len() == columns[0].array_ref().len(),
            "ops.len(): {}, ccolumns[0].array_ref().len(): {}, events.len(): {}",
            ops.len(),
            columns[0].array_ref().len(),
            events.len()
        );

        Ok(StreamChunk::new(ops, columns, None))
    }
}

impl NEXMarkSourceBatchReader {}

#[async_trait]
impl BatchSourceReader for NEXMarkSourceBatchReader {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        return Ok(None);
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
