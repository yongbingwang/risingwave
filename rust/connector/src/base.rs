use anyhow::Result;
use async_trait::async_trait;

use crate::kafka::enumerator::client::KafkaSplitEnumerator;
use crate::kafka::split::{KafkaSplits};
use crate::pulsar::enumerator::client::PulsarSplitEnumerator;
use crate::pulsar::split::{PulsarSplits};

pub trait SourceMessage {
    fn payload(&self) -> Result<Option<&[u8]>>;
}

pub trait SourceSplit {
    fn id(&self) -> String;
}

#[async_trait]
pub trait SourceReader: Sized {
    type Message: SourceMessage + Send + Sync;
    type Split: SourceSplit + Send + Sync;

    async fn next(&mut self) -> Result<Option<Vec<Self::Message>>>;
    async fn assign_split(&mut self, split: Self::Split) -> Result<()>;
}

#[async_trait]
pub trait SplitEnumerator {
    type Split: SourceSplit + Send + Sync;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

pub enum SplitEnumeratorImpl {
    Kafka(KafkaSplitEnumerator),
    Pulsar(PulsarSplitEnumerator),
}

pub enum SourceSplitsImpl {
    Kafka(KafkaSplits),
    Pulsar(PulsarSplits),
}

impl From<PulsarSplits> for SourceSplitsImpl {
    fn from(splits: PulsarSplits) -> Self {
        SourceSplitsImpl::Pulsar(splits)
    }
}

impl From<KafkaSplits> for SourceSplitsImpl {
    fn from(splits: KafkaSplits) -> Self {
        SourceSplitsImpl::Kafka(splits)
    }
}

impl SplitEnumeratorImpl {
    async fn list_splits(&mut self) -> Result<SourceSplitsImpl> {
        match self {
            SplitEnumeratorImpl::Kafka(k) => {
                Ok(SourceSplitsImpl::from(k.list_splits().await?))
            }
            SplitEnumeratorImpl::Pulsar(p) => {
                Ok(SourceSplitsImpl::from(p.list_splits().await?))
            }
        }
    }
}
