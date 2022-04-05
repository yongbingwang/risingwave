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
//
use std::ops::RangeBounds;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use risingwave_common::error::Result;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{yield_now, JoinHandle};

use super::StateStoreMetrics;
use crate::hummock::key::Epoch;
use crate::hummock::HummockError;
use crate::{dispatch_state_store, StateStore, StateStoreImpl, StateStoreIter};

#[derive(Debug)]
enum StateStoreSyncMsg {
    Sync(Option<Epoch>, oneshot::Sender<Result<()>>),
}

/// Background worker for synchronizing state store to S3
#[derive(Clone)]
struct StateStoreSyncWorker {
    input_tx: mpsc::UnboundedSender<StateStoreSyncMsg>,
}

impl StateStoreSyncWorker {
    pub fn new(state_store_impl: Weak<StateStoreImpl>) -> (Self, JoinHandle<Result<()>>) {
        let (input_tx, mut input_rx) = mpsc::unbounded_channel();

        let join_handle = tokio::spawn(async move {
            let mut time_interval = tokio::time::interval(Duration::from_millis(50));
            while let Some(state_store) = state_store_impl.upgrade() {
                // #1 threshold-based sync
                dispatch_state_store!(state_store.as_ref(), store, {
                    let shared_buff_cur_size =
                        store.stats.shared_buffer_cur_size.load(Ordering::SeqCst);
                    if store.stats.shared_buffer_threshold_size <= shared_buff_cur_size {
                        store.stats.write_shared_buffer_sync_counts.inc();
                        let timer = store.stats.write_shared_buffer_sync_time.start_timer();

                        if let Err(e) = store.inner().sync(None).await {
                            panic!("Failed to sync state store based on threshold due to {}", e);
                        }
                        timer.observe_duration();
                    } else {
                        // wait a moment for executors to fill the shared buffer
                        time_interval.tick().await;
                    }

                    // #2 message-driven sync
                    match input_rx.try_recv() {
                        Ok(StateStoreSyncMsg::Sync(epoch, notifier_tx)) => {
                            store.stats.write_shared_buffer_sync_counts.inc();
                            let timer = store.stats.write_shared_buffer_sync_time.start_timer();
                            if let Err(e) = store.inner().sync(epoch).await {
                                panic!(
                                    "Failed to sync state store based of epoch {:?} due to {}",
                                    epoch, e
                                )
                            }

                            timer.observe_duration();
                            notifier_tx.send(Ok(())).map_err(|_| {
                                HummockError::shared_buffer_error(
                                    "Failed to notify state store sync due to send drop",
                                )
                            })?;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            break; // exit
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {} // do nothing
                    }
                })
            }
            Ok(())
        });

        (Self { input_tx }, join_handle)
    }

    pub async fn do_sync(&self, epoch: Option<Epoch>) -> Result<()> {
        let (notifier_tx, notifier_rx) = oneshot::channel();
        self.input_tx
            .send(StateStoreSyncMsg::Sync(epoch, notifier_tx))
            .unwrap();
        notifier_rx.await.unwrap()
    }
}

/// A state store wrapper for monitoring metrics.
#[derive(Clone)]
pub struct MonitoredStateStore<S> {
    inner: S,
    stats: Arc<StateStoreMetrics>,
    sync_worker: Option<StateStoreSyncWorker>,
}

impl<S> MonitoredStateStore<S>
where
    S: StateStore,
{
    pub fn new(inner: S, stats: Arc<StateStoreMetrics>) -> Self {
        Self {
            inner,
            stats,
            sync_worker: None,
        }
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn start_sync_worker(
        &mut self,
        state_store_impl: Arc<StateStoreImpl>,
    ) -> JoinHandle<Result<()>> {
        let (sync_worker, join_handle) =
            StateStoreSyncWorker::new(Arc::downgrade(&state_store_impl));
        self.sync_worker.replace(sync_worker);
        join_handle
    }
}

impl<S> MonitoredStateStore<S>
where
    S: StateStore,
{
    async fn monitored_iter<'a, I>(
        &self,
        iter: I,
    ) -> Result<<MonitoredStateStore<S> as StateStore>::Iter<'a>>
    where
        I: Future<Output = Result<S::Iter<'a>>>,
    {
        self.stats.iter_counts.inc();

        let timer = self.stats.iter_seek_latency.start_timer();
        let iter = iter.await?;
        timer.observe_duration();

        let monitored = MonitoredStateStoreIter {
            inner: iter,
            stats: self.stats.clone(),
        };
        Ok(monitored)
    }
}

#[async_trait]
impl<S> StateStore for MonitoredStateStore<S>
where
    S: StateStore,
{
    type Iter<'a> = MonitoredStateStoreIter<S::Iter<'a>>;

    async fn get(&self, key: &[u8], epoch: u64) -> Result<Option<Bytes>> {
        self.stats.get_counts.inc();

        let timer = self.stats.get_latency.start_timer();
        let value = self.inner.get(key, epoch).await?;
        timer.observe_duration();

        self.stats.get_key_size.observe(key.len() as _);
        if let Some(value) = value.as_ref() {
            self.stats.get_value_size.observe(value.len() as _);
        }

        Ok(value)
    }

    async fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.stats.range_scan_counts.inc();

        let timer = self.stats.range_scan_latency.start_timer();
        let result = self.inner.scan(key_range, limit, epoch).await?;
        timer.observe_duration();

        self.stats
            .range_scan_size
            .observe(result.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as _);

        Ok(result)
    }

    async fn reverse_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.stats.reverse_range_scan_counts.inc();

        let timer = self.stats.range_scan_latency.start_timer();
        let result = self.inner.scan(key_range, limit, epoch).await?;
        timer.observe_duration();

        self.stats
            .range_scan_size
            .observe(result.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as _);

        Ok(result)
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, epoch: u64) -> Result<u64> {
        if kv_pairs.is_empty() {
            return Ok(0);
        }

        self.stats.write_batch_counts.inc();
        self.stats
            .write_batch_tuple_counts
            .inc_by(kv_pairs.len() as _);

        let timer = self.stats.write_batch_shared_buffer_time.start_timer();
        let batch_size = self.inner.ingest_batch(kv_pairs, epoch).await?;
        timer.observe_duration();

        self.stats.write_batch_size.observe(batch_size as _);
        let mut shared_buff_cur_size = (batch_size as u64)
            + self
                .stats
                .shared_buffer_cur_size
                .fetch_add(batch_size as _, Ordering::SeqCst);

        // yield current task if threshold has been reached after ingest batch
        while self.stats.shared_buffer_threshold_size <= shared_buff_cur_size {
            yield_now().await;
            shared_buff_cur_size = self.stats.shared_buffer_cur_size.load(Ordering::SeqCst);
        }

        Ok(batch_size)
    }

    async fn iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.monitored_iter(self.inner.iter(key_range, epoch)).await
    }

    async fn reverse_iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.monitored_iter(self.inner.reverse_iter(key_range, epoch))
            .await
    }

    async fn wait_epoch(&self, epoch: u64) -> Result<()> {
        self.inner.wait_epoch(epoch).await
    }

    async fn sync(&self, epoch: Option<u64>) -> Result<()> {
        if let Some(worker) = &self.sync_worker {
            worker.do_sync(epoch).await?
        }
        Ok(())
    }

    fn monitored(self, _stats: Arc<StateStoreMetrics>) -> MonitoredStateStore<Self> {
        panic!("the state store is already monitored")
    }

    async fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        epoch: u64,
    ) -> Result<()> {
        self.inner.replicate_batch(kv_pairs, epoch).await
    }
}

/// A state store iterator wrapper for monitoring metrics.
pub struct MonitoredStateStoreIter<I> {
    inner: I,

    stats: Arc<StateStoreMetrics>,
}

#[async_trait]
impl<I> StateStoreIter for MonitoredStateStoreIter<I>
where
    I: StateStoreIter<Item = (Bytes, Bytes)>,
{
    type Item = I::Item;

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let timer = self.stats.iter_next_latency.start_timer();
        let pair = self.inner.next().await?;
        timer.observe_duration();

        if let Some((key, value)) = pair.as_ref() {
            self.stats
                .iter_next_size
                .observe((key.len() + value.len()) as _);
        }

        Ok(pair)
    }
}
