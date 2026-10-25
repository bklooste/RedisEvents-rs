//! [`RecordingHandler`], a port of the C# `TestHandler`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::ThreadId;
use std::time::Duration;

use redisevents::{BatchHandler, HandleResult, MessageHandler, PartitionContext, StreamMsg};
use tokio::sync::Notify;

type BatchHook = dyn Fn(&[StreamMsg], &PartitionContext) -> HandleResult + Send + Sync;

/// Records every message a consumer delivers, as a [`BatchHandler`] or a [`MessageHandler`].
///
/// Optional [`RecordingHandler::with_delay`] slows each call down, and
/// [`RecordingHandler::with_hook`] decides each call's result (to inject `Skip`/`Block`). Messages
/// are recorded **before** the hook runs, so a blocked batch shows up once per attempt.
pub struct RecordingHandler {
    state: Mutex<State>,
    changed: Notify,
    batches: AtomicUsize,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    delay: Duration,
    hook: Option<Arc<BatchHook>>,
}

#[derive(Default)]
struct State {
    messages: Vec<StreamMsg>,
    threads: Vec<ThreadId>,
}

impl Default for RecordingHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RecordingHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingHandler")
            .field("count", &self.count())
            .field("batches", &self.batches())
            .finish_non_exhaustive()
    }
}

impl RecordingHandler {
    /// A handler that records and succeeds.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            batches: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            delay: Duration::ZERO,
            hook: None,
        }
    }

    /// Sleeps `delay` in every call (C# `Delay`).
    #[must_use]
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Decides every call's result (C# `OnBatch`).
    #[must_use]
    pub fn with_hook(
        mut self,
        hook: impl Fn(&[StreamMsg], &PartitionContext) -> HandleResult + Send + Sync + 'static,
    ) -> Self {
        self.hook = Some(Arc::new(hook));
        self
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every message recorded so far, in delivery order.
    #[must_use]
    pub fn messages(&self) -> Vec<StreamMsg> {
        self.state().messages.clone()
    }

    /// The bodies recorded so far, as lossy UTF-8.
    #[must_use]
    pub fn bodies(&self) -> Vec<String> {
        self.state()
            .messages
            .iter()
            .map(|m| String::from_utf8_lossy(m.body()).into_owned())
            .collect()
    }

    /// How many messages were recorded.
    #[must_use]
    pub fn count(&self) -> usize {
        self.state().messages.len()
    }

    /// How many calls were made.
    #[must_use]
    pub fn batches(&self) -> usize {
        self.batches.load(Ordering::Acquire)
    }

    /// The most messages in flight at once, across concurrent calls.
    #[must_use]
    pub fn peak_in_flight(&self) -> usize {
        self.peak_in_flight.load(Ordering::Acquire)
    }

    /// The distinct threads calls ran on.
    #[must_use]
    pub fn thread_ids(&self) -> Vec<ThreadId> {
        let mut ids = self.state().threads.clone();
        ids.dedup();
        let mut seen = Vec::with_capacity(ids.len());
        for id in ids {
            if !seen.contains(&id) {
                seen.push(id);
            }
        }
        seen
    }

    /// Waits until at least `count` messages are recorded.
    ///
    /// # Panics
    /// After `timeout` (default 30 s), naming what was seen.
    pub async fn wait_for(&self, count: usize, timeout: Option<Duration>) {
        let limit = timeout.unwrap_or(Duration::from_secs(30));
        let wait = async {
            loop {
                let notified = self.changed.notified();
                if self.count() >= count {
                    return;
                }
                notified.await;
            }
        };
        let finished = tokio::time::timeout(limit, wait).await.is_ok();
        assert!(
            finished,
            "timed out after {limit:?} waiting for {count} message(s); saw {} in {} batch(es)",
            self.count(),
            self.batches()
        );
    }

    /// Waits for `count` messages, then `settle` (default 500 ms), and asserts no more arrived.
    ///
    /// # Panics
    /// On timeout, or when more than `count` were delivered.
    pub async fn wait_for_exactly(
        &self,
        count: usize,
        settle: Option<Duration>,
        timeout: Option<Duration>,
    ) {
        self.wait_for(count, timeout).await;
        tokio::time::sleep(settle.unwrap_or(Duration::from_millis(500))).await;
        let seen = self.count();
        assert_eq!(
            seen, count,
            "expected exactly {count} message(s) but {seen} were delivered: duplicates or over-delivery"
        );
    }

    async fn record(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
        let depth = self.in_flight.fetch_add(batch.len(), Ordering::AcqRel) + batch.len();
        self.peak_in_flight.fetch_max(depth, Ordering::AcqRel);
        self.batches.fetch_add(1, Ordering::AcqRel);
        {
            let mut state = self.state();
            state.messages.extend_from_slice(batch);
            state.threads.push(std::thread::current().id());
        }
        self.changed.notify_waiters();
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let result = self.hook.as_ref().map_or(Ok(()), |hook| hook(batch, ctx));
        self.in_flight.fetch_sub(batch.len(), Ordering::AcqRel);
        result
    }
}

impl BatchHandler for RecordingHandler {
    async fn handle_batch(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
        self.record(batch, ctx).await
    }
}

impl MessageHandler for RecordingHandler {
    async fn handle(&self, msg: &StreamMsg, ctx: &PartitionContext) -> HandleResult {
        self.record(std::slice::from_ref(msg), ctx).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use redisevents::consumer::CancellationToken;
    use redisevents::{HandleError, StreamId};

    use super::*;

    fn token() -> PartitionContext {
        PartitionContext::new("t", "c", 0, CancellationToken::new())
    }

    fn msg(n: u64) -> StreamMsg {
        StreamMsg::new(StreamId::new(n, 0), 0, "T", Bytes::from(n.to_string()))
    }

    #[tokio::test]
    async fn records_and_waits() {
        let h = Arc::new(RecordingHandler::new());
        let ctx = token();
        let h2 = Arc::clone(&h);
        let task = tokio::spawn(async move {
            let ctx = token();
            h2.handle_batch(&[msg(1), msg(2)], &ctx).await.unwrap();
        });
        h.wait_for(2, Some(Duration::from_secs(5))).await;
        task.await.unwrap();
        h.handle(&msg(3), &ctx).await.unwrap();
        assert_eq!(h.bodies(), ["1", "2", "3"]);
        assert_eq!(h.batches(), 2);
        assert_eq!(h.peak_in_flight(), 2);
    }

    #[tokio::test]
    async fn hook_decides_result() {
        let h = RecordingHandler::new().with_hook(|_, _| Err(HandleError::block("no")));
        assert!(h.handle(&msg(1), &token()).await.unwrap_err().is_block());
        assert_eq!(h.count(), 1);
    }
}
