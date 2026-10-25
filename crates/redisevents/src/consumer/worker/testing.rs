//! Test seams for the worker (C# `Pipeline.Script`, recording position delegates, `MeterListener`
//! rigs). Test-only, crate-private; C2t's ported tests build on these.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;

use super::{FetchSource, TAP};
use crate::consumer::fetch::StreamBatch;
use crate::consumer::shared::{BoxFuture, PositionSink};
use crate::error::{Error, Result};
use crate::wire::{StreamId, StreamMsg};

/// One message `ms-seq` of `msg_type` on `partition`, body `"{ms}"`.
pub(crate) fn msg(partition: u32, ms: u64, msg_type: &str) -> StreamMsg {
    StreamMsg::new(
        StreamId::new(ms, 0),
        partition,
        msg_type,
        Bytes::from(ms.to_string()),
    )
}

/// A batch of `msgs` on `partition`, `last` = the last message's id.
pub(crate) fn batch(partition: u32, msgs: Vec<StreamMsg>) -> StreamBatch {
    let last = msgs.last().map_or(StreamId::MIN, StreamMsg::id);
    StreamBatch {
        partition,
        msgs,
        last,
    }
}

/// One scripted read.
#[derive(Debug)]
pub(crate) enum Step {
    /// The read returns these batches.
    Batches(Vec<StreamBatch>),
    /// The read fails.
    Fail(Error),
}

/// What a [`ScriptedFetch`] saw, shared with the test.
#[derive(Debug, Default)]
pub(crate) struct FetchLog {
    /// `(partition, after)` per `seek` call.
    pub(crate) seeks: Vec<(u32, StreamId)>,
    /// Reads issued (including the final parked one).
    pub(crate) reads: usize,
}

/// A fetch that plays a script, then parks forever (until the worker stops or drops it).
#[derive(Debug)]
pub(crate) struct ScriptedFetch {
    steps: VecDeque<Step>,
    log: Arc<Mutex<FetchLog>>,
}

impl ScriptedFetch {
    /// Plays `steps` in order.
    pub(crate) fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            log: Arc::default(),
        }
    }

    /// One read per batch, then park.
    pub(crate) fn batches(batches: impl IntoIterator<Item = StreamBatch>) -> Self {
        Self::new(batches.into_iter().map(|b| Step::Batches(vec![b])))
    }

    /// The shared log.
    pub(crate) fn log(&self) -> Arc<Mutex<FetchLog>> {
        self.log.clone()
    }
}

impl FetchSource for ScriptedFetch {
    async fn fetch(&mut self) -> Result<Vec<StreamBatch>> {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reads += 1;
        match self.steps.pop_front() {
            Some(Step::Batches(b)) => Ok(b),
            Some(Step::Fail(e)) => Err(e),
            None => std::future::pending().await,
        }
    }

    fn seek(&mut self, partition: u32, after: StreamId) -> bool {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .seeks
            .push((partition, after));
        true
    }
}

/// One call on a [`RecordingSink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkEvent {
    /// `record(partition, id)`.
    Record(u32, StreamId),
    /// `rewind(partition)`.
    Rewind(u32),
    /// `flush()`.
    Flush,
}

/// A [`PositionSink`] that remembers every call. `fail_flushes` makes that many flushes fail with
/// `Error::Transport` (which blocks).
#[derive(Debug, Default)]
pub(crate) struct RecordingSink {
    events: Mutex<Vec<SinkEvent>>,
    fail_flushes: Mutex<usize>,
    /// Every `blocked(partition, flag)` health call, kept apart from `events` so the exact-sequence
    /// assertions on record/flush order are unaffected.
    blocked_calls: Mutex<Vec<(u32, bool)>>,
}

impl RecordingSink {
    /// A sink whose first `n` flushes fail.
    pub(crate) fn failing_flushes(n: usize) -> Self {
        Self {
            events: Mutex::default(),
            fail_flushes: Mutex::new(n),
            blocked_calls: Mutex::default(),
        }
    }

    /// Every `blocked(partition, flag)` call so far.
    pub(crate) fn blocked_calls(&self) -> Vec<(u32, bool)> {
        self.blocked_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Every call so far.
    pub(crate) fn events(&self) -> Vec<SinkEvent> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The recorded ids, in order.
    pub(crate) fn records(&self) -> Vec<(u32, StreamId)> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                SinkEvent::Record(p, id) => Some((p, id)),
                _ => None,
            })
            .collect()
    }

    fn push(&self, e: SinkEvent) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(e);
    }
}

impl PositionSink for RecordingSink {
    fn record(&self, partition: u32, id: StreamId) {
        self.push(SinkEvent::Record(partition, id));
    }

    fn rewind(&self, partition: u32) {
        self.push(SinkEvent::Rewind(partition));
    }

    fn blocked(&self, partition: u32, blocked: bool) {
        self.blocked_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((partition, blocked));
    }

    fn flush(&self) -> BoxFuture<'_, Result<()>> {
        self.push(SinkEvent::Flush);
        let fail = {
            let mut left = self
                .fail_flushes
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let fail = *left > 0;
            *left = left.saturating_sub(1);
            fail
        };
        Box::pin(async move {
            if fail {
                Err(Error::transport("scripted flush failure"))
            } else {
                Ok(())
            }
        })
    }
}

/// A measurement the worker made (the C# `MeterListener` rig's view).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Measurement {
    /// `streams.filtered`.
    Filtered {
        /// Tag.
        topic: String,
        /// Tag.
        consumer: String,
        /// Value.
        count: u64,
    },
    /// `streams.consumed`.
    Consumed {
        /// Tag.
        topic: String,
        /// Tag.
        partition: u32,
        /// Value.
        count: u64,
    },
    /// `streams.errors`.
    Error {
        /// Tag.
        topic: String,
        /// Tag.
        policy: &'static str,
        /// Tag.
        exception_type: &'static str,
    },
    /// `streams.batch.size` / `streams.batch.duration` (one attempt).
    Batch {
        /// Tag.
        topic: String,
        /// Value.
        size: u64,
    },
}

impl Measurement {
    fn topic(&self) -> &str {
        match self {
            Self::Filtered { topic, .. }
            | Self::Consumed { topic, .. }
            | Self::Error { topic, .. }
            | Self::Batch { topic, .. } => topic,
        }
    }
}

/// Every measurement tagged `topic` so far (use a unique topic per test).
pub(crate) fn measurements(topic: &str) -> Vec<Measurement> {
    TAP.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|m| m.topic() == topic)
        .cloned()
        .collect()
}

/// A unique topic name.
pub(crate) fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}
