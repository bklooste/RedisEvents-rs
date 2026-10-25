//! Shared test fixtures: events, messages, a tracing capture rig.
#![allow(dead_code, missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Once};

use bytes::Bytes;
use redisevents::{HandleError, StreamId, StreamMsg};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventA {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventB {
    pub id: String,
}

pub fn json<T: Serialize>(value: &T) -> Bytes {
    Bytes::from(serde_json::to_vec(value).unwrap())
}

/// A message on partition 0 with the given wire type, key, correlation id and id.
pub fn msg(wire: &str, body: Bytes, key: &str, corr: &str, id: StreamId) -> StreamMsg {
    StreamMsg::new(id, 0, wire, body)
        .with_partition_key(key)
        .with_correlation_id(corr)
}

pub fn msg_a(wire: &str, event: &EventA, key: &str, id: StreamId) -> StreamMsg {
    msg(wire, json(event), key, "c", id)
}

/// An error that must be redelivered (C# `DontIgnoreException`).
pub fn store_down() -> HandleError {
    HandleError::block("the store is down")
}

/// What a tracing capture saw.
#[derive(Debug, Clone, Default)]
pub struct Rec {
    pub name: String,
    pub level: String,
    pub fields: BTreeMap<String, String>,
}

impl Rec {
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }

    pub fn message(&self) -> &str {
        self.field("message").unwrap_or_default()
    }
}

#[derive(Clone, Default)]
pub struct Rig {
    spans: Arc<Mutex<Vec<Rec>>>,
    events: Arc<Mutex<Vec<Rec>>>,
}

struct Index(usize);

#[derive(Default)]
struct Collect(BTreeMap<String, String>);

impl Visit for Collect {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

// The rig capturing on this thread, if any. Tests run on current-thread runtimes, so a test's
// events and spans all happen on its own thread.
thread_local! {
    static CURRENT: RefCell<Option<Rig>> = const { RefCell::new(None) };
}

/// Remembers which rig a span belongs to, so later records reach it from any thread.
struct Owner(Rig);

/// The one process-wide layer. A per-thread `set_default` subscriber races with tracing's global
/// callsite-interest cache (a callsite first hit on a thread with no subscriber can be cached as
/// "never"), which made the capture tests flaky; a global subscriber that is always installed keeps
/// every callsite enabled, and each test only picks which rig its thread feeds.
struct Global;

impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Global {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let Some(rig) = CURRENT.with(|c| c.borrow().clone()) else {
            return;
        };
        let mut visit = Collect::default();
        attrs.record(&mut visit);
        let mut spans = rig.spans.lock().unwrap();
        let span = ctx.span(id).unwrap();
        span.extensions_mut().insert(Index(spans.len()));
        span.extensions_mut().insert(Owner(rig.clone()));
        spans.push(Rec {
            name: attrs.metadata().name().to_owned(),
            level: attrs.metadata().level().to_string(),
            fields: visit.0,
        });
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();
        let ext = span.extensions();
        let (Some(Owner(rig)), Some(Index(index))) = (ext.get::<Owner>(), ext.get::<Index>())
        else {
            return;
        };
        let mut visit = Collect::default();
        values.record(&mut visit);
        rig.spans.lock().unwrap()[*index].fields.extend(visit.0);
    }

    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let Some(rig) = CURRENT.with(|c| c.borrow().clone()) else {
            return;
        };
        let mut visit = Collect::default();
        event.record(&mut visit);
        rig.events.lock().unwrap().push(Rec {
            name: event.metadata().name().to_owned(),
            level: event.metadata().level().to_string(),
            fields: visit.0,
        });
    }
}

/// Stops this thread feeding its rig when dropped.
pub struct RigGuard;

impl Drop for RigGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = None);
    }
}

impl Rig {
    /// Captures what this thread traces until the guard drops. Use with a current-thread test
    /// runtime.
    pub fn install(&self) -> RigGuard {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Global))
                .expect("no other test installs a global tracing subscriber");
        });
        CURRENT.with(|c| *c.borrow_mut() = Some(self.clone()));
        RigGuard
    }

    pub fn spans(&self) -> Vec<Rec> {
        self.spans.lock().unwrap().clone()
    }

    pub fn events(&self) -> Vec<Rec> {
        self.events.lock().unwrap().clone()
    }
}
