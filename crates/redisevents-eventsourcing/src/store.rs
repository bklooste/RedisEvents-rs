//! The slice of the core `StreamStore` the repository needs, as a trait so tests (and other
//! stores) can stand in for it (C# `IStreamStore`).

use std::future::Future;

use redisevents::StreamId;
use redisevents::StreamMsg;
use redisevents::producer::{StateEvent, StreamStore};

/// Reads and appends state streams. Implemented by [`StreamStore`].
pub trait EventStore: Send + Sync {
    /// Entries of `name` strictly after `after`, at most `max`.
    fn read(
        &self,
        name: &str,
        after: StreamId,
        max: usize,
    ) -> impl Future<Output = redisevents::Result<Vec<StreamMsg>>> + Send;

    /// Appends and publishes if the stream length is still `expected_len`; `None` = it was not.
    fn append_and_publish(
        &self,
        name: &str,
        expected_len: u64,
        partition_key: &str,
        events: &[StateEvent],
    ) -> impl Future<Output = redisevents::Result<Option<Vec<StreamId>>>> + Send;

    /// Appends and publishes with no version check.
    fn append_and_publish_unchecked(
        &self,
        name: &str,
        partition_key: &str,
        events: &[StateEvent],
    ) -> impl Future<Output = redisevents::Result<Vec<StreamId>>> + Send;
}

impl EventStore for StreamStore {
    async fn read(
        &self,
        name: &str,
        after: StreamId,
        max: usize,
    ) -> redisevents::Result<Vec<StreamMsg>> {
        StreamStore::read(self, name, after, max).await
    }

    async fn append_and_publish(
        &self,
        name: &str,
        expected_len: u64,
        partition_key: &str,
        events: &[StateEvent],
    ) -> redisevents::Result<Option<Vec<StreamId>>> {
        StreamStore::append_and_publish(self, name, expected_len, partition_key, events).await
    }

    async fn append_and_publish_unchecked(
        &self,
        name: &str,
        partition_key: &str,
        events: &[StateEvent],
    ) -> redisevents::Result<Vec<StreamId>> {
        StreamStore::append_and_publish_unchecked(self, name, partition_key, events).await
    }
}
