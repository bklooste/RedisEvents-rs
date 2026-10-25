//! [`RedisPositionStore`] and [`NullPositionStore`]. **Owner: B4** (port of `RedisPositionStore.cs`,
//! `NullPositionStore.cs`). Struct names, `new` and the trait impls are frozen; fields are B4's.

use std::collections::BTreeMap;
use std::future::{Future, ready};

use uuid::Uuid;

use super::PositionStore;
use crate::connection::Connection;
use crate::error::Result;
use crate::wire::{Keys, StreamId};

/// Positions in `p:{topic}:<consumer>` plus `p:{topic}:<consumer>:meta`.
#[derive(Debug, Clone)]
pub struct RedisPositionStore {
    conn: Connection,
    keys: Keys,
    instance_id: Uuid,
}

impl RedisPositionStore {
    /// A store writing under `instance_id`.
    #[must_use]
    pub fn new(conn: Connection, keys: Keys, instance_id: Uuid) -> Self {
        Self {
            conn,
            keys,
            instance_id,
        }
    }

    /// The instance id stamped on every write.
    #[must_use]
    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }
}

impl PositionStore for RedisPositionStore {
    async fn load(&self, _topic: &str, _consumer: &str) -> Result<BTreeMap<u32, StreamId>> {
        let _ = (&self.conn, &self.keys);
        todo!("B4")
    }

    async fn save(
        &self,
        _topic: &str,
        _consumer: &str,
        _positions: &[(u32, StreamId)],
    ) -> Result<()> {
        todo!("B4")
    }

    async fn reset(
        &self,
        _topic: &str,
        _consumer: &str,
        _to: StreamId,
        _partition: Option<u32>,
    ) -> Result<()> {
        todo!("B4")
    }
}

/// Stores nothing (`PersistMode::None`): `load` is always empty.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullPositionStore;

impl PositionStore for NullPositionStore {
    fn load(
        &self,
        _topic: &str,
        _consumer: &str,
    ) -> impl Future<Output = Result<BTreeMap<u32, StreamId>>> + Send {
        ready(Ok(BTreeMap::new()))
    }

    fn save(
        &self,
        _topic: &str,
        _consumer: &str,
        _positions: &[(u32, StreamId)],
    ) -> impl Future<Output = Result<()>> + Send {
        ready(Ok(()))
    }

    fn reset(
        &self,
        _topic: &str,
        _consumer: &str,
        _to: StreamId,
        _partition: Option<u32>,
    ) -> impl Future<Output = Result<()>> + Send {
        ready(Ok(()))
    }
}
