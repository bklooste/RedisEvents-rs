//! Partition routing (`WIRE.md` §6). **API: W0.2. Bodies: A1** (port of `Wire/PartitionRouter.cs`).

use std::sync::atomic::AtomicU32;

/// The partition for a non-empty key: `xxh3_64(key, seed 0)` folded with `& (n-1)` when `n` is a
/// power of two, else `% n`. `partitions <= 1` gives `0` without hashing.
///
/// Callers route an **empty** key with [`RoundRobin`] instead.
#[must_use]
pub fn partition_for_key(key: &[u8], partitions: u32) -> u32 {
    let _ = (key, partitions);
    todo!("A1")
}

/// Round-robin partition choice for empty keys. Thread-safe; one per publisher.
#[derive(Debug, Default)]
pub struct RoundRobin {
    counter: AtomicU32,
}

impl RoundRobin {
    /// A counter starting at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
        }
    }

    /// The next partition in `0..partitions` (`0` when `partitions <= 1`).
    pub fn next(&self, partitions: u32) -> u32 {
        let _ = (&self.counter, partitions);
        todo!("A1")
    }
}
