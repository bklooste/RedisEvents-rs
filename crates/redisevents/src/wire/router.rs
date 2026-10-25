//! Partition routing (`WIRE.md` §6). **API: W0.2. Bodies: A1** (port of `Wire/PartitionRouter.cs`).

use std::sync::atomic::{AtomicU32, Ordering};

/// The partition for a non-empty key: `xxh3_64(key, seed 0)` folded with `& (n-1)` when `n` is a
/// power of two, else `% n`. `partitions <= 1` gives `0` without hashing.
///
/// Only ever called with a **non-empty** key: callers (B1) check for an empty key first and use
/// [`RoundRobin`], as C# `StreamPublisher` checks `IsNullOrEmpty` before `PartitionRouter.ForKey`.
/// Pinned by `wire-spec/vectors/routing.json`.
#[must_use]
pub fn partition_for_key(key: &[u8], partitions: u32) -> u32 {
    if partitions <= 1 {
        return 0;
    }
    reduce(xxhash_rust::xxh3::xxh3_64(key), partitions)
}

/// `value & (n-1)` for a power of two, else `value % n`. The result is below `n`, so it fits a `u32`.
fn reduce(value: u64, partitions: u32) -> u32 {
    let n = u64::from(partitions);
    let folded = if partitions.is_power_of_two() {
        value & (n - 1)
    } else {
        value % n
    };
    u32::try_from(folded).unwrap_or(0)
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
        if partitions <= 1 {
            return 0;
        }
        // `fetch_add` wraps at u32::MAX; the value routed is the incremented one, as C#'s Interlocked.Increment.
        let next = self.counter.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        reduce(u64::from(next), partitions)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn round_robin_wraps_at_u32_overflow() {
        // C# `RoundRobin_WrapsAtUintOverflow`: 0xFFFFFFFE, 0xFFFFFFFF, 0, 1, 2, 3 over 8 partitions.
        let rr = RoundRobin {
            counter: AtomicU32::new(u32::MAX - 2),
        };
        let observed: Vec<u32> = (0..6).map(|_| rr.next(8)).collect();
        assert_eq!(observed, [6, 7, 0, 1, 2, 3]);
        assert_eq!(rr.counter.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn round_robin_non_power_of_two_stays_in_range_across_overflow() {
        // C# `RoundRobin_NonPowerOfTwo_NeverNegativeAcrossOverflow`.
        let rr = RoundRobin {
            counter: AtomicU32::new(u32::MAX - 3),
        };
        for _ in 0..20 {
            assert!(rr.next(10) < 10);
        }
    }
}
