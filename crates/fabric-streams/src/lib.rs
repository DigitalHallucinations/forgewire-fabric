//! Stream-sequence accounting for ForgeWire task streams.
//!
//! The hub assigns a strictly-increasing per-task sequence number to every
//! stdout/stderr/info line. Today that's done with a SQLite
//! `BEGIN IMMEDIATE` + `SELECT MAX(seq)` + `INSERT` round-trip per line.
//! The MAX scan is amortized by the index but the BEGIN IMMEDIATE acquires
//! the database write lock for ~150–300 µs per call, which limits hub
//! throughput under high-volume runner output (1000+ lines/sec).
//!
//! This crate moves the counter into a process-local `parking_lot::Mutex`
//! map. SQLite still owns durability (the hub still INSERTs every line) but
//! the seq is assigned in-memory under a per-counter lock, eliminating the
//! per-call `BEGIN IMMEDIATE` + `MAX(seq)` SELECT.
//!
//! Correctness model:
//! - On first touch of a task_id, the caller primes the counter via
//!   [`StreamCounter::prime`] with the current `MAX(seq)` from SQLite.
//!   Subsequent [`StreamCounter::next_seq`] calls are pure in-memory.
//! - Only one hub process owns the SQLite database; cross-process writers
//!   would race, but that's already true today (NSSM serializes the hub).
//! - On hub restart the counters reset and re-prime from SQLite, so
//!   sequence continuity survives `kill -9`.

use std::collections::HashMap;

use parking_lot::Mutex;

#[derive(Debug, Default)]
pub struct StreamCounter {
    /// Per-task next-seq-to-assign. Sharded by task_id under a single lock;
    /// the lock window is single-digit nanoseconds (HashMap lookup + u64++)
    /// so contention is negligible at any realistic line rate.
    inner: Mutex<HashMap<i64, u64>>,
}

impl StreamCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Prime a task's counter from SQLite's current `MAX(seq)`. Idempotent:
    /// if the counter is already higher than `current_max` the call is a
    /// no-op (defensive against double-priming after a transient hub blip).
    pub fn prime(&self, task_id: i64, current_max: u64) {
        let mut g = self.inner.lock();
        let entry = g.entry(task_id).or_insert(0);
        // No-regression: a stale prime can never push the counter backwards.
        // First prime always installs the entry (even at 0), which is what
        // marks the task as "primed" for `next_seq`.
        if current_max > *entry {
            *entry = current_max;
        }
    }

    /// Return whether this task's counter has been primed.
    pub fn is_primed(&self, task_id: i64) -> bool {
        self.inner.lock().contains_key(&task_id)
    }

    /// Atomically allocate the next seq for `task_id`. Returns `None` if the
    /// counter has not been primed yet — caller must prime from SQLite first.
    pub fn next_seq(&self, task_id: i64) -> Option<u64> {
        let mut g = self.inner.lock();
        let entry = g.get_mut(&task_id)?;
        *entry += 1;
        Some(*entry)
    }

    /// Forget a task's counter (e.g. after task termination). Subsequent
    /// `next_seq` calls will require re-priming.
    pub fn forget(&self, task_id: i64) {
        self.inner.lock().remove(&task_id);
    }

    /// Number of tasks with a live counter. Diagnostic only.
    pub fn task_count(&self) -> usize {
        self.inner.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_seq_requires_prime() {
        let c = StreamCounter::new();
        assert!(c.next_seq(1).is_none());
        c.prime(1, 0);
        assert_eq!(c.next_seq(1), Some(1));
        assert_eq!(c.next_seq(1), Some(2));
    }

    #[test]
    fn prime_resumes_after_existing_rows() {
        let c = StreamCounter::new();
        c.prime(42, 17);
        assert_eq!(c.next_seq(42), Some(18));
    }

    #[test]
    fn prime_is_idempotent_no_regression() {
        let c = StreamCounter::new();
        c.prime(7, 100);
        let _ = c.next_seq(7).unwrap(); // 101
        c.prime(7, 50); // stale prime
        assert_eq!(c.next_seq(7), Some(102)); // counter held its ground
    }

    #[test]
    fn distinct_tasks_are_independent() {
        let c = StreamCounter::new();
        c.prime(1, 0);
        c.prime(2, 0);
        assert_eq!(c.next_seq(1), Some(1));
        assert_eq!(c.next_seq(2), Some(1));
        assert_eq!(c.next_seq(1), Some(2));
    }

    #[test]
    fn forget_resets() {
        let c = StreamCounter::new();
        c.prime(1, 5);
        assert_eq!(c.next_seq(1), Some(6));
        c.forget(1);
        assert!(c.next_seq(1).is_none());
    }

    #[test]
    fn concurrent_increments_are_unique() {
        use std::sync::Arc;
        use std::thread;

        let c = Arc::new(StreamCounter::new());
        c.prime(1, 0);
        let n_threads = 8;
        let per_thread = 1000;
        let mut handles = vec![];
        for _ in 0..n_threads {
            let c = c.clone();
            handles.push(thread::spawn(move || {
                let mut local = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    local.push(c.next_seq(1).unwrap());
                }
                local
            }));
        }
        let mut all: Vec<u64> = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        let total = (n_threads * per_thread) as u64;
        assert_eq!(all.first().copied(), Some(1));
        assert_eq!(all.last().copied(), Some(total));
        for (i, v) in all.iter().enumerate() {
            assert_eq!(*v, (i as u64) + 1);
        }
    }
}
