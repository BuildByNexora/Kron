use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use chrono::{DateTime, Utc};

use crate::timer::{RunId, TimerId};

/// Entry in the scheduler heap.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduledTimer {
    pub timer_id: TimerId,
    pub next_run_at: DateTime<Utc>,
    pub run_id: Option<RunId>,
    pub attempt: u32,
}

// BinaryHeap is a max-heap; wrapping in Reverse gives us a min-heap
// so the earliest deadline is always at the top.
impl Ord for ScheduledTimer {
    fn cmp(&self, other: &Self) -> Ordering {
        self.next_run_at.cmp(&other.next_run_at)
    }
}

impl PartialOrd for ScheduledTimer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Min-heap of scheduled timers, ordered by next_run_at.
pub struct TimerHeap {
    inner: BinaryHeap<Reverse<ScheduledTimer>>,
}

impl TimerHeap {
    pub fn new() -> Self {
        Self {
            inner: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, entry: ScheduledTimer) {
        self.inner.push(Reverse(entry));
    }

    /// Peek at the nearest deadline without consuming it.
    pub fn peek_next_at(&self) -> Option<DateTime<Utc>> {
        self.inner.peek().map(|Reverse(e)| e.next_run_at)
    }

    /// Pop all timers whose `next_run_at <= now`.
    pub fn pop_due(&mut self, now: DateTime<Utc>) -> Vec<ScheduledTimer> {
        let mut due = Vec::new();
        while let Some(Reverse(entry)) = self.inner.peek() {
            if entry.next_run_at <= now {
                // Safe: we just peeked successfully.
                let Reverse(entry) = self.inner.pop().unwrap();
                due.push(entry);
            } else {
                break;
            }
        }
        due
    }

    /// Remove all entries for a given timer (e.g. on cancel or pause).
    /// BinaryHeap has no remove-by-key; we rebuild without the target.
    pub fn remove_timer(&mut self, id: &TimerId) {
        let kept: Vec<_> = self
            .inner
            .drain()
            .filter(|Reverse(e)| &e.timer_id != id)
            .collect();
        self.inner.extend(kept);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for TimerHeap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn dt(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 10, h, m, 0).unwrap()
    }

    #[test]
    fn pops_in_chronological_order() {
        let mut heap = TimerHeap::new();
        heap.push(entry("b", dt(9, 0)));
        heap.push(entry("a", dt(8, 0)));
        heap.push(entry("c", dt(10, 0)));

        let due = heap.pop_due(dt(9, 30));
        assert_eq!(due.len(), 2);
        assert_eq!(due[0].timer_id, TimerId::new("a"));
        assert_eq!(due[1].timer_id, TimerId::new("b"));
        assert_eq!(heap.len(), 1);
    }

    #[test]
    fn remove_timer_cleans_up() {
        let mut heap = TimerHeap::new();
        heap.push(entry("x", dt(8, 0)));
        heap.push(entry("y", dt(9, 0)));
        heap.remove_timer(&TimerId::new("x"));
        assert_eq!(heap.len(), 1);
        let due = heap.pop_due(dt(10, 0));
        assert_eq!(due[0].timer_id, TimerId::new("y"));
    }

    fn entry(id: &str, next_run_at: DateTime<Utc>) -> ScheduledTimer {
        ScheduledTimer {
            timer_id: TimerId::new(id),
            next_run_at,
            run_id: None,
            attempt: 1,
        }
    }
}
