//! File-authored wakeups, lowered to the agent's existing inbox and actor alarm.
//!
//! These values are projections of `monitors/` and `schedules/` in an agent's
//! private volume. Neither a definition nor a followed event starts a model
//! request: only the Notice's `wake` policy does that.

mod cron;
mod monitor;
mod schedule;

pub use cron::Cron;
pub use monitor::{Monitor, MonitorCursor, MonitorEvent, MonitorFilter, MonitorFollow};
pub use schedule::{Lifetime, Missed, Recurrence, Schedule, ScheduleFire};

use super::{Error, IntentId};

fn invalid(reason: &'static str) -> Error {
    Error::Record(reason)
}

fn event_id(domain: &str, parts: &[&[u8]]) -> Result<IntentId, Error> {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.wake.event.v1");
    hash.update(domain.as_bytes());
    for part in parts {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    IntentId::try_from(format!("wake:{}", hash.finalize().to_hex()))
}
