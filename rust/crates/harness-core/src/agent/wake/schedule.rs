use super::{Cron, event_id, invalid};
use crate::agent::{AgentPath, Error, InboxRecord, IntentId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Explicit schedule ownership; no implicit expiry is applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Lifetime {
    /// Ends when its owning session is closed.
    Session,
    /// Survives a session ending, until removed or explicitly expired.
    Daemon,
}

/// Restart policy, stated in the schedule rather than hidden in the timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Missed {
    /// Deliver every occurrence, in bounded actor-delivery pages.
    CatchUp,
    /// Deliver the retained due occurrence once, then advance past now.
    Coalesce,
}

/// Recurrence shares the same alarm as a one-shot or self-paced pause.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Recurrence {
    /// Fixed cadence anchored to the preceding due time.
    Every { milliseconds: u64 },
    /// Five-field calendar recurrence in the schedule's timezone.
    Cron { expression: String },
}

/// `schedules/<next_due:020>-<id>.json`, ordered by the volume's directory index.
/// Editing/moving a file and rearming the owner's alarm is one publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    /// Persisted format; only v1 is accepted.
    pub version: u32,
    /// Stable definition identity. Replacement uses a new identity.
    pub id: IntentId,
    /// Actor whose inbox receives the waking Notice.
    pub owner: AgentPath,
    /// Prompt or immutable content reference, interpreted by Notice core.
    pub body: Value,
    /// IANA timezone, including UTC, used only for calendar recurrence.
    pub timezone: String,
    /// Absolute next occurrence. No dormant task or OS timer accompanies it.
    pub next_due_unix_ms: u64,
    /// Omitted means a one-shot, including a self-paced wakeup.
    pub recurrence: Option<Recurrence>,
    /// Restart behavior.
    pub missed: Missed,
    /// Explicit ownership beyond the current session.
    pub lifetime: Lifetime,
    /// Omitted means no expiry.
    pub expires_unix_ms: Option<u64>,
    /// Omitted means unlimited occurrences.
    pub remaining: Option<u64>,
}

/// One alarm delivery's file mutation and inbox publication, committed together.
pub struct ScheduleFire {
    /// Stable occurrence identity, unchanged on restart or delivery replay.
    pub record: InboxRecord,
    /// Replacement document; `None` removes the completed definition.
    pub next: Option<Schedule>,
}

impl Schedule {
    /// The directory index is the due index; listing its first entry is bounded.
    #[must_use]
    pub fn path(&self) -> String {
        // Intent text need not be path-safe; its digest always is.
        let key = blake3::hash(self.id.as_str().as_bytes());
        format!(
            "schedules/{:020}-{}.json",
            self.next_due_unix_ms,
            key.to_hex()
        )
    }

    /// Validate before publishing a file or changing the actor's alarm.
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != 1 || self.next_due_unix_ms == 0 {
            return Err(invalid("invalid schedule v1 definition"));
        }
        jiff::tz::TimeZone::get(&self.timezone)
            .map_err(|_| invalid("unknown schedule timezone"))?;
        match &self.recurrence {
            Some(Recurrence::Every { milliseconds: 0 }) => {
                return Err(invalid("schedule cadence is zero"));
            }
            Some(Recurrence::Cron { expression }) => {
                Cron::parse(expression)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// The sole actor alarm considers only the earliest indexed file and the
    /// execution deadline. There is no schedule-count-dependent actor state.
    #[must_use]
    pub fn alarm(execution: Option<u64>, earliest_due: Option<u64>) -> Option<u64> {
        execution.into_iter().chain(earliest_due).min()
    }

    /// Lower one due occurrence. Missed work never causes a timer retry loop.
    /// An expired, cancelled or exhausted file produces no Notice.
    pub fn fire(&self, now: u64, session_closed: bool) -> Result<Option<ScheduleFire>, Error> {
        self.validate()?;
        if self.next_due_unix_ms > now
            || self.remaining == Some(0)
            || self.expires_unix_ms.is_some_and(|end| now >= end)
            || session_closed && self.lifetime == Lifetime::Session
        {
            return Ok(None);
        }
        let id = event_id(
            "schedule",
            &[
                self.owner.as_str().as_bytes(),
                self.id.as_str().as_bytes(),
                &self.next_due_unix_ms.to_le_bytes(),
            ],
        )?;
        let record = InboxRecord::Notice {
            id,
            notice: crate::conversation::wire::Notice {
                source: self.path(),
                kind: "schedule.due".into(),
                body: serde_json::json!({"schedule": self.id, "due_unix_ms": self.next_due_unix_ms,
                    "body": self.body}),
                wake: true,
                coalesce_key: None,
            },
        };
        let mut next = self.clone();
        next.remaining = self.remaining.map(|n| n - 1);
        let after = match self.missed {
            Missed::CatchUp => self.next_due_unix_ms,
            Missed::Coalesce => now,
        };
        let due = match &self.recurrence {
            None => None,
            Some(Recurrence::Every { milliseconds }) => {
                let steps = (after - self.next_due_unix_ms) / milliseconds;
                steps
                    .checked_add(1)
                    .and_then(|steps| steps.checked_mul(*milliseconds))
                    .and_then(|delta| self.next_due_unix_ms.checked_add(delta))
            }
            Some(Recurrence::Cron { expression }) => {
                Cron::parse(expression)?.next(after, &self.timezone)?
            }
        };
        let next = due
            .filter(|due| {
                next.remaining != Some(0) && next.expires_unix_ms.is_none_or(|end| *due < end)
            })
            .map(|due| {
                next.next_due_unix_ms = due;
                next
            });
        Ok(Some(ScheduleFire { record, next }))
    }
}
