//! Versioned lifecycle and supervision feed shared by every harness client.

use super::{Action, Kind, Signal};
use crate::{OperationId, agent::AgentPath};
use acyclic_stream::StreamPath;
use serde::{Deserialize, Serialize};

/// Immutable source record establishing one feed event. No observer clock,
/// guessed time or current filesystem generation replaces the original cut.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct Position {
    /// Original stream, admitted under the owner's ordinary authority.
    #[heap_layout(as = std::sync::Arc<str>)]
    pub stream: StreamPath,
    /// Original zero-based source sequence.
    pub sequence: u64,
}

/// One JSONL value on the general lifecycle/supervision event stream. The same
/// schema is available in process, through a remote session, and to file views.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Schema version, always one. Consumers refuse unsupported versions.
    pub version: u32,
    /// Agent owning the event; its tree root identifies the local session.
    pub agent: AgentPath,
    /// Original run identity; a later run never reuses its counters or signals.
    /// Null for explicit owner controls outside any particular run.
    pub run: Option<OperationId>,
    /// Original record establishing the event.
    pub source: Position,
    /// Lifecycle or query observation.
    pub event: Lifecycle,
}

impl crate::filesystem::PreparationRecord for Event {}

/// General event vocabulary, independent of any CLI, deployment or consumer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Lifecycle {
    /// The original run was admitted.
    Started {
        /// Original accepted inbox input, allowing clients to correlate a run
        /// before its first turn settles.
        input: u64,
    },
    /// An original turn settled.
    TurnDone {
        /// Original turn identity.
        turn: OperationId,
    },
    /// A successful original PR publication.
    #[serde(rename = "pr_opened")]
    PullRequestOpened {
        /// Original PR URL; never inferred merely from a command's text.
        url: String,
    },
    /// An incremental supervision query fired.
    Signal {
        /// Exact value projected as the signal's status file.
        signal: Signal,
        /// Owner policy at the same original source cut.
        action: Action,
    },
    /// The execution owner durably parked this agent.
    Paused {
        /// Signal causing the transition; null means an explicit control file.
        signal: Option<Kind>,
    },
    /// The owner reopened admission after an explicit control change.
    Resumed {},
    /// A committed file revision could not replace its admitted control state.
    ControlRejected {
        /// Public file name, without any rejected content.
        path: String,
    },
    /// The execution owner durably stopped this agent.
    Stopped {
        /// Signal causing the transition; null means an explicit control file.
        signal: Option<Kind>,
    },
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{Result, agent::AgentId, filesystem::PreparationIo};

    #[test]
    fn supervision_event_decode_custody_covers_native_storage() -> Result<()> {
        let agent = AgentPath::root(&AgentId::from_bytes([23; 16]));
        let run = OperationId::from_bytes([24; 16]);
        let signal = Signal {
            version: 1,
            sequence: 7,
            kind: Kind::Spend,
            count: 1,
            evidence: crate::supervision::Evidence::Spend {
                tokens: crate::supervision::Total::default(),
                usd_micros: crate::supervision::Total::default(),
                reservation: crate::supervision::Reservation::default(),
            },
        };
        for event in [
            Lifecycle::Started { input: 0 },
            Lifecycle::TurnDone { turn: run },
            Lifecycle::PullRequestOpened {
                url: "original-url/".repeat(1024),
            },
            Lifecycle::Signal {
                signal,
                action: Action::Stop,
            },
            Lifecycle::Paused {
                signal: Some(Kind::RepeatFailure),
            },
            Lifecycle::Resumed {},
            Lifecycle::ControlRejected {
                path: "original-path/".repeat(1024),
            },
            Lifecycle::Stopped { signal: None },
        ] {
            let expected = Event {
                version: 1,
                agent: agent.clone(),
                run: Some(run),
                source: Position {
                    stream: agent.inbox()?,
                    sequence: 7,
                },
                event,
            };
            let bytes = serde_json::to_vec(&expected)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let original = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(10_000_000, 10_000_000),
                &cancellation,
            )
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
            let baseline = original.live_allocation_bytes();
            let io = PreparationIo::from_native_work(&original)?;
            let mut decoded = None;
            let physical = acyclic_test_support::measure(|| {
                decoded = Some(io.decode(
                    std::marker::PhantomData::<Event>,
                    &bytes,
                    crate::wire::ErrorCode::Storage,
                ));
            });
            let (actual, custody) = decoded.expect("original decoder ran")?;
            assert_eq!(actual, expected);
            assert!(physical.count_total > 0, "allocation observer is active");
            assert!(
                physical.bytes_max <= original.receipt().peak_allocation_bytes - baseline,
                "supervision decoder exceeded its original peak reservation: {physical:?}"
            );
            assert!(
                physical.bytes_current.cast_unsigned() <= custody.bytes(),
                "supervision decoder storage escaped its original custody: {physical:?}"
            );
            drop(actual);
            drop(custody);
            drop(io);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        Ok(())
    }
}
