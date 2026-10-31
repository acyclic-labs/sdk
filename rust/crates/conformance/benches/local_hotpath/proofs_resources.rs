use super::report::{Result, emit};
use acyclic_native_runtime::WallAlarm;
use acyclic_test_support::scale::Idle;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

struct Producer {
    retained: Arc<AtomicU64>,
    fired: Arc<AtomicU64>,
    _closed: oneshot::Sender<()>,
}

impl Producer {
    fn fire(&self) {
        self.fired.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.retained.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The original native alarm index alone retains each callback's producer.
/// Cancelling its registration releases that producer and closes its channel.
pub(super) async fn native_timers(recovery: &str) -> Result {
    let retained = Arc::new(AtomicU64::new(0));
    let fired = Arc::new(AtomicU64::new(0));
    // This is dormant fixture input, never a wait budget. The control waits
    // only for cancellation of the actual registrations on its own path.
    let deadline = u64::try_from(
        SystemTime::now()
            .checked_add(Duration::from_secs(24 * 60 * 60))
            .ok_or("native timer fixture deadline overflow")?
            .duration_since(UNIX_EPOCH)?
            .as_millis(),
    )?;
    for owners in [1, 4, 16] {
        let idle = |timers| Idle {
            bytes: None,
            threads: None,
            timers: Some(timers),
        };
        if retained.load(Ordering::SeqCst) != 0
            || idle(0)
                .findings(owners, recovery)
                .iter()
                .any(|finding| finding.step == "idle resources")
        {
            return Err("native timer baseline contains an injected registration".into());
        }
        let mut registrations = Vec::new();
        let mut retired = Vec::new();
        for _ in 0..owners {
            let (closed, receiver) = oneshot::channel();
            retained.fetch_add(1, Ordering::SeqCst);
            let producer = Producer {
                retained: retained.clone(),
                fired: fired.clone(),
                _closed: closed,
            };
            registrations.push(WallAlarm::register(
                deadline,
                Arc::new(move || producer.fire()),
            )?);
            retired.push(receiver);
        }
        let observed = retained.load(Ordering::SeqCst);
        if observed != owners
            || fired.load(Ordering::SeqCst) != 0
            || !idle(observed)
                .findings(owners, recovery)
                .iter()
                .any(|finding| finding.step == "idle resources")
        {
            return Err("idle oracle accepted actual per-owner native alarm registrations".into());
        }
        drop(registrations);
        for receiver in retired {
            if receiver.await.is_ok() {
                return Err("cancelled native alarm fabricated a completion".into());
            }
        }
        if retained.load(Ordering::SeqCst) != 0 || fired.load(Ordering::SeqCst) != 0 {
            return Err("native alarm cancellation retained or fired a control producer".into());
        }
        emit(serde_json::json!({
            "schema": 1, "kind": "proof_negative_control",
            "control": "per-owner original native alarm registrations", "owners": owners,
            "retained_callback_producers": observed, "result": "rejected",
            "cancelled_producers": owners, "remaining_callback_producers": 0,
            "fired": 0, "retirement_event": "original callback producer channel closed",
            "scope": "injected public WallAlarm registrations only; other idle resources unobserved",
        }))?;
    }
    Ok(())
}
