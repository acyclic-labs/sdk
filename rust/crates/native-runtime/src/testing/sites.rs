//! Scheduled adverse events at a primitive's own logical operations.
//!
//! A simulated store names each operation it intercepts as a site (an
//! authority append, an object read) and asks the schedule, per call, which
//! planned fault applies. The store owns only its fault vocabulary, meaning
//! what a fault does to that call. The schedule owns occurrence counting,
//! the plan, the trace and hold boundaries. Logical faults therefore replay,
//! sweep and minimize like physical [`super::DurableFault`] cuts.
use super::*;
use std::{any::Any, collections::BTreeMap, fmt};

/// A store's fault value: any debuggable, shareable type.
pub trait SiteFault: Any + fmt::Debug + Send + Sync {}
impl<T: Any + fmt::Debug + Send + Sync> SiteFault for T {}

/// One call at a logical site, with its planned fault.
#[derive(Clone)]
pub struct SiteCall {
    /// Site name chosen by the store.
    pub site: &'static str,
    /// Zero-based index among this site's calls in the scope.
    pub ordinal: u64,
    fault: Option<Arc<dyn SiteFault>>,
}

impl SiteCall {
    /// The planned fault, when it was planned as an `F`.
    pub fn fault<F: SiteFault>(&self) -> Option<&F> {
        let fault: &dyn Any = self.fault.as_deref()?;
        fault.downcast_ref()
    }
}

impl fmt::Debug for SiteCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.site, self.ordinal)?;
        match &self.fault {
            Some(fault) => write!(f, " {fault:?}"),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
pub(super) struct Sites {
    calls: BTreeMap<&'static str, u64>,
    plan: BTreeMap<(&'static str, u64), Arc<dyn SiteFault>>,
    trace: Vec<SiteCall>,
}

impl Sites {
    fn plan(&mut self, site: &'static str, at: Option<u64>, fault: Arc<dyn SiteFault>) -> u64 {
        let next = self.calls.get(site).copied().unwrap_or(0);
        let ordinal = at.unwrap_or(next);
        assert!(ordinal >= next, "{site}#{ordinal} has already run");
        assert!(
            self.plan.insert((site, ordinal), fault).is_none(),
            "{site}#{ordinal} already has a planned fault"
        );
        ordinal
    }
}

impl Schedule {
    /// A handle that plans and observes this scope without owning it. Stores
    /// inside a world hold this, so a world never keeps its own executor alive.
    #[must_use]
    pub fn observer(&self) -> Self {
        Self(self.0.clone(), None)
    }

    /// Plans `fault` at call `ordinal` (zero-based) of `site`.
    ///
    /// # Panics
    ///
    /// Panics if that call has already run or already has a planned fault.
    pub fn inject(&self, site: &'static str, ordinal: u64, fault: impl SiteFault) {
        self.0
            .state()
            .sites
            .plan(site, Some(ordinal), Arc::new(fault));
    }

    /// Plans `fault` at the next call of `site`, under the same lock that
    /// registers calls, and returns that call's ordinal.
    ///
    /// # Panics
    ///
    /// Panics if that call already has a planned fault.
    pub fn inject_next(&self, site: &'static str, fault: impl SiteFault) -> u64 {
        self.0.state().sites.plan(site, None, Arc::new(fault))
    }

    /// Counts one call at `site` and returns its planned fault, if any.
    ///
    /// # Panics
    ///
    /// Panics if the planned fault is not an `F`: the store cannot apply it.
    pub fn intercept<F: SiteFault + Clone>(&self, site: &'static str) -> Option<F> {
        let mut state = self.0.state();
        let sites = &mut state.sites;
        let ordinal = sites.calls.entry(site).or_insert(0);
        let call = SiteCall {
            site,
            ordinal: *ordinal,
            fault: sites.plan.remove(&(site, *ordinal)),
        };
        *ordinal += 1;
        let fault = call.fault::<F>().cloned();
        assert!(
            fault.is_some() || call.fault.is_none(),
            "{call:?} is not a fault this site applies"
        );
        sites.trace.push(call);
        fault
    }

    /// Complete ordered record of logical site calls in this scope.
    pub fn site_calls(&self) -> Vec<SiteCall> {
        self.0.state().sites.trace.clone()
    }

    /// Holds the calling operation at a [`Boundary`] named `site`. The driver
    /// releases it to continue, or drops the boundary (or its receiver) to
    /// crash the operation, which then never resumes.
    pub async fn hold(&self, site: &'static str) {
        let (release, waiting) = oneshot::channel();
        self.0
            .events
            .send(Boundary {
                name: site,
                owner: 0,
                detail: Arc::from(site),
                release,
            })
            .ok();
        if waiting.await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    enum Fault {
        Reject,
        Stale(u8),
    }

    #[test]
    fn planned_faults_apply_at_exact_site_ordinals_and_are_traced() {
        let (schedule, _events) = Schedule::new();
        schedule.inject("append", 1, Fault::Reject);
        assert_eq!(schedule.inject_next("head", Fault::Stale(7)), 0);
        assert_eq!(schedule.intercept::<Fault>("append"), None);
        assert_eq!(schedule.intercept::<Fault>("append"), Some(Fault::Reject));
        assert_eq!(schedule.intercept::<Fault>("head"), Some(Fault::Stale(7)));
        assert_eq!(schedule.inject_next("append", Fault::Reject), 2);
        let calls = schedule.site_calls();
        assert_eq!(
            format!("{calls:?}"),
            "[append#0, append#1 Reject, head#0 Stale(7)]"
        );
        assert_eq!(calls[2].fault::<Fault>(), Some(&Fault::Stale(7)));
        assert_eq!(calls[2].fault::<u8>(), None);
    }

    #[test]
    #[should_panic(expected = "append#0 has already run")]
    fn planning_a_past_call_is_refused() {
        let (schedule, _events) = Schedule::new();
        schedule.intercept::<Fault>("append");
        schedule.inject("append", 0, Fault::Reject);
    }

    #[test]
    #[should_panic(expected = "append#0 already has a planned fault")]
    fn planning_one_call_twice_is_refused() {
        let (schedule, _events) = Schedule::new();
        schedule.inject("append", 0, Fault::Reject);
        schedule.inject_next("append", Fault::Reject);
    }

    #[test]
    #[should_panic(expected = "append#0 \"wrong\" is not a fault this site applies")]
    fn a_fault_the_site_cannot_apply_fails_the_world() {
        let (schedule, _events) = Schedule::new();
        schedule.inject("append", 0, "wrong");
        schedule.intercept::<Fault>("append");
    }

    async fn pending<F: Future>(mut future: std::pin::Pin<&mut F>) -> bool {
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
    }

    #[tokio::test]
    async fn a_hold_resumes_on_release_and_never_after_a_drop() {
        let (schedule, mut events) = Schedule::new();
        let held = schedule.hold("put");
        tokio::pin!(held);
        assert!(pending(held.as_mut()).await);
        let boundary = events.recv().await.expect("hold boundary");
        assert_eq!(boundary.name, "put");
        boundary.release();
        held.await;

        let crashed = schedule.hold("put");
        tokio::pin!(crashed);
        assert!(pending(crashed.as_mut()).await);
        drop(events.recv().await.expect("hold boundary"));
        assert!(pending(crashed.as_mut()).await);
    }
}
