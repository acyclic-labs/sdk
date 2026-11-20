//! Admission and cooperative work bounds, shared by continuous and event hosts.
use super::*;
use crate::guest::AdmissionClass;

/// One value per admission class, addressed by the class itself.
#[derive(Clone, Copy, Debug)]
pub(super) struct PerClass<T> {
    work: T,
    control: T,
}

impl<T> PerClass<T> {
    pub(super) fn new(mut value: impl FnMut() -> T) -> Self {
        Self {
            work: value(),
            control: value(),
        }
    }

    pub(super) fn iter(&self) -> std::array::IntoIter<&T, 2> {
        [&self.work, &self.control].into_iter()
    }

    pub(super) fn map<U>(self, mut project: impl FnMut(T) -> U) -> PerClass<U> {
        PerClass {
            work: project(self.work),
            control: project(self.control),
        }
    }
}

impl<T> std::ops::Index<AdmissionClass> for PerClass<T> {
    type Output = T;

    fn index(&self, class: AdmissionClass) -> &T {
        match class {
            AdmissionClass::Work => &self.work,
            AdmissionClass::Control => &self.control,
        }
    }
}

impl<T> std::ops::IndexMut<AdmissionClass> for PerClass<T> {
    fn index_mut(&mut self, class: AdmissionClass) -> &mut T {
        match class {
            AdmissionClass::Work => &mut self.work,
            AdmissionClass::Control => &mut self.control,
        }
    }
}

/// The class whose turn it is; `turn` moves on to the other, so neither
/// class can starve the other's admission.
pub(super) fn take_turn(turn: &mut AdmissionClass) -> AdmissionClass {
    let class = *turn;
    *turn = match class {
        AdmissionClass::Work => AdmissionClass::Control,
        AdmissionClass::Control => AdmissionClass::Work,
    };
    class
}

/// Runtime scheduling credit, independent of a package's resource policy.
/// The default has no active-delivery ceiling; work batches remain chunked.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeLimits {
    pub(super) active: PerClass<Option<usize>>,
    pub(super) work: usize,
}

impl RuntimeLimits {
    /// Reserve at most `active` ordinary deliveries and perform at most `work`
    /// immediate steps before yielding. Controls have their own capacity; the
    /// same work chunk limits each due-index batch. Default has no reservation.
    pub fn new(active: usize, work: usize) -> Result<Self, Error> {
        if active == 0 || active > tokio::sync::Semaphore::MAX_PERMITS || work == 0 {
            return Err(Error::Invalid(
                "runtime bounds must be positive and representable",
            ));
        }
        Ok(Self {
            active: PerClass {
                work: Some(active),
                control: None,
            },
            work,
        })
    }

    /// Independently reserve control deliveries. None is truly unbounded.
    pub fn with_control_capacity(mut self, capacity: Option<usize>) -> Result<Self, Error> {
        if capacity
            .is_some_and(|capacity| capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Error::Invalid(
                "control capacity must be positive and representable",
            ));
        }
        self.active.control = capacity;
        Ok(self)
    }

    pub(super) fn allows(self, class: AdmissionClass, running: usize) -> bool {
        self.active[class].is_none_or(|capacity| running < capacity)
    }
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            active: PerClass::new(|| None),
            work: 64,
        }
    }
}

/// Yield to the executor without a timer or a new task.
pub(super) async fn yield_now() {
    let mut yielded = false;
    poll_fn(|context| {
        if std::mem::replace(&mut yielded, true) {
            Poll::Ready(())
        } else {
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}
