//! One oracle for healthy cases and their targeted mutants.
//!
//! A family declares named invariants over its ordered observation events and
//! at least one control (an input plus a fault) for every invariant. The
//! fixture only records events; every verdict comes from the invariants here.
//! Evaluation is online: the first event that violates any invariant ends the
//! run, so a fault is attributed to the observation that first exposes it.
//!
//! A control passes only when its input is clean without the fault and a fresh
//! faulted run's first exposing event violates its named invariant. Then the
//! assertion-deletion check reruns the fault with that one invariant deleted:
//! the run must get past the exposing event, so no other invariant was
//! carrying the detection there. Later consequences of the corrupted state may
//! still trip other invariants; they are reported, not required. A panic or fixture error
//! anywhere is an infrastructure failure, never a detection. Healthy checks run
//! every control first, once per oracle, and an oracle dropped after zero
//! healthy cases fails.
//!
//! The helper cannot establish that an invariant is semantically right, that
//! expected values in events derive only from inputs (fixtures must compute
//! them before calling the candidate), or that a fault models a real defect.
//!
//! ```
//! use acyclic_test_support::differential::oracle::Oracle;
//! // Sums its input; the fault drops the last element.
//! let oracle = Oracle::<Vec<u8>, (), (u32, u32)>::declare(|input, fault, see| {
//!     let expected = input.iter().map(|&byte| u32::from(byte)).sum();
//!     let used = &input[..input.len() - usize::from(fault.is_some())];
//!     see.see((used.iter().map(|&byte| u32::from(byte)).sum(), expected))
//! })
//! .invariant("sum", |(actual, expected)| {
//!     if actual == expected { Ok(()) } else { Err(format!("{actual} != {expected}")) }
//! })
//! .control("sum", vec![1, 2], ())
//! .build();
//! oracle.check(&vec![3, 4, 5]);
//! ```
//!
//! An oracle needs controls before it can be built:
//!
//! ```compile_fail
//! use acyclic_test_support::differential::oracle::Oracle;
//! let oracle = Oracle::<u8, (), u8>::declare(|_, _, see| see.see(1))
//!     .invariant("one", |event| if *event == 1 { Ok(()) } else { Err("not one".into()) })
//!     .build();
//! ```
//!
//! and has no public fields or literal constructor:
//!
//! ```compile_fail
//! use acyclic_test_support::differential::oracle::Oracle;
//! let oracle: Oracle<u8, (), u8> = Oracle { invariants: Vec::new() };
//! ```
use std::{
    cell::{Cell, OnceCell},
    collections::BTreeSet,
    fmt::Debug,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind},
};

/// Returned to the fixture once an event violates an invariant; the fixture
/// propagates it with `?` and performs no further operations. A fixture may
/// also return it right after recording an event it knows is a violation and
/// cannot continue past (such as a failed production call).
#[derive(Debug)]
pub struct Stop;

type Check<E> = fn(&E) -> Result<(), String>;
type Run<I, F, E> = Box<dyn Fn(&I, Option<&F>, &mut Observer<'_, E>) -> Result<(), Stop>>;

/// Ordered event sink for one run.
pub struct Observer<'a, E> {
    invariants: &'a [(&'static str, Check<E>)],
    deleted: Option<&'a str>,
    suppressed: usize,
    events: Vec<E>,
    violated: Vec<(&'static str, String)>,
}

impl<E> Observer<'_, E> {
    /// Records one event and evaluates every invariant on it.
    ///
    /// # Panics
    /// Panics if the fixture ignored an earlier [`Stop`].
    pub fn see(&mut self, event: E) -> Result<(), Stop> {
        assert!(
            self.violated.is_empty(),
            "fixture continued after Stop (infrastructure failure)"
        );
        for (name, check) in self.invariants {
            if let Err(detail) = check(&event) {
                if Some(*name) == self.deleted {
                    self.suppressed += 1;
                } else {
                    self.violated.push((name, detail));
                }
            }
        }
        self.events.push(event);
        if self.violated.is_empty() {
            Ok(())
        } else {
            Err(Stop)
        }
    }
}

/// Typestate: an oracle without controls has no `build`.
pub struct Uncontrolled;
/// Typestate: at least one control is declared.
pub struct Controlled;

/// Declares invariants and controls; see the module documentation.
pub struct Builder<I, F, E, S> {
    invariants: Vec<(&'static str, Check<E>)>,
    controls: Vec<(&'static str, I, F)>,
    run: Run<I, F, E>,
    state: PhantomData<S>,
}

/// A built oracle. Construct it only through [`Oracle::declare`].
pub struct Oracle<I, F, E> {
    invariants: Vec<(&'static str, Check<E>)>,
    controls: Vec<(&'static str, I, F)>,
    run: Run<I, F, E>,
    verified: OnceCell<()>,
    healthy: Cell<usize>,
}

impl<I: Debug, F: Debug, E: Debug> Oracle<I, F, E> {
    /// `run` builds its own fixture from the input, applies the fault (if any)
    /// and records every observation in order.
    pub fn declare(
        run: impl Fn(&I, Option<&F>, &mut Observer<'_, E>) -> Result<(), Stop> + 'static,
    ) -> Builder<I, F, E, Uncontrolled> {
        Builder {
            invariants: Vec::new(),
            controls: Vec::new(),
            run: Box::new(run),
            state: PhantomData,
        }
    }

    /// Checks one healthy case. Runs every control first, once per oracle.
    ///
    /// # Panics
    /// Panics if any control or the case fails, or the fixture panics.
    pub fn check(&self, input: &I) {
        self.verified.get_or_init(|| self.verify_controls());
        let (events, violated) = self.observe(input, None, None, "healthy case");
        assert!(
            violated.is_empty(),
            "healthy case {input:?} violated {violated:?} at {:?}",
            events.last()
        );
        assert!(
            !events.is_empty(),
            "healthy case {input:?} observed nothing"
        );
        self.healthy.set(self.healthy.get() + 1);
    }

    fn verify_controls(&self) {
        for (target, input, fault) in &self.controls {
            let label = format!("control {fault:?} for {target}");
            let (clean, violated) = self.observe(input, None, None, &label);
            assert!(
                violated.is_empty() && !clean.is_empty(),
                "{label}: its input is not a clean nonempty case: {violated:?}"
            );
            let (exposed, violated) = self.observe(input, Some(fault), None, &label);
            assert!(
                violated.iter().any(|(name, _)| name == target),
                "{label}: the mutant was not detected by its target ({violated:?} at {:?})",
                exposed.last()
            );
            let (events, violated) = self.observe(input, Some(fault), Some(target), &label);
            assert!(
                violated.is_empty() || events.len() > exposed.len(),
                "{label}: still detected after deleting {target}: {violated:?} at {:?}",
                events.last()
            );
        }
    }

    fn observe(
        &self,
        input: &I,
        fault: Option<&F>,
        deleted: Option<&str>,
        label: &str,
    ) -> (Vec<E>, Vec<(&'static str, String)>) {
        let mut observer = Observer {
            invariants: &self.invariants,
            deleted,
            suppressed: 0,
            events: Vec::new(),
            violated: Vec::new(),
        };
        let outcome = catch_unwind(AssertUnwindSafe(|| (self.run)(input, fault, &mut observer)));
        match outcome {
            Ok(Ok(())) => assert!(
                observer.violated.is_empty(),
                "{label}: fixture discarded Stop (infrastructure failure)"
            ),
            // With its invariant deleted, a fixture may still stop where it
            // cannot continue past the (suppressed) violation.
            Ok(Err(Stop)) => assert!(
                !observer.violated.is_empty() || observer.suppressed > 0,
                "{label}: fixture stopped without a violation (infrastructure failure)"
            ),
            Err(panic) => {
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic");
                panic!("{label}: infrastructure failure, not a detection: {message}");
            }
        }
        (observer.events, observer.violated)
    }
}

impl<I, F, E> Drop for Oracle<I, F, E> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            assert!(self.healthy.get() > 0, "oracle selected zero healthy cases");
        }
    }
}

impl<I, F, E, S> Builder<I, F, E, S> {
    /// Adds a named invariant over single events.
    pub fn invariant(mut self, name: &'static str, check: Check<E>) -> Self {
        self.invariants.push((name, check));
        self
    }

    /// Adds a control: `fault` applied to `input` must violate exactly `target`.
    pub fn control(self, target: &'static str, input: I, fault: F) -> Builder<I, F, E, Controlled> {
        let Builder {
            invariants,
            mut controls,
            run,
            state: _,
        } = self;
        controls.push((target, input, fault));
        Builder {
            invariants,
            controls,
            run,
            state: PhantomData,
        }
    }
}

impl<I, F, E> Builder<I, F, E, Controlled> {
    /// # Panics
    /// Panics on duplicate invariant names, a control naming an unknown
    /// invariant, or an invariant without a control.
    pub fn build(self) -> Oracle<I, F, E> {
        let names: BTreeSet<_> = self.invariants.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            names.len(),
            self.invariants.len(),
            "duplicate invariant name"
        );
        let targets: BTreeSet<_> = self.controls.iter().map(|(target, _, _)| *target).collect();
        assert_eq!(
            targets, names,
            "every invariant needs a control and every control a declared target"
        );
        Oracle {
            invariants: self.invariants,
            controls: self.controls,
            run: self.run,
            verified: OnceCell::new(),
            healthy: Cell::new(0),
        }
    }
}

#[cfg(test)]
mod tests;
