//! Resume inputs are durable work, including after the delivery owner restarts.
use crate::Result;

pub(crate) enum Slot {
    Settled,
    Busy,
    Idle(u64),
}

// The session's one driver owns both consuming a retained wake and supplying
// its next input. Observing only Idle strands an already accepted resume.
pub(crate) async fn step<A, I, R>(
    advance: impl Fn() -> A,
    inspect: impl Fn() -> I,
    resume: impl Fn(u64) -> R,
) -> Result<bool>
where
    A: Future<Output = Result<()>> + Send,
    I: Future<Output = Result<Slot>> + Send,
    R: Future<Output = Result<()>> + Send,
{
    // A confirmed empty slot has no retained input to recover. Drive only busy
    // slots before admitting the exit; never re-run an already settled session.
    let slot = match inspect().await? {
        Slot::Busy => {
            advance().await?;
            inspect().await?
        }
        slot => slot,
    };
    match slot {
        Slot::Settled => Ok(true),
        Slot::Busy => Ok(false),
        Slot::Idle(revision) => {
            resume(revision).await?;
            advance().await?;
            Ok(matches!(inspect().await?, Slot::Settled))
        }
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::{cell::Cell, collections::VecDeque, future::ready};

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        // An empty slot is resumed exactly once, a retained resume is driven,
        // and an already terminal session performs no worker or resume work.
        #[test]
        fn settlement_only_drives_work_present_at_the_cut(revision in any::<u64>()) {
            for (slots, expected_advances, expected_resumes, expected_result) in [
                (vec![Slot::Idle(revision), Slot::Settled], 1, 1, true),
                (vec![Slot::Settled], 0, 0, true),
                (vec![Slot::Busy, Slot::Settled], 1, 0, true),
                (vec![Slot::Busy, Slot::Busy], 1, 0, false),
                (vec![Slot::Busy, Slot::Idle(revision), Slot::Settled], 2, 1, true),
            ] {
                let advances = Cell::new(0);
                let resumes = Cell::new(0);
                let slots = std::cell::RefCell::new(VecDeque::from(slots));
                let settled = futures::executor::block_on(step(
                    || { advances.set(advances.get() + 1); ready(Ok(())) },
                    || ready(Ok(slots.borrow_mut().pop_front().expect("extra inspection"))),
                    |observed| {
                        assert_eq!(observed, revision);
                        resumes.set(resumes.get() + 1);
                        ready(Ok(()))
                    },
                ))?;
                prop_assert_eq!(settled, expected_result);
                prop_assert_eq!(advances.get(), expected_advances);
                prop_assert_eq!(resumes.get(), expected_resumes);
                prop_assert!(slots.borrow().is_empty());
            }
        }
    }
}
