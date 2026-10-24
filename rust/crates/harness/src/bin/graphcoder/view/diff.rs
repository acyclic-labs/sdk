/// The selected workspace exists only while its stream exists. A failed
/// subscription waits for a new presentation revision before reopening.
pub(super) struct DiffState<W, S> {
    accepted: Option<u64>,
    subscription: Option<(W, S)>,
}

impl<W: PartialEq, S> DiffState<W, S> {
    pub(super) fn new() -> Self {
        Self {
            accepted: None,
            subscription: None,
        }
    }

    pub(super) fn needs_refresh(&self, revision: u64) -> bool {
        self.accepted != Some(revision)
    }

    pub(super) fn select(&mut self, revision: u64, workspace: W, open: impl FnOnce() -> S) -> bool {
        self.accepted = Some(revision);
        if self
            .subscription
            .as_ref()
            .is_some_and(|(selected, _)| selected == &workspace)
        {
            return false;
        }
        self.subscription = Some((workspace, open()));
        true
    }

    pub(super) fn stream(&mut self) -> Option<&mut S> {
        self.subscription.as_mut().map(|(_, stream)| stream)
    }

    pub(super) fn failed(&mut self) {
        self.subscription = None;
    }

    pub(super) fn selection_failed(&mut self, revision: u64) {
        self.failed();
        self.accepted = Some(revision);
    }

    pub(super) fn close(&mut self) {
        self.failed();
        self.accepted = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use proptest::prelude::*;

    fn recovers(revision: u64, workspace: u32, failures: usize, retain_failed: bool) -> bool {
        futures::executor::block_on(async {
            let mut state = DiffState::new();
            let open = || futures::stream::iter([Err::<(), _>("query failed")]);
            assert!(state.select(revision, workspace, open));
            for offset in 0..failures as u64 {
                assert_eq!(
                    state.stream().unwrap().next().await,
                    Some(Err("query failed"))
                );
                if !retain_failed {
                    state.failed();
                }
                // No immediate retry loop while rendering the same error frame.
                assert!(!state.needs_refresh(revision + offset));
                assert!(state.needs_refresh(revision + offset + 1));
                if !state.select(revision + offset + 1, workspace, open) {
                    return false;
                }
            }
            state.failed();
            assert!(state.select(revision + failures as u64 + 1, workspace, || {
                futures::stream::iter([Ok(())])
            }));
            assert_eq!(state.stream().unwrap().next().await, Some(Ok(())));
            // A healthy stream is reused, but closing always invalidates it.
            assert!(!state.select(revision + failures as u64 + 2, workspace, open));
            state.close();
            assert!(state.needs_refresh(revision + failures as u64 + 2));
            state.selection_failed(revision + failures as u64 + 2);
            assert!(!state.needs_refresh(revision + failures as u64 + 2));
            assert!(state.select(revision + failures as u64 + 3, workspace, open));
            true
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn failed_stream_reopens_on_later_presentation(
            revision in 0_u64..1_000_000, workspace in any::<u32>(), failures in 1_usize..8,
        ) {
            prop_assert!(recovers(revision, workspace, failures, false));
            // Negative control: the former independent selection survives the
            // failed stream, so the same workspace suppresses reopening.
            prop_assert!(!recovers(revision, workspace, failures, true));
        }
    }
}
