//! Lifecycle cuts exclude retained prompts, inbox bodies and earlier answers.
use super::*;

/// Selects a bounded range in the existing ordered child-result index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildResultWindow {
    /// Lifecycle and claim metadata only; no answer body is projected.
    None,
    /// Results strictly after this completed turn, in increasing order.
    After(u64),
    /// The most recent results, in increasing order within the returned page.
    Latest,
}

/// One coherent direct-child observation with a caller-bounded answer window.
/// Retained prompts, older answers and unrelated children are never projected.
#[derive(Clone, Debug)]
pub struct ChildObservation {
    /// Desired state after the latest admitted control.
    pub state: ChildState,
    /// Exact admitted control at this cut; absence differs from Resume.
    pub control: Option<Control>,
    /// Last worker acknowledgment, absent before its first claim.
    pub status: Option<ChildStatus>,
    /// Original publication of the status at this same cut.
    pub status_publication: Option<crate::OperationId>,
    /// Existing durable execution claim identified by that status publication.
    /// This is an observation; execution still requires the claim API's fence.
    pub claim: Option<ChildTurnClaim>,
    /// Inputs not yet acknowledged as delivered.
    pub pending_inputs: u64,
    /// Gapless delivered input sequence.
    pub delivered: u64,
    /// At most the requested number of immutable answers in the selected window.
    pub results: Vec<FileValue>,
    /// Exact task-volume cut shared by all fields above.
    pub generation: Generation,
    /// Opens the ordinary child follow at that same cut.
    pub cursor: Cursor,
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Observes one admitted child at the captured accepted tail. Journal
    /// continuation stays in the canonical refresh operation; callers never
    /// repeat an admitted mutation to obtain its observation. Only the selected
    /// result window is projected, without scanning sibling files.
    pub async fn observe_child(
        &self,
        reference: &Component,
        window: ChildResultWindow,
        maximum: usize,
    ) -> Result<ChildObservation> {
        if maximum == 0 {
            return Err(Error::LimitExceeded);
        }
        self.refresh().await?;
        let index = self.index.lock().await;
        let LifecycleObservation {
            state,
            control,
            status,
            status_publication,
            claim,
            pending_inputs,
            delivered,
            results,
        } = observation(&index, reference, window, maximum)?;
        let generation = self.generation(&index);
        drop(index);
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        Ok(ChildObservation {
            state,
            control,
            status,
            status_publication,
            claim,
            pending_inputs,
            delivered,
            results,
            generation,
            cursor: self.cursor(&Query::Child(reference.clone()), generation, watermarks)?,
        })
    }

    /// A bounded direct-child page, resuming strictly after its last reference.
    /// `None` means a bounded suffix replay made progress; continue with the
    /// same key. New references preceding an existing cursor arrive through
    /// the ordinary Children follow opened at the caller's initial cut.
    pub async fn child_references_page(
        &self,
        after: Option<&Component>,
        maximum: usize,
    ) -> Result<Option<Vec<Component>>> {
        if maximum == 0 {
            return Err(Error::LimitExceeded);
        }
        let (_, complete) = self.refresh_bounded::<true>(maximum as u64).await?;
        if !complete {
            return Ok(None);
        }
        let index = self.index.lock().await;
        Ok(Some(reference_page(&index, after, maximum)))
    }

    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[doc(hidden)]
    pub async fn child_request(&self, reference: &Component) -> Result<FileValue> {
        let key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        };
        let snapshot = self
            .snapshot_files(std::slice::from_ref(&key), &Query::File(key.clone()))
            .await?;
        snapshot
            .files
            .into_iter()
            .next()
            .ok_or(Error::InvalidSegment)
    }

    #[doc(hidden)]
    pub async fn child_input_count(&self, reference: &Component) -> Result<u64> {
        self.refresh().await?;
        let index = self.index.lock().await;
        Ok(index
            .child_inputs
            .get(reference)
            .map_or(0, |inputs| inputs.len() as u64))
    }

    // The derived index preserves accepted order, independently of inbox names.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[doc(hidden)]
    pub async fn child_input(
        &self,
        reference: &Component,
        delivered: u64,
    ) -> Result<Option<FileValue>> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let key = index
            .child_inputs
            .get(reference)
            .and_then(|inputs| usize::try_from(delivered).ok().and_then(|i| inputs.get(i)));
        Ok(key
            .and_then(|key| index.files.get(key))
            .map(|stored| stored.file.clone()))
    }

    #[doc(hidden)]
    pub async fn child_lifecycle(&self, reference: Option<&Component>) -> Result<Snapshot> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let mut files = Vec::new();
        let candidates = match reference {
            Some(reference) => index
                .child_inputs
                .range(reference.clone()..=reference.clone()),
            None => index.child_inputs.range(..),
        };
        for (candidate, _) in candidates {
            let key = |file| FileKey::Child {
                reference: candidate.clone(),
                file,
            };
            for kind in [
                ChildFile::Control,
                ChildFile::Status,
                ChildFile::Delivered,
                ChildFile::ApprovalAttention,
            ] {
                if let Some(stored) = index.files.get(&key(kind)) {
                    files.push(stored.file.clone());
                }
            }
            if let Some((_, stored)) = index
                .files
                .range(key(ChildFile::Result(0))..=key(ChildFile::Result(u64::MAX)))
                .next_back()
            {
                files.push(stored.file.clone());
            }
        }
        let generation = self.generation(&index);
        drop(index);
        let query = reference.map_or(Query::Children, |reference| Query::Child(reference.clone()));
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        Ok(Snapshot {
            files,
            cursor: self.cursor(&query, generation, watermarks)?,
            generation,
        })
    }
}

fn result_page(
    index: &Index,
    reference: &Component,
    window: ChildResultWindow,
    maximum: usize,
) -> Vec<FileValue> {
    let first = match window {
        ChildResultWindow::None => return Vec::new(),
        ChildResultWindow::After(after) => match after.checked_add(1) {
            Some(first) => first,
            None => return Vec::new(),
        },
        ChildResultWindow::Latest => 1,
    };
    let key = |turn| FileKey::Child {
        reference: reference.clone(),
        file: ChildFile::Result(turn),
    };
    let range = index.files.range(key(first)..=key(u64::MAX));
    match window {
        ChildResultWindow::None => Vec::new(),
        ChildResultWindow::After(_) => range
            .take(maximum)
            .map(|(_, stored)| stored.file.clone())
            .collect(),
        ChildResultWindow::Latest => {
            let mut page: Vec<_> = range
                .rev()
                .take(maximum)
                .map(|(_, stored)| stored.file.clone())
                .collect();
            page.reverse();
            page
        }
    }
}

struct LifecycleObservation {
    state: ChildState,
    control: Option<Control>,
    status: Option<ChildStatus>,
    status_publication: Option<crate::OperationId>,
    claim: Option<ChildTurnClaim>,
    pending_inputs: u64,
    delivered: u64,
    results: Vec<FileValue>,
}

fn observation(
    index: &Index,
    reference: &Component,
    window: ChildResultWindow,
    maximum: usize,
) -> Result<LifecycleObservation> {
    let key = |file| FileKey::Child {
        reference: reference.clone(),
        file,
    };
    item_examined();
    if !index.child_requests.contains(reference) {
        return Err(Error::InvalidSegment);
    }
    let (state, control) = super::child_turn::child_lifecycle(index, reference)?;
    let status_file = index.files.get(&key(ChildFile::Status));
    let status = status_file
        .map(|stored| ChildStatus::parse(&stored.file.bytes))
        .transpose()?;
    let status_publication = status_file.and_then(|stored| stored.file.publication);
    let claim = status_publication
        .and_then(|operation| index.child_claims.get(&operation))
        .cloned();
    let delivered = match index.files.get(&key(ChildFile::Delivered)) {
        Some(stored) => {
            let (task, delivered): (crate::TaskId, u64) =
                serde_json::from_slice(&stored.file.bytes).map_err(|_| Error::InvalidSegment)?;
            if status.is_none_or(|status| status.task != task) {
                return Err(Error::FenceConflict);
            }
            delivered
        }
        None => 0,
    };
    item_examined();
    let inputs = index
        .child_inputs
        .get(reference)
        .map_or(0, |inputs| inputs.len() as u64);
    let pending_inputs = inputs.checked_sub(delivered).ok_or(Error::InvalidSegment)?;
    if claim.as_ref().is_some_and(|claim| {
        claim.reference != *reference
            || status.is_none_or(|status| status.task != claim.task || status.inputs != claim.turn)
            || delivered != claim.turn
    }) {
        return Err(Error::FenceConflict);
    }
    Ok(LifecycleObservation {
        state,
        control,
        status,
        status_publication,
        claim,
        pending_inputs,
        delivered,
        results: result_page(index, reference, window, maximum),
    })
}

fn reference_page(index: &Index, after: Option<&Component>, maximum: usize) -> Vec<Component> {
    index
        .child_requests
        .range((
            after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            std::ops::Bound::Unbounded,
        ))
        .take(maximum)
        .map(|reference| {
            item_examined();
            reference.clone()
        })
        .collect()
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    fn reference(n: u32) -> Result<Component> {
        Component::new(format!("worker-{n:06}"))
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn child_pages_match_the_independent_sorted_request_set(
            admitted in proptest::collection::vec(0u32..512, 0..256), window in 1usize..17,
        ) {
            let mut index = Index::default();
            let expected: std::collections::BTreeSet<_> = admitted.iter().map(|n| reference(*n)).collect::<Result<_>>()?;
            for n in 0..512 {
                let key = FileKey::Child { reference: reference(n)?, file: ChildFile::Inbox(Component::new("mail")?) };
                apply(&mut index, FileValue::new(key, Vec::new()), 1);
            }
            for n in admitted {
                let key = FileKey::Child { reference: reference(n)?, file: ChildFile::Request };
                apply(&mut index, FileValue::new(key, Vec::new()), 1);
            }
            let mut after = None;
            let mut actual = Vec::new();
            loop {
                let (page, examined) = crate::test_support::measure_task_volume_work(|| reference_page(&index, after.as_ref(), window));
                prop_assert!(examined <= window as u64);
                if page.is_empty() { break; }
                prop_assert!(after.as_ref().is_none_or(|old| old < &page[0]));
                after = page.last().cloned();
                actual.extend(page);
            }
            prop_assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
        }

        #[test]
        fn latest_results_select_the_bounded_suffix_of_the_same_index(
            turns in proptest::collection::btree_set(1u64..10_000, 0..256), maximum in 1usize..17,
        ) {
            let mut index = Index::default();
            let reference = reference(7)?;
            for turn in &turns {
                let key = FileKey::Child { reference: reference.clone(), file: ChildFile::Result(*turn) };
                apply(&mut index, FileValue::new(key, Vec::new()), *turn);
            }
            let (page, examined) = crate::test_support::measure_task_volume_work(|| {
                result_page(&index, &reference, ChildResultWindow::Latest, maximum)
            });
            let actual: Vec<_> = page.into_iter().filter_map(|file| match file.key {
                FileKey::Child { file: ChildFile::Result(turn), .. } => Some(turn),
                _ => None,
            }).collect();
            let expected: Vec<_> = turns.iter().skip(turns.len().saturating_sub(maximum)).copied().collect();
            prop_assert_eq!(actual, expected);
            prop_assert!(examined <= maximum as u64);
        }
    }
    fn populated() -> Result<Index> {
        let mut index = Index::default();
        for n in 0..100_000 {
            index.child_requests.insert(reference(n)?);
        }
        Ok(index)
    }
    #[test]
    fn one_child_page_is_bounded_with_one_hundred_thousand_siblings() -> Result<()> {
        let index = populated()?;
        let after = reference(99_990)?;
        let (page, examined) = crate::test_support::measure_task_volume_work(|| {
            reference_page(&index, Some(&after), 4)
        });
        assert_eq!(
            page,
            (99_991..99_995)
                .map(reference)
                .collect::<Result<Vec<_>>>()?
        );
        assert_eq!(examined, 4);
        Ok(())
    }

    #[test]
    fn child_observation_shares_one_cut_and_does_not_visit_unrelated_requests()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut index = Index::default();
        for n in 0..100_000 {
            let key = FileKey::Child {
                reference: reference(n)?,
                file: ChildFile::Request,
            };
            apply(&mut index, FileValue::new(key, b"work".to_vec()), 1);
        }
        let reference = reference(99_999)?;
        let task = crate::TaskId::from_bytes([83; 16]);
        let key = |file| FileKey::Child {
            reference: reference.clone(),
            file,
        };
        for turn in 2..=8 {
            let inbox = key(ChildFile::Inbox(Component::new(format!("mail-{turn}"))?));
            apply(&mut index, FileValue::new(inbox, b"work".to_vec()), turn);
        }
        let status = ChildStatus {
            task,
            state: ChildState::Running,
            settled: true,
            inputs: 8,
        };
        apply(
            &mut index,
            FileValue::new(key(ChildFile::Status), serde_json::to_vec(&status)?),
            9,
        );
        apply(
            &mut index,
            FileValue::new(key(ChildFile::Delivered), serde_json::to_vec(&(task, 8))?),
            10,
        );
        for turn in 1..=8 {
            let result = ChildTurnResult {
                task,
                turn,
                outcome: crate::Outcome::Cancelled,
            };
            apply(
                &mut index,
                FileValue::new(key(ChildFile::Result(turn)), serde_json::to_vec(&result)?),
                10 + turn,
            );
        }
        let (observed, examined) = crate::test_support::measure_task_volume_work(|| {
            observation(&index, &reference, ChildResultWindow::After(4), 4)
        });
        let observed = observed?;
        assert_eq!(observed.state, ChildState::Running);
        assert_eq!(observed.status, Some(status));
        assert_eq!(observed.pending_inputs, 0);
        assert_eq!(observed.delivered, 8);
        assert_eq!(
            observed
                .results
                .iter()
                .map(|file| ChildTurnResult::parse(&file.bytes).map(|result| result.turn))
                .collect::<Result<Vec<_>>>()?,
            vec![5, 6, 7, 8]
        );
        assert_eq!(examined, 9);
        let latest = observation(&index, &reference, ChildResultWindow::Latest, 1)?;
        assert_eq!(latest.results.len(), 1);
        assert_eq!(ChildTurnResult::parse(&latest.results[0].bytes)?.turn, 8);
        let lifecycle = observation(&index, &reference, ChildResultWindow::None, 1)?;
        assert!(lifecycle.results.is_empty());
        assert_eq!(lifecycle.status, latest.status);
        assert_eq!(lifecycle.delivered, latest.delivered);
        assert_eq!(lifecycle.pending_inputs, latest.pending_inputs);
        assert!(result_page(&index, &reference, ChildResultWindow::After(u64::MAX), 4).is_empty());

        // Restore the former all-files projection before filtering the child.
        // The same four-result request now examines every unrelated request.
        let (_, old) = crate::test_support::measure_task_volume_work(|| {
            index.files.iter().filter_map(|(candidate, stored)| {
            matches!(candidate, FileKey::Child { reference: child, file: ChildFile::Result(turn) } if child == &reference && *turn > 4)
                .then_some(stored.file.clone())
        }).take(4).collect::<Vec<_>>()
        });
        assert!(
            old > 100_000,
            "old broad projection escaped the bounded oracle"
        );
        Ok(())
    }
    #[test]
    #[should_panic(expected = "child page exceeded its work bound")]
    fn negative_control_filtering_all_siblings_exceeds_the_page_bound() {
        let Ok(index) = populated() else {
            panic!("invalid population fixture")
        };
        let Ok(after) = reference(99_990) else {
            panic!("invalid cursor fixture")
        };
        let (_, examined) = crate::test_support::measure_task_volume_work(|| {
            index
                .child_requests
                .iter()
                .filter(|candidate| {
                    item_examined();
                    *candidate > &after
                })
                .take(4)
                .cloned()
                .collect::<Vec<_>>()
        });
        assert!(examined <= 4, "child page exceeded its work bound");
    }
}
