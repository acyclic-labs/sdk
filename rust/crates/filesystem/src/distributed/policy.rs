use super::*;

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn publish_with_policy<G: AuthorityGuard>(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<(CommitOutcome, ProviderAllocation), AuthorityError<G::Error>> {
        self.publish_with_policy_cuts(request, deadline, &[], work, guard)
            .await
    }

    pub(super) async fn publish_with_policy_cuts<G: AuthorityGuard>(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        cuts: &[crate::PolicyCut],
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<(CommitOutcome, ProviderAllocation), AuthorityError<G::Error>> {
        let request = self.with_policy_cuts(request, cuts, work)?;
        self.commit(request, deadline, work, guard).await
    }

    pub(super) fn with_policy_cuts(
        &self,
        mut request: CommitRequest,
        cuts: &[crate::PolicyCut],
        work: &mut AuthorityWork<'_>,
    ) -> Result<CommitRequest, AuthorityStoreError> {
        for cut in cuts {
            request = self.fence_policy(request, *cut, work)?;
        }
        if let Some(current) = crate::PolicyCut::current() {
            let mut captured = false;
            for cut in cuts {
                let visit = WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                };
                work.admit(visit)?;
                work.charge(visit)?;
                if *cut == current {
                    captured = true;
                    break;
                }
            }
            if !captured {
                request = self.fence_policy(request, current, work)?;
            }
        }
        Ok(request)
    }

    /// Every disclosure publication checks its source in the same Streams
    /// transaction. No earlier check, writer-local registry or lease can replace it.
    fn fence_policy(
        &self,
        mut request: CommitRequest,
        cut: crate::PolicyCut,
        work: &mut AuthorityWork<'_>,
    ) -> Result<CommitRequest, AuthorityStoreError> {
        if cut.owner != self.operation_claims().owner_identity() {
            return Err(AuthorityStoreError::RejectedStatic(
                "policy cut and publication use different Streams owners",
            ));
        }
        let mut conditions = work.vec(request.conditions.len() + 3)?;
        conditions.append(&mut request.conditions);
        merge_condition(
            &mut conditions,
            CommitCondition::Tail {
                path: work.path(&self.placement, cut.authority, "/records", None)?,
                expected: cut
                    .head
                    .sequence
                    .get()
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)?,
            },
            work,
        )?;
        merge_condition(
            &mut conditions,
            CommitCondition::Tail {
                path: work.path(&self.placement, cut.authority, "/epochs", None)?,
                expected: cut.epoch_tail,
            },
            work,
        )?;
        merge_condition(
            &mut conditions,
            CommitCondition::Absent {
                path: work.path(&self.placement, cut.authority, "/retirement", None)?,
            },
            work,
        )?;
        request.conditions = conditions;
        // A rejected stale cut cannot consume a later freshly authorized attempt.
        let key = work.key(
            self.placement.key_scope(),
            b"policy-cut-attempt\0",
            &[
                &cut.authority.into_bytes(),
                cut.head.digest.as_bytes(),
                &cut.head.sequence.get().to_le_bytes(),
                &cut.epoch_tail.to_le_bytes(),
            ],
        )?;
        request.idempotency_key = work.folded_key(
            b"authority-policy-attempt\0",
            &request.idempotency_key,
            &key,
        )?;
        Ok(request)
    }
}

pub(super) fn condition_path(condition: &CommitCondition) -> &StreamPath {
    match condition {
        CommitCondition::Head { path, .. }
        | CommitCondition::Tail { path, .. }
        | CommitCondition::Present { path }
        | CommitCondition::Absent { path } => path,
    }
}

/// Adds `condition` to `conditions`, the one merge of every condition a
/// commit composes. Each comparison is admitted and charged as examined work.
/// A condition on a path already held must agree with it: an equal one, or a
/// `Tail` matching a `Head`'s tail, collapses into the stronger of the two in
/// either order, releasing the discarded path; any other pairing refuses the
/// commit and leaves the held conditions unchanged.
pub(super) fn merge_condition(
    conditions: &mut Vec<CommitCondition>,
    condition: CommitCondition,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    for existing in conditions.iter_mut() {
        let examined = WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        };
        work.admit(examined)?;
        work.charge(examined)?;
        if condition_path(existing) != condition_path(&condition) {
            continue;
        }
        let (compatible, stronger) = match (&*existing, &condition) {
            (
                CommitCondition::Head {
                    expected: actual, ..
                },
                CommitCondition::Tail { expected, .. },
            ) => (actual == expected, false),
            (
                CommitCondition::Tail { expected, .. },
                CommitCondition::Head {
                    expected: actual, ..
                },
            ) => (actual == expected, true),
            _ => (*existing == condition, false),
        };
        if !compatible {
            return Err(AuthorityStoreError::RejectedStatic(
                "a commit condition conflicts with one the commit already holds",
            ));
        }
        work.release(condition_path(&condition).as_str().len() as u64)?;
        if stronger {
            *existing = condition;
        }
        return Ok(());
    }
    conditions.push(condition);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::StreamProvider;

    fn comparison_prefix(omit_charge: bool) -> Result<(), Box<dyn std::error::Error>> {
        for allowance in 0..=4 {
            let token = CancellationToken::new();
            let mut budget = WorkBudget::UNBOUNDED;
            budget.items_examined = allowance;
            let mut work = AuthorityWork::new(budget, &token);
            let mut conditions = ["policy/a", "policy/b", "policy/c", "policy/target"]
                .into_iter()
                .map(|path| StreamPath::new(path).map(|path| CommitCondition::Absent { path }))
                .collect::<Result<Vec<_>, _>>()?;
            let candidate = CommitCondition::Absent {
                path: StreamPath::new("policy/target")?,
            };
            work.hold_opaque(condition_path(&candidate).as_str().len() as u64)?;
            let result = if omit_charge {
                conditions.iter().try_for_each(|_| {
                    work.admit(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })
                })
            } else {
                merge_condition(&mut conditions, candidate, &mut work)
            };
            assert_eq!(
                work.spent().items_examined,
                allowance,
                "policy comparison prefix was not charged"
            );
            assert_eq!(
                result.is_ok(),
                allowance == 4,
                "policy comparisons escaped their allowance"
            );
            assert_eq!(conditions.len(), 4);
        }
        Ok(())
    }

    /// A `Head` and a matching `Tail` on one path keep the `Head`, in either
    /// insertion order; a mismatched tail refuses and keeps what was held.
    #[test]
    fn a_matching_tail_and_head_keep_the_head_in_either_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = StreamPath::new("policy/lineage")?;
        let head = CommitCondition::Head {
            path: path.clone(),
            expected: 3,
            commit_id: acyclic_stream::CommitId::from_bytes([7; 32]),
        };
        let tail = |expected| CommitCondition::Tail {
            path: path.clone(),
            expected,
        };
        for order in [[head.clone(), tail(3)], [tail(3), head.clone()]] {
            let token = CancellationToken::new();
            let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
            let [first, second] = order;
            let mut conditions = vec![first];
            work.hold_opaque(condition_path(&second).as_str().len() as u64)?;
            merge_condition(&mut conditions, second, &mut work)?;
            assert_eq!(conditions, [head.clone()]);
            work.hold_opaque(path.as_str().len() as u64)?;
            assert!(merge_condition(&mut conditions, tail(4), &mut work).is_err());
            assert_eq!(conditions, [head.clone()]);
        }
        Ok(())
    }

    #[test]
    fn policy_condition_comparisons_charge_each_admitted_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        comparison_prefix(false)
    }

    #[test]
    #[should_panic(expected = "policy comparison prefix was not charged")]
    fn uncharged_policy_comparisons_negative_control() {
        let outcome = comparison_prefix(true);
        assert!(
            outcome.is_ok(),
            "control failed before its work oracle: {outcome:?}"
        );
    }

    async fn duplicate_condition_control(
        omit_merge: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let provider = acyclic_stream::MemoryStream::default();
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        let path = StreamPath::new("policy/source")?;
        let mut conditions = vec![CommitCondition::Absent { path: path.clone() }];
        let candidate = CommitCondition::Absent { path };
        if omit_merge {
            conditions.push(candidate);
        } else {
            // Model the funded candidate path which the production fence owns.
            work.hold_opaque(condition_path(&candidate).as_str().len() as u64)?;
            merge_condition(&mut conditions, candidate, &mut work)?;
        }
        let outcome = provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                conditions,
                mutations: vec![CommitMutation::Append {
                    // The destination must have its own condition. Use the same
                    // authority to isolate duplicate policy/publication checks.
                    path: StreamPath::new("policy/source")?,
                    records: vec![acyclic_stream::OwnedBytes::from_static(b"published").into()],
                }],
                idempotency_key: acyclic_stream::IdempotencyKey::new(
                    acyclic_stream::OwnedBytes::from_static(b"policy-conditions"),
                )?,
            })
            .await;
        assert!(
            matches!(outcome, Ok(CommitOutcome::Committed(_))),
            "compatible policy conditions prevented publication: {outcome:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn matching_policy_conditions_publish_once() -> Result<(), Box<dyn std::error::Error>> {
        duplicate_condition_control(false).await
    }

    #[tokio::test]
    #[should_panic(expected = "compatible policy conditions prevented publication")]
    async fn duplicate_policy_conditions_negative_control() {
        let outcome = duplicate_condition_control(true).await;
        assert!(
            outcome.is_ok(),
            "control failed before its publication oracle: {outcome:?}"
        );
    }
}
