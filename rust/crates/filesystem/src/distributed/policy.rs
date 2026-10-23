use super::*;

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) async fn publish_with_policy<G: AuthorityGuard>(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<(CommitOutcome, ProviderAllocation), AuthorityError<G::Error>> {
        let request = self.fence_policy(request, work)?;
        self.commit(request, deadline, work, guard).await
    }

    /// Every disclosure publication checks its source in the same Streams
    /// transaction. No earlier check, writer-local registry or lease can replace it.
    fn fence_policy(
        &self,
        mut request: CommitRequest,
        work: &mut AuthorityWork<'_>,
    ) -> Result<CommitRequest, AuthorityStoreError> {
        let Some(cut) = crate::PolicyCut::current() else {
            return Ok(request);
        };
        if cut.owner != self.operation_claims().owner_identity() {
            return Err(AuthorityStoreError::RejectedStatic(
                "policy cut and publication use different Streams owners",
            ));
        }
        let mut conditions = work.vec(request.conditions.len() + 3)?;
        conditions.append(&mut request.conditions);
        merge_policy_condition(
            &mut conditions,
            CommitCondition::Tail {
                path: work.path(cut.authority, "/records", None)?,
                expected: cut
                    .head
                    .sequence
                    .get()
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)?,
            },
            work,
        )?;
        merge_policy_condition(
            &mut conditions,
            CommitCondition::Tail {
                path: work.path(cut.authority, "/epochs", None)?,
                expected: cut.epoch_tail,
            },
            work,
        )?;
        merge_policy_condition(
            &mut conditions,
            CommitCondition::Absent {
                path: work.path(cut.authority, "/retirement", None)?,
            },
            work,
        )?;
        request.conditions = conditions;
        // A rejected stale cut cannot consume a later freshly authorized attempt.
        request.idempotency_key = work.key(
            b"policy-cut-attempt\0",
            &[
                request.idempotency_key.as_bytes(),
                &cut.authority.into_bytes(),
                cut.head.digest.as_bytes(),
                &cut.head.sequence.get().to_le_bytes(),
                &cut.epoch_tail.to_le_bytes(),
            ],
        )?;
        Ok(request)
    }
}

fn condition_path(condition: &CommitCondition) -> &StreamPath {
    match condition {
        CommitCondition::Head { path, .. }
        | CommitCondition::Tail { path, .. }
        | CommitCondition::Absent { path } => path,
    }
}

fn merge_policy_condition(
    conditions: &mut Vec<CommitCondition>,
    policy: CommitCondition,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    for existing in conditions.iter() {
        let examined = WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        };
        work.admit(examined)?;
        work.charge(examined)?;
        if condition_path(existing) != condition_path(&policy) {
            continue;
        }
        let compatible = match (existing, &policy) {
            (
                CommitCondition::Head {
                    expected: actual, ..
                },
                CommitCondition::Tail { expected, .. },
            ) => actual == expected,
            _ => existing == &policy,
        };
        if !compatible {
            return Err(AuthorityStoreError::RejectedStatic(
                "policy cut conflicts with the publication condition",
            ));
        }
        work.release(condition_path(&policy).as_str().len() as u64)?;
        return Ok(());
    }
    conditions.push(policy);
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
                merge_policy_condition(&mut conditions, candidate, &mut work)
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
            merge_policy_condition(&mut conditions, candidate, &mut work)?;
        }
        let outcome = provider
            .commit(CommitRequest {
                conditions,
                mutations: vec![CommitMutation::Append {
                    path: StreamPath::new("policy/destination")?,
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
