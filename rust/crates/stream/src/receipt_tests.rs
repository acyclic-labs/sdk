#[cfg(all(test, not(target_arch = "wasm32")))]
mod receipt_membership_tests {
    use crate::OwnedBytes as Bytes;
    use crate::*;

    #[tokio::test]
    async fn compact_membership_borrows_the_original_request_and_rejects_corrupt_ranges()
    -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let paths = [
            "b-head",
            "f-tail",
            "h-present",
            "j-unconditioned",
            "z-source",
        ]
        .map(StreamPath::new)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        let base = provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                effects: vec![crate::CommitEffect::hard(
                    Vec::new(),
                    paths
                        .iter()
                        .map(|path| CommitMutation::Append {
                            path: path.clone(),
                            records: vec![Bytes::from_static(b"base").into()],
                        })
                        .collect(),
                )],
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let CommitOutcome::Committed(base) = base else {
            return Err(StreamError::Unavailable);
        };
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            effects: vec![crate::CommitEffect::hard(
                vec![
                    CommitCondition::Absent {
                        path: StreamPath::new("x-new")?,
                    },
                    CommitCondition::Present {
                        path: paths[2].clone(),
                    },
                    CommitCondition::Tail {
                        path: paths[1].clone(),
                        expected: 1,
                    },
                    CommitCondition::Head {
                        path: paths[0].clone(),
                        expected: 1,
                        commit_id: base.commit_id,
                    },
                ],
                paths[..4]
                    .iter()
                    .map(|path| CommitMutation::Append {
                        path: path.clone(),
                        records: vec![Bytes::from_static(b"next").into()],
                    })
                    .collect(),
            )],
            idempotency_key: IdempotencyKey::unique(),
        };
        request.effects[0].mutations.push(CommitMutation::Append {
            path: StreamPath::new("x-new")?,
            records: vec![Bytes::from_static(b"new").into()],
        });
        for (name, at_tail, records) in [
            ("d-fork", 1, vec![Bytes::from_static(b"forked").into()]),
            ("g-empty-fork", 0, Vec::new()),
        ] {
            let destination = StreamPath::new(name)?;
            request.effects[0].conditions.push(CommitCondition::Absent {
                path: destination.clone(),
            });
            request.effects[0].mutations.push(CommitMutation::Fork {
                source: paths[4].clone(),
                destination,
                at_tail,
                records,
                exclude: Vec::new(),
            });
        }
        request::validate_commit(&mut request)?;
        let CommitOutcome::Committed(receipt) = provider.commit(request.clone()).await? else {
            return Err(StreamError::Unavailable);
        };
        let mut validation = None;
        let allocations = acyclic_test_support::measure(|| {
            validation = Some(receipt.validate_request(&request));
        });
        validation.expect("validation ran")?;
        assert_eq!(allocations.count_total, 0);
        for index in 0..receipt.appends.len() {
            let mut missing = receipt.clone();
            missing.appends.remove(index);
            assert_eq!(
                missing
                    .validate_request(&request)
                    .map_err(|error| error.category()),
                Err(StreamCategory::InvalidResponse)
            );
            let mut wrong_range = receipt.clone();
            wrong_range.appends[index].tail += 1;
            assert_eq!(
                wrong_range
                    .validate_request(&request)
                    .map_err(|error| error.category()),
                Err(StreamCategory::InvalidResponse)
            );
        }
        for index in 0..receipt.forks.len() {
            let mut missing = receipt.clone();
            missing.forks.remove(index);
            assert_eq!(
                missing
                    .validate_request(&request)
                    .map_err(|error| error.category()),
                Err(StreamCategory::InvalidResponse)
            );
            for change in 0..6 {
                let mut corrupt = receipt.clone();
                let fork = &mut corrupt.forks[index];
                match change {
                    0 => fork.source = paths[0].clone(),
                    1 => fork.destination = paths[0].clone(),
                    2 => fork.forked_at += 1,
                    3 => fork.tail += 1,
                    4 => fork.commit_id = base.commit_id,
                    _ => fork.revision = base.revision.clone(),
                }
                assert_eq!(
                    corrupt
                        .validate_request(&request)
                        .map_err(|error| error.category()),
                    Err(StreamCategory::InvalidResponse)
                );
            }
        }
        let mut reordered = receipt.clone();
        reordered.appends.swap(0, 1);
        assert_eq!(
            reordered
                .validate_request(&request)
                .map_err(|error| error.category()),
            Err(StreamCategory::InvalidResponse)
        );
        let mut reordered = receipt.clone();
        reordered.forks.swap(0, 1);
        assert_eq!(
            reordered
                .validate_request(&request)
                .map_err(|error| error.category()),
            Err(StreamCategory::InvalidResponse)
        );
        let mut surplus = receipt.clone();
        surplus.appends.push(receipt.appends[0].clone());
        assert_eq!(
            surplus
                .validate_request(&request)
                .map_err(|error| error.category()),
            Err(StreamCategory::InvalidResponse)
        );
        // A consistent but shifted range cannot override an original anchor.
        for index in [0, 1, 4] {
            let mut shifted = receipt.clone();
            shifted.appends[index].start += 1;
            shifted.appends[index].tail += 1;
            assert_eq!(
                shifted
                    .validate_request(&request)
                    .map_err(|error| error.category()),
                Err(StreamCategory::InvalidResponse)
            );
        }
        // Present/unconditioned starts have no independent anchor in this
        // request. Structural membership must not fabricate such a proof.
        for index in [2, 3] {
            let mut shifted = receipt.clone();
            shifted.appends[index].start += 1;
            shifted.appends[index].tail += 1;
            shifted.validate_request(&request)?;
        }
        let mut overflow = receipt.clone();
        overflow.appends[3].start = u64::MAX;
        overflow.appends[3].tail = 0;
        assert_eq!(
            overflow
                .validate_request(&request)
                .map_err(|error| error.category()),
            Err(StreamCategory::InvalidResponse)
        );
        let mut noncanonical = request.clone();
        noncanonical.effects[0].conditions.swap(0, 1);
        assert_eq!(
            receipt
                .validate_request(&noncanonical)
                .map_err(|error| error.category()),
            Err(StreamCategory::InvalidArgument)
        );
        noncanonical = request.clone();
        noncanonical.effects[0].mutations.swap(0, 1);
        assert_eq!(
            receipt
                .validate_request(&noncanonical)
                .map_err(|error| error.category()),
            Err(StreamCategory::InvalidArgument)
        );
        assert_eq!(
            provider.commit(request.clone()).await?,
            CommitOutcome::Committed(receipt.clone())
        );
        receipt.validate_request(&request)?;
        Ok(())
    }
}
