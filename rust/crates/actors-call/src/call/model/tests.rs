use super::*;
use crate::call::codec_fuzz;

fn schedules(remaining: &mut Vec<Hop>, prefix: &mut Vec<Hop>, out: &mut Vec<Vec<Hop>>) {
    if remaining.is_empty() {
        out.push(prefix.clone());
        return;
    }
    let mut tried = Vec::new();
    for (index, hop) in remaining.clone().into_iter().enumerate() {
        if tried.contains(&hop) {
            continue;
        }
        tried.push(hop);
        remaining.remove(index);
        prefix.push(hop);
        schedules(remaining, prefix, out);
        prefix.pop();
        remaining.insert(index, hop);
    }
}

async fn explore(fault: Fault) -> Result<usize, String> {
    let mut all = Vec::new();
    schedules(
        &mut vec![
            Hop::Request,
            Hop::Executor,
            Hop::Executor,
            Hop::Executor,
            Hop::Rival,
            Hop::Cancel,
            Hop::Fence,
        ],
        &mut Vec::new(),
        &mut all,
    );
    assert_eq!(all.len(), 840);
    let mut violations = 0;
    for requested in [false, true] {
        for schedule in &all {
            let mut model = Model::new(fault, Outcome::Ok(OwnedBytes::from(&b"done"[..])))?;
            model.requested = requested;
            for hop in schedule {
                if let Err(violation) = model.step(*hop).await {
                    if fault == Fault::None {
                        return Err(format!("requested={requested} {schedule:?}: {violation}"));
                    }
                    violations += 1;
                    break;
                }
            }
        }
    }
    Ok(violations)
}

/// An executor terminal names the original acceptance's session. Other hops
/// may settle without a session, including before any executor accepts.
async fn terminal_sessions() -> Result<(), String> {
    for accepted in [false, true] {
        for session in [None, Some(true), Some(false)] {
            for terminal in [
                Outcome::Ok(OwnedBytes::from(&b"done"[..])),
                Outcome::Error("failed".into()),
                Outcome::Cancelled("stop".into()),
                Outcome::Indeterminate("lost".into()),
            ] {
                let mut model =
                    Model::new(Fault::None, Outcome::Ok(OwnedBytes::from(&b"done"[..])))?;
                model.requested = false;
                let mut journal = Journal::new(model.call.log());
                if accepted {
                    model.step(Hop::Executor).await?;
                    for record in model.records(model.call.log(), 0).await? {
                        journal.apply(&record).map_err(|error| error.to_string())?;
                    }
                    model.step(Hop::Executor).await?;
                    let records = model.records(model.call.log(), 0).await?;
                    let chunk = records.last().ok_or("missing committed chunk")?;
                    let work = acyclic_test_support::measure(|| {
                        assert!(matches!(journal.apply(chunk), Ok(Entry::Chunk(_))));
                    });
                    assert_eq!(work.count_total, 0, "a chunk must borrow its session");
                    assert_eq!(work.bytes_total, 0);
                    if session.is_none() && matches!(terminal, Outcome::Ok(_)) {
                        // Restoring the second path decode to the original chunk
                        // check fails the zero-allocation assertion above.
                        let duplicate = acyclic_test_support::measure(|| {
                            assert_eq!(fence(chunk).unwrap().as_ref(), Some(&model.fence));
                        });
                        assert!(duplicate.count_total > 0);
                        println!(
                            "chunk_session_allocations={} chunk_session_bytes={} duplicate_decode_allocations={} duplicate_decode_bytes={}",
                            work.count_total,
                            work.bytes_total,
                            duplicate.count_total,
                            duplicate.bytes_total
                        );
                    }
                }
                let original = journal.clone();
                let other = StreamPath::new("executor/sessions/other/ended")
                    .map_err(|error| error.to_string())?;
                let fence = session.map(|same| if same { &model.fence } else { &other });
                let publication = model
                    .call
                    .settle(fence, &terminal)
                    .map_err(|error| error.to_string())?;
                assert!(model.publish(publication).await?);
                let records = model.records(model.call.log(), 0).await?;
                let record = records.last().ok_or("missing committed terminal")?;
                let outcome = journal.apply(record);
                if session.is_none() || (accepted && session == Some(true)) {
                    let expected = match terminal {
                        Outcome::Cancelled(reason) if accepted => {
                            Outcome::Indeterminate(format!("cancelled after acceptance: {reason}"))
                        }
                        terminal => terminal,
                    };
                    assert_eq!(
                        outcome.map_err(|error| error.to_string())?,
                        Entry::Terminal(expected)
                    );
                    assert_eq!(journal.next(), record.sequence + 1);
                    assert_eq!(
                        journal
                            .receipt(model.call.log())
                            .map(|receipt| receipt.terminal),
                        Some(Committed::of(record))
                    );
                } else {
                    // Removing only the terminal's session check admits this real
                    // committed record and fails the original-reader assertion.
                    assert!(matches!(
                        outcome,
                        Err(Error::Malformed("call terminal from another session"))
                    ));
                    assert_eq!(journal, original, "refusal changed the original reader");
                }
            }
        }
    }
    Ok(())
}

/// Invalid reason bytes must fail before acquiring an owned string. Other
/// calls on the same provider still progress, and successful output is opaque.
async fn terminal_reasons() -> Result<(), String> {
    let root = StreamPath::new("caller").map_err(|error| error.to_string())?;
    let call = CallPath::new(&root, "cancel-reason").map_err(|error| error.to_string())?;
    let mut nonempty_allocations = None;
    for reason in [String::new(), "stop".into(), "\u{03bb}".repeat(50_000)] {
        let mut cancellation = None;
        let work = acyclic_test_support::measure(|| {
            cancellation = Some(call.cancel(&reason).expect("cancellation"));
        });
        // The former construction copies into a temporary String, then into
        // the original terminal. Both paths must build the identical commit.
        let mut copied = None;
        let duplicate = acyclic_test_support::measure(|| {
            copied = Some(
                call.settle(None, &Outcome::Cancelled(reason.clone()))
                    .expect("owned cancellation"),
            );
        });
        assert_eq!(cancellation, copied);
        assert_eq!(
            duplicate.count_total,
            work.count_total + u64::from(!reason.is_empty())
        );
        assert_eq!(
            duplicate.bytes_total,
            work.bytes_total + reason.len() as u64
        );
        if !reason.is_empty() {
            assert_eq!(
                *nonempty_allocations.get_or_insert(work.count_total),
                work.count_total
            );
        }
    }
    let mut long = vec![b'a'; 16_384];
    long.push(0xff);
    for bytes in [vec![0xff], vec![0xc0, 0x80], vec![0xe2, 0x82], long] {
        for accepted in [false, true] {
            for terminal in [
                Outcome::Error("failure".into()),
                Outcome::Cancelled("stop".into()),
                Outcome::Indeterminate("lost".into()),
            ] {
                let mut model =
                    Model::new(Fault::None, Outcome::Ok(OwnedBytes::from(&b"done"[..])))?;
                model.requested = false;
                let mut journal = Journal::new(model.call.log());
                if accepted {
                    model.step(Hop::Executor).await?;
                    for record in model.records(model.call.log(), 0).await? {
                        journal.apply(&record).map_err(|error| error.to_string())?;
                    }
                }
                let original = journal.clone();
                let mut publication = model
                    .call
                    .settle(None, &terminal)
                    .map_err(|error| error.to_string())?;
                // Keep the original terminal's guards, attributes and marker;
                // only its externally supplied reason bytes are malformed.
                let CommitMutation::Append { records, .. } = &mut publication.mutations[0] else {
                    return Err("terminal has no original journal append".into());
                };
                records[0].value = OwnedBytes::from(bytes.clone());
                assert!(model.publish(publication).await?);
                let records = model.records(model.call.log(), 0).await?;
                let record = records.last().ok_or("missing malformed terminal")?;
                let work = acyclic_test_support::measure(|| {
                    assert!(matches!(
                        Entry::decode(record),
                        Err(Error::Malformed("call reason is not UTF-8"))
                    ));
                    assert!(matches!(
                        journal.apply(record),
                        Err(Error::Malformed("call reason is not UTF-8"))
                    ));
                });
                assert_eq!(work.count_total, 0, "rejected reasons must stay borrowed");
                assert_eq!(work.bytes_total, 0);
                assert_eq!(journal, original, "refusal changed the original reader");
                let copied = acyclic_test_support::measure(|| {
                    assert!(String::from_utf8(record.value.to_vec()).is_err());
                });
                assert_eq!(copied.count_total, 1, "the former decoder copies first");
                assert_eq!(copied.bytes_total, bytes.len() as u64);

                for (id, outcome) in [
                    ("valid", Outcome::Error("failed: \u{03bb}".into())),
                    ("opaque", Outcome::Ok(record.value.clone())),
                ] {
                    let sibling = CallPath::new(&root, id).map_err(|error| error.to_string())?;
                    assert!(
                        model
                            .publish(
                                sibling
                                    .settle(None, &outcome)
                                    .map_err(|error| error.to_string())?
                            )
                            .await?
                    );
                    let mut reader = Journal::new(sibling.log());
                    for record in model.records(sibling.log(), 0).await? {
                        assert_eq!(
                            reader.apply(&record).map_err(|error| error.to_string())?,
                            Entry::Terminal(outcome.clone())
                        );
                    }
                    assert_eq!(reader.outcome(), Some(&outcome));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn every_interleaving_settles_once_and_cancel_never_waits() -> Result<(), String> {
    codec_fuzz::seeds();
    check(&[]).await?;
    check(&[0, 1, 1, 1, 6, 4, 3]).await?;
    assert_eq!(explore(Fault::None).await?, 0);
    terminal_sessions().await?;
    terminal_reasons().await?;
    claim_recovery().await?;
    source_bound_claims().await?;
    // Preserve the owning acceptance control: only its settled guard is
    // removed from the original requestless, already cancelled call.
    let mut model = Model::new(Fault::None, Outcome::Ok(OwnedBytes::from(&b"done"[..])))?;
    model.step(Hop::Cancel).await?;
    let mut acceptance = model
        .call
        .accept(
            &Accepted {
                executor: OwnedBytes::from(&b"e1"[..]),
                fence: Some(model.fence.clone()),
            },
            true,
        )
        .map_err(|error| error.to_string())?;
    acceptance.conditions.retain(|condition| {
        !matches!(condition, CommitCondition::Absent { path } if path == model.call.settled())
    });
    assert!(model.publish(acceptance).await?);
    assert!(model.check(Hop::Rival).await.is_err());
    for fault in [
        Fault::Acceptance,
        Fault::Settlement,
        Fault::Fence,
        Fault::Retry,
        Fault::Producer,
    ] {
        assert!(explore(fault).await? > 0);
    }
    // Select each terminal kind independently of the schedule. Acceptance,
    // multiple chunks, settlement and a later replay/cancel/fence all use the
    // same generated entry point as arbitrary fuzz inputs.
    for terminal in [6, 13, 20, 27] {
        check(&[terminal, 0, 1, 5, 1, 1, 6, 3, 4]).await?;
    }
    Ok(())
}

/// Original committed holds survive acceptance reply loss, cancellation and
/// session loss. Recovery is tied to that acceptance and never redispatches.
async fn claim_recovery() -> Result<(), String> {
    for requested in [false, true] {
        for cancel in [false, true] {
            let mut model = Model::new(Fault::None, Outcome::Ok(OwnedBytes::from("done")))?;
            model.requested = requested;
            assert!(
                model
                    .journal
                    .recover(&model.call, "lost")
                    .map_err(|e| e.to_string())?
                    .is_none()
            );
            if requested {
                model.step(Hop::Request).await?;
            }
            model.step(Hop::Executor).await?;
            assert_eq!(
                model.journal.hold(),
                requested.then_some(crate::escrow::Escrow::new(Some(5)))
            );
            let hold = model.journal.hold();
            model.replay().await?;
            assert_eq!(model.tail(model.call.accepted()).await?, 1);
            let recovery = model
                .journal
                .recover(&model.call, "lost")
                .map_err(|e| e.to_string())?
                .ok_or("missing accepted recovery")?;
            assert!(
                !model.publish(recovery).await?,
                "a live session cannot be recovered"
            );
            if cancel {
                model.step(Hop::Cancel).await?;
            }
            model.step(Hop::Fence).await?;
            assert_eq!(
                model.journal.hold(),
                hold,
                "terminal released the original hold"
            );
            assert!(matches!(
                model.journal.outcome(),
                Some(Outcome::Indeterminate(_))
            ));
            let receipt = model
                .journal
                .receipt(model.call.log())
                .ok_or("missing committed terminal")?;
            let terminal = model
                .records(model.call.log(), receipt.terminal.sequence)
                .await?;
            assert_eq!(terminal.len(), 1);
            assert_eq!(receipt.terminal.commit_id, terminal[0].commit_id);
            model.replay().await?;
            model.step(Hop::Rival).await?;
            assert_eq!(model.tail(model.call.accepted()).await?, 1);
            assert_eq!(model.tail(model.call.settled()).await?, 1);
        }
    }
    for missing_fence in [false, true] {
        let mut model = Model::new(Fault::None, Outcome::Ok(OwnedBytes::from("done")))?;
        model.step(Hop::Request).await?;
        model.step(Hop::Executor).await?;
        let mut journal = model.journal.clone();
        if !missing_fence {
            model
                .publish(Commit {
                    conditions: Vec::new(),
                    mutations: vec![append(&model.fence, AppendRecord::from("lost"))],
                })
                .await?;
            // A reader handed another acceptance's commit cannot recover this
            // call. Removing its head guard admits the same wrong evidence.
            journal
                .acceptance
                .as_mut()
                .ok_or("missing acceptance")?
                .commit_id = CommitId::from_bytes([99; 32]);
        }
        let mut recovery = journal
            .recover(&model.call, "lost")
            .map_err(|e| e.to_string())?
            .ok_or("missing recovery")?;
        assert!(!model.publish(recovery.clone()).await?);
        recovery.conditions.retain(|condition| {
            !matches!(condition, CommitCondition::Present { path } if missing_fence && path == &model.fence)
                && !matches!(condition, CommitCondition::Head { path, .. } if !missing_fence && path == model.call.accepted())
        });
        assert!(
            model.publish(recovery).await?,
            "negative control did not remove the required guard"
        );
    }
    Ok(())
}

/// A commit identity covers every publication in its batch. It cannot replace
/// the original destination when two different executor sessions share it.
async fn source_bound_claims() -> Result<(), String> {
    let mut model = Model::new(Fault::None, Outcome::Ok(OwnedBytes::from("done")))?;
    let sibling = CallPath::new(&StreamPath::new("caller").map_err(|e| e.to_string())?, "c2")
        .map_err(|e| e.to_string())?;
    let live_fence = StreamPath::new("other/sessions/1/ended").map_err(|e| e.to_string())?;
    for accept in [false, true] {
        let mut joined = Commit {
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        for (call, fence) in [(&model.call, &model.fence), (&sibling, &live_fence)] {
            let step = if accept {
                call.accept(
                    &Accepted {
                        executor: OwnedBytes::from("executor"),
                        fence: Some(fence.clone()),
                    },
                    true,
                )
            } else {
                call.request(OwnedBytes::from("run"), crate::escrow::Escrow::new(Some(1)))
            }
            .map_err(|e| e.to_string())?;
            joined.conditions.extend(step.conditions);
            joined.mutations.extend(step.mutations);
        }
        assert!(model.publish(joined).await?);
    }
    let original = model.records(model.call.log(), 0).await?;
    let other = model.records(sibling.log(), 0).await?;
    assert_eq!(original.len(), 3);
    assert_eq!(other.len(), 3);
    assert_eq!(original[2].commit_id, other[2].commit_id);
    let child = CallPath::new(&StreamPath::new("child").map_err(|e| e.to_string())?, "c1")
        .map_err(|e| e.to_string())?;
    let grandchild = CallPath::new(
        &StreamPath::new("grandchild").map_err(|e| e.to_string())?,
        "c1",
    )
    .map_err(|e| e.to_string())?;
    for (source, destination) in [(model.call.clone(), child.clone()), (child, grandchild)] {
        assert!(
            model
                .publish(Commit {
                    conditions: vec![CommitCondition::Absent {
                        path: destination.log().clone(),
                    }],
                    mutations: vec![CommitMutation::Fork {
                        source: source.log().clone(),
                        destination: destination.log().clone(),
                        at_tail: 3,
                        exclude: Vec::new(),
                        records: Vec::new(),
                    }],
                })
                .await?
        );
        let inherited = model.records(destination.log(), 0).await?;
        assert_eq!(inherited, original);
        let mut branch = Journal::new(destination.log());
        let unchanged = branch.clone();
        codec_fuzz::check_record(&inherited[0], &branch).map_err(|error| error.to_string())?;
        assert!(branch.apply(&inherited[0]).is_err());
        assert_eq!(branch, unchanged, "copied prefix supplied a branch hold");
        // Negative control lets the first copied record choose the source,
        // exposing the parent's accepted hold as the branch's live work.
        let mut unbound = Journal::new(&inherited[0].origin);
        for record in &inherited {
            unbound.apply(record).map_err(|e| e.to_string())?;
        }
        assert!(unbound.accepted().is_some());
        assert!(unbound.hold().is_some());
    }
    let mut journal = Journal::new(model.call.log());
    let unstarted = journal.clone();
    codec_fuzz::check_record(&other[0], &journal).map_err(|error| error.to_string())?;
    assert!(journal.apply(&other[0]).is_err());
    assert_eq!(
        journal, unstarted,
        "foreign first record chose the reader's source"
    );
    for record in &original[..2] {
        journal.apply(record).map_err(|e| e.to_string())?;
    }
    let before = journal.clone();
    codec_fuzz::check_record(&other[2], &journal).map_err(|error| error.to_string())?;
    assert!(journal.apply(&other[2]).is_err());
    assert_eq!(journal, before, "foreign source changed the reader");
    journal.apply(&original[2]).map_err(|e| e.to_string())?;
    assert!(journal.recover(&sibling, "lost").is_err());
    assert!(
        model
            .publish(Commit {
                conditions: Vec::new(),
                mutations: vec![append(&model.fence, AppendRecord::from("ended"))],
            })
            .await?
    );
    assert_eq!(model.tail(&live_fence).await?, 0);
    let recovery = journal
        .recover(&model.call, "lost")
        .map_err(|e| e.to_string())?
        .ok_or("missing original recovery")?;
    assert!(model.publish(recovery).await?);
    let mut settled = journal.clone();
    for record in model.records(model.call.log(), settled.next()).await? {
        settled.apply(&record).map_err(|e| e.to_string())?;
    }
    assert!(settled.receipt_ref(model.call.log()).is_some());
    assert!(settled.receipt_ref(sibling.log()).is_none());
    assert_eq!(model.tail(sibling.settled()).await?, 0);

    // Negative control discards the retained source binding. Both acceptance
    // heads still carry the same genuine commit, so the original recovery
    // guards alone would end the sibling whose own session is still live.
    journal.origin = sibling.log().clone();
    let wrong = journal
        .recover(&sibling, "lost")
        .map_err(|e| e.to_string())?
        .ok_or("missing negative control")?;
    assert!(model.publish(wrong).await?);
    assert_eq!(model.tail(sibling.settled()).await?, 1);
    assert_eq!(model.tail(&live_fence).await?, 0);
    Ok(())
}
