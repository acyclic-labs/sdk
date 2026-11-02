use super::*;
use acyclic_actors::call::{Accepted, CallPath, Commit, Journal};
use acyclic_stream::{
    CommitId, CommitOutcome, IdempotencyKey, MemoryStream, OwnedBytes, Stream, StreamProvider,
    StreamProviderExt as _,
};
use arrow_array::{Array, StringArray, UInt64Array};
use futures::TryStreamExt as _;
use std::sync::Arc;

async fn publish<P: StreamProvider + ?Sized>(provider: &Arc<P>, commit: Commit) -> Result<()> {
    match provider
        .commit(commit.request(IdempotencyKey::unique()))
        .await
        .map_err(export_error)?
    {
        CommitOutcome::Committed(_) => Ok(()),
        CommitOutcome::Conflict(_) => {
            Err(export_error("original call control publication conflicted"))
        }
    }
}

async fn at<P: StreamProvider + ?Sized>(
    provider: &Arc<P>,
    path: &StreamPath,
    sequence: u64,
) -> Result<Record> {
    Stream::new(provider.clone(), path.as_str())
        .map_err(export_error)?
        .read(sequence, 1)
        .await
        .map_err(export_error)?
        .try_next()
        .await
        .map_err(export_error)?
        .ok_or_else(|| export_error("original call control record is absent"))
}

fn text<'a>(batch: &'a Batch, name: &str) -> &'a str {
    batch
        .data
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
}

fn number(batch: &Batch, name: &str) -> u64 {
    batch
        .data
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn original_call_fold_preserves_scope_and_cancellation_disposition() -> Result<()> {
    let scope = StreamPath::new("research/scope").map_err(export_error)?;
    let account = AccountId::new(b"original\0\xff").map_err(export_error)?;
    let source = || Source {
        scope: &scope,
        caller: &scope,
        agent: None,
        account: &account,
    };
    // Different terminals in one real commit share their sequence and commit
    // identity. The selected path cannot stand in for the record's source.
    let directory = tempfile::tempdir().map_err(export_error)?;
    let provider = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(export_error)?,
    );
    let call = CallPath::new(&scope, "first").map_err(export_error)?;
    let sibling = CallPath::new(&scope, "second").map_err(export_error)?;
    let mut joined = call.cancel("first").map_err(export_error)?;
    let other = sibling
        .cancel("longer sibling reason")
        .map_err(export_error)?;
    joined.conditions.extend(other.conditions);
    joined.mutations.extend(other.mutations);
    publish(&provider, joined).await?;
    drop(provider);
    // Reconstruct the receipt from the persisted source, rather than keeping
    // a live reader or an in-memory commit result across the reopen.
    let provider = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(export_error)?,
    );
    let original = at(&provider, call.log(), 0).await?;
    let mut foreign = at(&provider, sibling.log(), 0).await?;
    assert_eq!(original.commit_id, foreign.commit_id);
    assert_eq!(original.sequence, foreign.sequence);
    assert_ne!(original.origin, foreign.origin);
    let mut journal = Journal::new(call.log());
    journal.apply(&original).map_err(export_error)?;
    let row = project(source(), &journal, &call, call.log(), &original)?;
    assert_eq!(text(&row, "source_origin"), original.origin.as_str());
    assert_eq!(
        number(&row, "logical_value_bytes"),
        original.value.len() as u64
    );
    assert!(project(source(), &journal, &call, call.log(), &foreign).is_err());
    // Negative control loses the original source evidence. The remaining
    // position/commit checks then misattribute the sibling's actual bytes.
    // Projection cannot authenticate a caller that fabricates this evidence.
    foreign.origin = original.origin.clone();
    let mislabeled = project(source(), &journal, &call, call.log(), &foreign)?;
    assert_eq!(
        number(&mislabeled, "logical_value_bytes"),
        foreign.value.len() as u64
    );
    assert_ne!(original.value.len(), foreign.value.len());
    for accepted in [false, true] {
        let directory = tempfile::tempdir().map_err(export_error)?;
        let provider = Arc::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default())
                .await
                .map_err(export_error)?,
        );
        let call = CallPath::new(&scope, "cancel").map_err(export_error)?;
        if accepted {
            publish(
                &provider,
                call.request(
                    OwnedBytes::from("request"),
                    acyclic_actors::escrow::Escrow::new(Some(1)),
                )
                .map_err(export_error)?,
            )
            .await?;
            publish(
                &provider,
                call.accept(
                    &Accepted {
                        executor: OwnedBytes::from("executor"),
                        fence: None,
                    },
                    true,
                )
                .map_err(export_error)?,
            )
            .await?;
        }
        let terminal = call
            .cancel("stop")
            .map_err(export_error)?
            .request(IdempotencyKey::unique());
        let committed = provider
            .commit(terminal.clone())
            .await
            .map_err(export_error)?;
        let CommitOutcome::Committed(original_receipt) = &committed else {
            return Err(export_error("original cancellation conflicted"));
        };
        drop(provider);
        let provider = Arc::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default())
                .await
                .map_err(export_error)?,
        );
        assert_eq!(
            provider
                .commit(terminal.clone())
                .await
                .map_err(export_error)?,
            committed,
            "reopen must retain the original terminal receipt"
        );
        // A new identity cannot recover that receipt or append a second end.
        let mut rival = terminal;
        rival.idempotency_key = IdempotencyKey::unique();
        assert!(matches!(
            provider.commit(rival).await.map_err(export_error)?,
            CommitOutcome::Conflict(_)
        ));
        let mut journal = Journal::new(call.log());
        let last = if accepted { 3 } else { 0 };
        assert_eq!(
            provider
                .head(call.log().clone())
                .await
                .map_err(export_error)?
                .tail,
            last + 1
        );
        assert_eq!(
            provider
                .head(call.settled().clone())
                .await
                .map_err(export_error)?
                .tail,
            1
        );
        for sequence in 0..=last {
            journal
                .apply(&at(&provider, call.log(), sequence).await?)
                .map_err(export_error)?;
        }
        let record = at(&provider, call.log(), last).await?;
        assert_eq!(record.commit_id, original_receipt.commit_id);
        let row = project(source(), &journal, &call, call.log(), &record)?;
        assert_eq!(row.table, Table::Outcomes);
        assert_eq!(
            text(&row, "outcome"),
            if accepted {
                "indeterminate"
            } else {
                "cancelled"
            }
        );
        assert_eq!(text(&row, "scope_path"), scope.as_str());
        assert_eq!(text(&row, "call_log"), call.log().as_str());
        assert_eq!(text(&row, "coverage"), "original_call_terminal");
        assert_eq!(number(&row, "terminal_sequence"), last);
        assert_eq!(
            text(&row, "source_commit_id"),
            digest(*record.commit_id.as_bytes())
        );
        assert_eq!(text(&row, "source_origin"), record.origin.as_str());
        for unknown in ["agent_path", "operation_id", "artifact"] {
            assert!(row.data.column_by_name(unknown).unwrap().is_null(0));
        }
        assert_eq!(
            row.data
                .column_by_name("request_sequence")
                .unwrap()
                .is_null(0),
            !accepted
        );
        // Reading the raw terminal alone loses the accepted cancellation fence.
        if accepted {
            assert!(matches!(
                acyclic_actors::call::Entry::decode(&record).map_err(export_error)?,
                acyclic_actors::call::Entry::Terminal(Outcome::Cancelled(_))
            ));
            assert_ne!(text(&row, "outcome"), "cancelled");
        }
        let mut foreign = record.clone();
        foreign.commit_id = CommitId::from_bytes([99; 32]);
        assert!(project(source(), &journal, &call, call.log(), &foreign).is_err());
        foreign = record.clone();
        foreign.sequence += 1;
        assert!(project(source(), &journal, &call, call.log(), &foreign).is_err());
        let other = StreamPath::new("foreign/calls/cancel").map_err(export_error)?;
        assert!(project(source(), &journal, &call, &other, &record).is_err());
        let foreign_agent = AgentPath::root(&crate::agent::AgentId::from_bytes([9; 16]));
        assert!(
            project(
                Source {
                    agent: Some(&foreign_agent),
                    ..source()
                },
                &journal,
                &call,
                call.log(),
                &record
            )
            .is_err()
        );
        assert!(
            project(
                Source {
                    caller: &other,
                    ..source()
                },
                &journal,
                &call,
                call.log(),
                &record
            )
            .is_err()
        );
        assert!(
            project(
                source(),
                &Journal::new(call.log()),
                &call,
                call.log(),
                &record
            )
            .is_err()
        );
        #[cfg(feature = "provenance-export")]
        {
            let mut encoded = Vec::new();
            super::super::provenance::write(&row, &mut encoded)?;
            let triples = oxttl::NTriplesParser::new()
                .for_reader(encoded.as_slice())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(export_error)?;
            assert!(triples.iter().any(|triple| triple.predicate.as_str() == "urn:acyclic:research:scopePath"
                && matches!(&triple.object, oxrdf::Term::Literal(value) if value.value() == scope.as_str())));
            assert!(!triples.iter().any(|triple| triple.predicate.as_str() == "http://www.w3.org/ns/prov#wasAssociatedWith"
                || matches!(&triple.object, oxrdf::Term::NamedNode(value) if value.as_str() == "http://www.w3.org/ns/prov#SoftwareAgent")));
        }
    }
    Ok(())
}

#[tokio::test]
async fn original_session_loss_requires_the_call_terminal() -> Result<()> {
    let scope = StreamPath::new("research/scope").map_err(export_error)?;
    let account = AccountId::new(b"original").map_err(export_error)?;
    let source = || Source {
        scope: &scope,
        caller: &scope,
        agent: None,
        account: &account,
    };
    let provider = Arc::new(MemoryStream::default());
    let fence = StreamPath::new("executor/session/ended").map_err(export_error)?;
    let mut observations = Vec::new();
    for id in ["lost", "also-lost"] {
        let call = CallPath::new(&scope, id).map_err(export_error)?;
        publish(
            &provider,
            call.accept(
                &Accepted {
                    executor: OwnedBytes::from("executor"),
                    fence: Some(fence.clone()),
                },
                false,
            )
            .map_err(export_error)?,
        )
        .await?;
        let mut journal = Journal::new(call.log());
        journal
            .apply(&at(&provider, call.log(), 0).await?)
            .map_err(export_error)?;
        observations.push((call, journal));
    }
    Stream::new(provider.clone(), fence.as_str())
        .map_err(export_error)?
        .append("ended", IdempotencyKey::unique())
        .await
        .map_err(export_error)?;
    let original_fence = at(&provider, &fence, 0).await?;
    let mut commits = Vec::new();
    for (call, mut journal) in observations {
        assert!(journal.receipt_ref(call.log()).is_none());
        assert!(project(source(), &journal, &call, &fence, &original_fence).is_err());
        publish(
            &provider,
            journal
                .recover(&call, "original session ended")
                .map_err(export_error)?
                .ok_or_else(|| export_error("missing original recovery"))?,
        )
        .await?;
        let terminal = at(&provider, call.log(), 1).await?;
        journal.apply(&terminal).map_err(export_error)?;
        let row = project(source(), &journal, &call, call.log(), &terminal)?;
        assert_eq!(text(&row, "outcome"), "indeterminate");
        assert_eq!(text(&row, "coverage"), "original_call_terminal");
        assert_eq!(text(&row, "source_stream"), call.log().as_str());
        assert_eq!(number(&row, "terminal_sequence"), 1);
        for unknown in ["request_sequence", "request_commit_id", "agent_path"] {
            assert!(row.data.column_by_name(unknown).unwrap().is_null(0));
        }
        assert!(project(source(), &journal, &call, &fence, &original_fence).is_err());
        assert!(
            journal
                .recover(&call, "lost again")
                .map_err(export_error)?
                .is_none()
        );
        commits.push(terminal.commit_id);
    }
    assert_ne!(
        commits[0], commits[1],
        "one session fence cannot replace distinct call terminals"
    );
    Ok(())
}
#[tokio::test]
async fn receipt_projection_neither_copies_nor_scans_opaque_results() -> Result<()> {
    let scope = StreamPath::new("research/scope").map_err(export_error)?;
    let agent = AgentPath::root(&crate::agent::AgentId::from_bytes([1; 16]));
    let caller = StreamPath::new(agent.as_str()).map_err(export_error)?;
    let account = AccountId::new(b"original").map_err(export_error)?;
    for failure in [false, true] {
        let mut cost = None;
        for length in [0, 65537] {
            let provider = Arc::new(MemoryStream::default());
            let call = CallPath::new(&caller, "result").map_err(export_error)?;
            publish(
                &provider,
                call.accept(
                    &Accepted {
                        executor: OwnedBytes::from("executor"),
                        fence: None,
                    },
                    false,
                )
                .map_err(export_error)?,
            )
            .await?;
            let outcome = if failure {
                Outcome::Error("x".repeat(length))
            } else {
                Outcome::Ok(vec![255; length].into())
            };
            publish(
                &provider,
                call.settle(None, &outcome).map_err(export_error)?,
            )
            .await?;
            let mut journal = Journal::new(call.log());
            for sequence in 0..2 {
                journal
                    .apply(&at(&provider, call.log(), sequence).await?)
                    .map_err(export_error)?;
            }
            let record = at(&provider, call.log(), 1).await?;
            let source = || Source {
                scope: &scope,
                caller: &caller,
                agent: Some(&agent),
                account: &account,
            };
            let row = project(source(), &journal, &call, call.log(), &record)?;
            assert_eq!(text(&row, "agent_path"), agent.as_str());
            assert_eq!(text(&row, "outcome"), if failure { "error" } else { "ok" });
            assert_eq!(number(&row, "logical_value_bytes"), length as u64);
            let measured = acyclic_test_support::measure(|| {
                std::hint::black_box(
                    project(source(), &journal, &call, call.log(), &record).unwrap(),
                );
            });
            let current = (measured.count_total, measured.bytes_total);
            if let Some(previous) = cost {
                assert_eq!(current, previous);
            }
            cost = Some(current);
            let borrowed = acyclic_test_support::measure(|| {
                std::hint::black_box(journal.receipt_ref(call.log()).unwrap());
            });
            assert_eq!(borrowed.count_total, 0);
            if length > 0 {
                let copied = acyclic_test_support::measure(|| {
                    std::hint::black_box(record.value.to_vec());
                });
                assert!(copied.bytes_total >= length as u64);
                if failure {
                    // The owned boundary deliberately copies failure text; pure
                    // projection must retain the original borrowed view instead.
                    let owned = acyclic_test_support::measure(|| {
                        std::hint::black_box(journal.receipt(call.log()).unwrap());
                    });
                    assert!(owned.bytes_total >= length as u64);
                }
            }
        }
    }
    Ok(())
}
