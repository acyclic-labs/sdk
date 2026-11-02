use super::*;
use acyclic_actors::call::{Accepted, CallPath, Commit, Journal};
use acyclic_stream::{
    CommitId, CommitOutcome, IdempotencyKey, MemoryStream, OwnedBytes, Stream,
    StreamProviderExt as _,
};
use arrow_array::{Array, StringArray, UInt64Array};
use futures::TryStreamExt as _;
use std::sync::Arc;

async fn publish(provider: &MemoryStream, commit: Commit) -> Result<()> {
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

async fn at(provider: &Arc<MemoryStream>, path: &StreamPath, sequence: u64) -> Result<Record> {
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
async fn original_call_fold_preserves_scope_and_cancellation_disposition() -> Result<()> {
    let scope = StreamPath::new("research/scope").map_err(export_error)?;
    let account = AccountId::new(b"original\0\xff").map_err(export_error)?;
    let source = || Source {
        scope: &scope,
        caller: &scope,
        agent: None,
        account: &account,
    };
    for accepted in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let call = CallPath::new(&scope, "cancel").map_err(export_error)?;
        if accepted {
            publish(
                &provider,
                call.request(OwnedBytes::from("request"))
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
        publish(&provider, call.cancel("stop").map_err(export_error)?).await?;
        let mut journal = Journal::default();
        let last = if accepted { 2 } else { 0 };
        for sequence in 0..=last {
            journal
                .apply(&at(&provider, call.log(), sequence).await?)
                .map_err(export_error)?;
        }
        let record = at(&provider, call.log(), last).await?;
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
        for unknown in ["agent_path", "operation_id", "fence", "artifact"] {
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
        assert!(project(source(), &Journal::default(), &call, call.log(), &record).is_err());
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
async fn original_session_fence_has_no_invented_terminal() -> Result<()> {
    let scope = StreamPath::new("research/scope").map_err(export_error)?;
    let account = AccountId::new(b"original").map_err(export_error)?;
    let source = || Source {
        scope: &scope,
        caller: &scope,
        agent: None,
        account: &account,
    };
    let provider = Arc::new(MemoryStream::default());
    let call = CallPath::new(&scope, "lost").map_err(export_error)?;
    let sibling = CallPath::new(&scope, "also-lost").map_err(export_error)?;
    let fence = StreamPath::new("executor/session/ended").map_err(export_error)?;
    for call in [&call, &sibling] {
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
    }
    let mut journal = Journal::default();
    journal
        .apply(&at(&provider, call.log(), 0).await?)
        .map_err(export_error)?;
    Stream::new(provider.clone(), fence.as_str())
        .map_err(export_error)?
        .append("ended", IdempotencyKey::unique())
        .await
        .map_err(export_error)?;
    let original_fence = at(&provider, &fence, 0).await?;
    journal.fenced("original session ended");
    let row = project(source(), &journal, &call, &fence, &original_fence)?;
    // Existence of the first record closed the session. A later append to
    // that same path is not another observation of the original settlement.
    Stream::new(provider.clone(), fence.as_str())
        .map_err(export_error)?
        .append("later", IdempotencyKey::unique())
        .await
        .map_err(export_error)?;
    let later_fence = at(&provider, &fence, 1).await?;
    assert_ne!(later_fence.commit_id, original_fence.commit_id);
    assert!(project(source(), &journal, &call, &fence, &later_fence).is_err());
    assert_eq!(text(&row, "outcome"), "indeterminate");
    assert_eq!(text(&row, "coverage"), "original_call_fence");
    assert_eq!(text(&row, "fence"), fence.as_str());
    assert_eq!(text(&row, "source_stream"), fence.as_str());
    for unknown in [
        "terminal_sequence",
        "request_sequence",
        "request_commit_id",
        "agent_path",
    ] {
        assert!(row.data.column_by_name(unknown).unwrap().is_null(0));
    }
    assert!(project(source(), &journal, &call, call.log(), &original_fence).is_err());
    assert!(
        project(
            source(),
            &Journal::default(),
            &call,
            &fence,
            &original_fence
        )
        .is_err()
    );
    #[cfg(feature = "provenance-export")]
    {
        let mut sibling_journal = Journal::default();
        sibling_journal
            .apply(&at(&provider, sibling.log(), 0).await?)
            .map_err(export_error)?;
        sibling_journal.fenced("original session ended");
        let sibling_row = project(
            source(),
            &sibling_journal,
            &sibling,
            &fence,
            &original_fence,
        )?;
        assert_eq!(
            text(&row, "source_commit_id"),
            text(&sibling_row, "source_commit_id")
        );
        let subject = |batch: &Batch| -> Result<String> {
            let mut encoded = Vec::new();
            super::super::provenance::write(batch, &mut encoded)?;
            let triples = oxttl::NTriplesParser::new()
                .for_reader(encoded.as_slice())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(export_error)?;
            Ok(triples
                .into_iter()
                .find(|triple| triple.predicate.as_str() == "urn:acyclic:research:callLog")
                .expect("original call observation")
                .subject
                .to_string())
        };
        assert_ne!(
            subject(&row)?,
            subject(&sibling_row)?,
            "one original session fence must not merge distinct call observations"
        );
    }
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
            let mut journal = Journal::default();
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
