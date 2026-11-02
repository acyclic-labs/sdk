use super::*;
use crate::reader::{Failure, Op, Outcome, Priority, ReaderPolicy};
use acyclic_harness_core::{
    OperationId,
    agent::{AgentRecord as _, Phase, StatusRecord},
    executor::{ModelPriceRecord, ModelPurpose, ModelTokenUsage, ModelUsageRecord, TokenPrice},
    live::{
        AgentState, Budget, ContextAdmission, ContextInfo, ContextPartInfo, ContextPartKind,
        Counters, Error as ProjectionError, StateCounts, Status, StatusBudget, Usage,
    },
    model::ModelDispatch,
};
use acyclic_stream::{
    AppendRequest, ForkRequest, IdempotencyKey, MemoryStream, StreamProvider as _,
    StreamProviderExt as _,
};
use futures::StreamExt as _;
use std::borrow::Cow;

// Generated arithmetic operands, not captured or fabricated provider output.
fn call() -> (ContextAdmission, ModelUsageRecord, ModelPriceRecord) {
    let dispatch = ModelDispatch {
        operation_id: OperationId::from_bytes([31; 16]),
        step: 0,
        request_digest: [7; 32],
    };
    let admission = ContextAdmission {
        dispatch,
        revision: 4,
        capacity: 1000,
        notify_at: Some(0.75),
        auto_trim_at_tokens: 900,
        total_tokens: 100,
        parts: "live/agents/self/context/parts".into(),
    };
    let usage = ModelUsageRecord {
        version: ModelUsageRecord::VERSION,
        completed: true,
        dispatch,
        purpose: ModelPurpose::Response,
        provider: "generated-provider".into(),
        model: "generated-model".into(),
        agent_path: None,
        sampling: None,
        max_output_tokens: None,
        tokens: ModelTokenUsage {
            input: Some(100),
            cached: Some(30),
            cache_write: Some(10),
            output: Some(5),
            reasoning: None,
        },
        latency_ns: None,
        provider_request_id: None,
        system_fingerprint: None,
        ttft_ns: None,
        cache: None,
    };
    let price = ModelPriceRecord {
        version: 1,
        revision: "generated-price".into(),
        provider: usage.provider.clone(),
        model: usage.model.clone(),
        effective_at: 1,
        currency: "USD".into(),
        input_nano_units_per_million: Some(10_000_000_000),
        output_nano_units_per_million: Some(20_000_000_000),
        cached: TokenPrice::Subset {
            nano_units_per_million: Some(1_000_000_000),
        },
        cache_write: TokenPrice::Subset {
            nano_units_per_million: Some(30_000_000_000),
        },
        reasoning: TokenPrice::Included,
    };
    (admission, usage, price)
}

// These are the file values consumed by hooks and typed readers, separate from
// the original records that supply them. Compare JSON values, not rendered bytes.
fn public_file_contract(usage: &Usage<'_>, context: &ContextInfo<'_>) {
    assert_eq!(
        serde_json::to_value(usage).unwrap(),
        serde_json::json!({
            "revision": 4, "model": "generated-model", "inputTokens": 100,
            "cachedTokens": 30, "outputTokens": 5, "capacity": 1000,
            "cost": 0.00103,
            "compaction": {
                "summariseNow": null, "perCallIfNot": null, "autoTrimAtTokens": 900
            }
        })
    );
    assert_eq!(
        serde_json::to_value(context).unwrap(),
        serde_json::json!({
            "revision": 5, "capacity": 1000, "notifyAt": null, "autoTrimAtTokens": 900,
            "current": {"request": "08".repeat(32), "totalTokens": 200},
            "lastCall": {
                "request": "07".repeat(32), "totalTokens": 100,
                "cachedReadTokens": 30, "cacheWriteTokens": 10
            },
            "parts": "live/agents/self/context/parts"
        })
    );
    let mut unreported = context.clone();
    unreported.last_call.cached_read_tokens = None;
    unreported.last_call.cache_write_tokens = None;
    assert_eq!(
        serde_json::to_value(unreported).unwrap()["lastCall"],
        serde_json::json!({
            "request": "07".repeat(32), "totalTokens": 100,
            "cachedReadTokens": null, "cacheWriteTokens": null
        })
    );
    let children = StateCounts {
        working: 1,
        waiting: 2,
        blocked: 0,
        paused: 0,
        idle: 0,
        done: 3,
        failed: 0,
        archived: 0,
    };
    let states = serde_json::json!({
        "working": 1, "waiting": 2, "blocked": 0, "paused": 0,
        "idle": 0, "done": 3, "failed": 0, "archived": 0
    });
    assert_eq!(
        serde_json::to_value(Status {
            state: AgentState::Working,
            summary: "generated summary".into(),
            children: children.clone(),
            budget: StatusBudget {
                spent: 1.25,
                limit: None
            },
            updated: "2026-11-02T00:00:00Z".into(),
        })
        .unwrap(),
        serde_json::json!({
            "state": "working", "summary": "generated summary", "children": states,
            "budget": {"spent": 1.25, "limit": null}, "updated": "2026-11-02T00:00:00Z"
        })
    );
    assert_eq!(
        serde_json::to_value(Counters {
            state: children,
            kind: [("worker".into(), 6)].into(),
        })
        .unwrap(),
        serde_json::json!({"state": states, "kind": {"worker": 6}})
    );
    assert_eq!(
        serde_json::to_value(Budget {
            escrow: None,
            spent: 1.25,
            reserved: 0.5
        })
        .unwrap(),
        serde_json::json!({"escrow": null, "spent": 1.25, "reserved": 0.5})
    );
    let mut part = ContextPartInfo {
        id: "c4".into(),
        kind: ContextPartKind::Pinned,
        tokens: 9,
        covers: None,
    };
    assert_eq!(
        serde_json::to_value(&part).unwrap(),
        serde_json::json!({"id": "c4", "kind": "pinned", "tokens": 9, "covers": null})
    );
    part.kind = ContextPartKind::Summary;
    part.covers = Some(["c0".into(), "c3".into()]);
    assert_eq!(
        serde_json::to_value(part).unwrap(),
        serde_json::json!({"id": "c4", "kind": "summary", "tokens": 9, "covers": ["c0", "c3"]})
    );
}

#[test]
fn live_files_keep_original_request_counts_and_bounded_committed_windows() {
    futures::executor::block_on(async {
        let (admission, mut call, mut price) = call();
        let projected = Usage::project(&admission, &call, Some(&price)).unwrap();
        assert!(matches!(&projected.model, Cow::Borrowed(_)));
        assert_eq!(projected.model.as_ptr(), call.model.as_ptr());
        // A decoded file owns its text independently of the producer. The
        // serialization view must not pre-copy that same original model name.
        let first: Usage<'static> =
            serde_json::from_value(serde_json::to_value(&projected).unwrap()).unwrap();
        assert_eq!(first, projected);
        assert!(matches!(&first.model, Cow::Owned(_)));
        assert_eq!(first.input_tokens, 100);
        assert!((first.cost - 0.00103).abs() < f64::EPSILON);
        assert_eq!(first.compaction.summarise_now, None);
        assert_eq!(first.compaction.per_call_if_not, None);

        // The same units under an exclusive reporting convention must include
        // the cache exactly once, while retaining the same billing estimate.
        price.cached = TokenPrice::Additional {
            nano_units_per_million: Some(1_000_000_000),
        };
        price.cache_write = TokenPrice::Additional {
            nano_units_per_million: Some(30_000_000_000),
        };
        call.tokens.input = Some(60);
        let exclusive = Usage::project(&admission, &call, Some(&price)).unwrap();
        assert_eq!(exclusive, first);
        call.tokens.cached = None;
        assert_eq!(
            Usage::project(&admission, &call, Some(&price)),
            Err(ProjectionError::IncompleteCounters)
        );
        call.tokens.cached = Some(30);
        assert_eq!(
            Usage::project(&admission, &call, None),
            Err(ProjectionError::IncompleteCost)
        );
        price.currency = "EUR".into();
        assert_eq!(
            Usage::project(&admission, &call, Some(&price)),
            Err(ProjectionError::IncompleteCost)
        );
        price.currency = "USD".into();
        call.completed = false;
        assert_eq!(
            Usage::project(&admission, &call, Some(&price)),
            Err(ProjectionError::IncompleteCall)
        );
        call.completed = true;
        call.dispatch.step += 1;
        assert_eq!(
            Usage::project(&admission, &call, Some(&price)),
            Err(ProjectionError::CallMismatch)
        );
        call.dispatch = admission.dispatch;
        call.tokens.input = Some(u64::MAX);
        assert_eq!(
            Usage::project(&admission, &call, Some(&price)),
            Err(ProjectionError::IncompleteCounters)
        );
        call.tokens.input = Some(60);

        let mut current = admission.clone();
        current.revision += 1;
        current.dispatch.request_digest = [8; 32];
        current.total_tokens = 200;
        current.notify_at = None;
        let context = ContextInfo::project(&current, &admission, &call).unwrap();
        assert!(matches!(&context.parts, Cow::Borrowed(_)));
        assert_eq!(context.parts.as_ptr(), current.parts.as_ptr());
        let decoded: ContextInfo<'static> =
            serde_json::from_value(serde_json::to_value(&context).unwrap()).unwrap();
        assert_eq!(decoded, context);
        assert!(matches!(&decoded.parts, Cow::Owned(_)));
        assert_eq!(context.current.total_tokens, 200);
        assert_eq!(context.last_call.total_tokens, 100);
        assert_ne!(context.current.request, context.last_call.request);
        assert_eq!(context.last_call.cached_read_tokens, Some(30));
        assert_eq!(context.revision, 5);
        public_file_contract(&first, &context);

        let provider = MemoryStream::default();
        let stream = StreamPath::new("original/usage").unwrap();
        let source = Source::new(stream.clone());
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        assert_eq!(source.latest(&reader), Err(Error::Pending));
        // Keep the existing usage format. These generated counters exercise
        // projection arithmetic, not an actual provider completion.
        let bytes = serde_json::to_vec(&call).unwrap();
        let records = vec![bytes.clone().into(), bytes.into()];
        provider
            .append(AppendRequest {
                path: stream.clone(),
                records,
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(b"original-usage".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        reader.plan([(source.want(), Priority::Visible)]);
        let [head]: [Op; 1] = reader.poll(0).try_into().unwrap();
        let Op::Head { id, stream: path } = head else {
            panic!("snapshot must request its original head")
        };
        reader.complete(id, Outcome::Head(provider.head(path).await.unwrap()), 0);
        let latest = source.latest(&reader).unwrap();
        assert!(std::ptr::eq(
            latest,
            reader.cache().record(&stream, 1).unwrap()
        ));
        let original: ModelUsageRecord = serde_json::from_slice(latest).unwrap();
        assert_eq!(
            Usage::project(&admission, &original, Some(&price)).unwrap(),
            first
        );
        let issued = reader.issued();
        let count = NonZeroU32::new(1).unwrap();
        let mut page = source.page(&reader, 0, count).unwrap();
        assert_eq!(page.end, 1);
        assert_eq!(page.next(), Some((0, Err(Error::Pending))));
        assert!(page.next().is_none());
        assert_eq!(reader.issued(), issued, "a projection cannot dispatch IO");
        drop(page);

        reader.plan([(source.want_page(0, count).unwrap(), Priority::Visible)]);
        let [read]: [Op; 1] = reader.poll(0).try_into().unwrap();
        let id = read.id();
        reader.complete(id, super::super::driver::execute(&provider, read).await, 0);
        let mut page = source.page(&reader, 0, count).unwrap();
        let (sequence, bytes) = page.next().unwrap();
        assert_eq!(sequence, 0);
        let original: ModelUsageRecord = serde_json::from_slice(bytes.unwrap()).unwrap();
        assert_eq!(
            Usage::project(&admission, &original, Some(&price)).unwrap(),
            first
        );
        assert!(page.next().is_none());
        drop(page);
        assert!(source.page(&reader, 2, count).unwrap().next().is_none());
        assert!(matches!(source.page(&reader, 3, count), Err(Error::Range)));
        assert_eq!(source.want_page(u64::MAX, count), Err(Error::Range));

        // An ordinary listing supplies fork origin, but no child tail. Its
        // finite nonempty read must make the requested page immediately usable
        // without a head, another request, or fabricated end-of-stream proof.
        let child = StreamPath::new("original/forked-usage").unwrap();
        provider
            .fork(ForkRequest {
                exclude: Vec::new(),
                source: stream.clone(),
                destination: child.clone(),
                at_tail: Some(2),
                idempotency_key: IdempotencyKey::new(b"inherited-usage".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        let selected = Source::new(child.clone());
        let mut listed = Reader::new(ReaderPolicy::default()).unwrap();
        listed.plan([(
            Want::Listing {
                parent: StreamPath::new("original").unwrap(),
                index: 0,
                tail: None,
                witness: None,
            },
            Priority::Visible,
        )]);
        let [listing]: [Op; 1] = listed.poll(0).try_into().unwrap();
        assert!(matches!(&listing, Op::Children { .. }));
        let id = listing.id();
        listed.complete(
            id,
            super::super::driver::execute(&provider, listing).await,
            0,
        );
        assert!(listed.cache().origin_known(&child));
        assert_eq!(listed.cache().tail(&child), None);
        let window = NonZeroU32::new(2).unwrap();
        let want = selected.want_page(0, window).unwrap();
        assert_eq!(
            selected.page(&listed, 0, window).unwrap().next(),
            Some((0, Err(Error::Pending)))
        );
        listed.plan([(want.clone(), Priority::Visible)]);
        let [read]: [Op; 1] = listed.poll(0).try_into().unwrap();
        assert!(matches!(&read, Op::Read { .. }));
        let id = read.id();
        let loaded = super::super::driver::execute(&provider, read).await;
        let Outcome::Records(records) = &loaded else {
            panic!("original finite read");
        };
        let pointer = records[0].value.as_slice().as_ptr();
        listed.complete(id, loaded, 0);
        assert_eq!(listed.cache().tail(&child), None);
        let issued = listed.issued();
        let mut page = selected.page(&listed, 0, window).unwrap();
        assert_eq!(page.end, 2);
        for sequence in 0..2 {
            let (position, row) = page.next().unwrap();
            let row = row.unwrap();
            assert_eq!(position, sequence);
            let original: ModelUsageRecord = serde_json::from_slice(row).unwrap();
            assert_eq!(
                Usage::project(&admission, &original, Some(&price)).unwrap(),
                first
            );
            if sequence == 0 {
                assert_eq!(row.as_ptr(), pointer);
            }
        }
        assert!(page.next().is_none());
        drop(page);
        listed.plan([(want.clone(), Priority::Visible)]);
        assert!(listed.poll(0).is_empty());
        assert_eq!(listed.issued(), issued);

        // An original stream follow publishes a new source record. Its
        // unsupported layout must fail, rather than serve the previous value.
        reader.set_follows([stream.clone()]);
        let mut follow = acyclic_stream::records_only(
            provider
                .follow(acyclic_stream::FollowRequest::new(stream.clone(), 2))
                .await
                .unwrap(),
        );
        call.version = ModelUsageRecord::VERSION + 1;
        provider
            .append(AppendRequest {
                path: stream.clone(),
                records: vec![serde_json::to_vec(&call).unwrap().into()],
                if_tail: Some(2),
                idempotency_key: IdempotencyKey::new(b"unsupported-layout".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        let record = follow.next().await.unwrap().unwrap();
        reader.deliver(&stream, super::super::driver::fetched(record), 3);
        let original: ModelUsageRecord =
            serde_json::from_slice(source.latest(&reader).unwrap()).unwrap();
        assert_eq!(
            Usage::project(&admission, &original, Some(&price)),
            Err(ProjectionError::Version)
        );

        // The same reader also borrows the actual agent status encoding.
        // A live view must not require the phase owner to publish another row.
        let status = StatusRecord {
            phase: Phase::Idle,
            cursor: 0,
            conversation: 0,
        };
        let status_stream = StreamPath::new("original/status").unwrap();
        provider
            .append(AppendRequest {
                path: status_stream.clone(),
                records: vec![status.encode().unwrap().into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(b"original-status".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        let status_source = Source::new(status_stream);
        reader.plan([(status_source.want(), Priority::Visible)]);
        let [head]: [Op; 1] = reader.poll(0).try_into().unwrap();
        let id = head.id();
        reader.complete(id, super::super::driver::execute(&provider, head).await, 0);
        assert_eq!(
            StatusRecord::decode(status_source.latest(&reader).unwrap()).unwrap(),
            status
        );

        // A refusal of the same original head must not leave stale values
        // readable. It is not an empty successful log or a forged zero.
        reader.cache_mut().forget(&stream);
        reader.plan([(source.want(), Priority::Visible)]);
        let [head]: [Op; 1] = reader.poll(0).try_into().unwrap();
        reader.complete(head.id(), Outcome::Failed(Failure::Denied), 0);
        assert_eq!(
            source.latest(&reader),
            Err(Error::Source(super::super::ReadIssue::AccessDenied))
        );
    });
}
