use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use acyclic_native_runtime::NativeReadMeter;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitOutcome,
    ForkReceipt, ForkRequest, IdempotencyObservation, NativeProviderFuture, NativeRecordStream,
    ReadRequest, RecordStream, StreamBounds, StreamError,
};
use std::sync::Arc;

struct NativeOnly<P>(Arc<P>);

#[async_trait::async_trait]
impl<P: StreamProvider> StreamProvider for NativeOnly<P> {
    async fn inspect_idempotency(
        &self,
        _: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        panic!("ordinary inspection on a native-only provider")
    }
    async fn tail(&self, _: StreamPath) -> std::result::Result<u64, StreamError> {
        panic!("ordinary tail on a native-only provider")
    }
    async fn bounds(&self, _: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        panic!("ordinary bounds on a native-only provider")
    }
    async fn append(&self, _: AppendRequest) -> std::result::Result<AppendOutcome, StreamError> {
        panic!("ordinary append on a native-only provider")
    }
    async fn fork(&self, _: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
        panic!("ordinary fork on a native-only provider")
    }
    async fn read(&self, _: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        panic!("ordinary read on a native-only provider")
    }
    async fn follow(
        &self,
        _: StreamPath,
        _: u64,
    ) -> std::result::Result<RecordStream, StreamError> {
        panic!("ordinary follow on a native-only provider")
    }
    async fn children(&self, _: ChildrenRequest) -> std::result::Result<ChildStream, StreamError> {
        panic!("ordinary children on a native-only provider")
    }
    async fn commit(&self, _: CommitRequest) -> std::result::Result<CommitOutcome, StreamError> {
        panic!("ordinary commit on a native-only provider")
    }
    async fn read_commit(
        &self,
        _: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        panic!("ordinary envelope read on a native-only provider")
    }
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        self.0.native_tail(path, meter)
    }
    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, StreamBounds> {
        self.0.native_bounds(path, meter)
    }
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        self.0.native_read(request, meter)
    }
    fn native_commit(
        &self,
        request: CommitRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.0.native_commit(request, meter)
    }
}

fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 32,
        authority_bytes_read: 1_048_576,
        backend_read_operations: 32,
        source_bytes_read: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        authority_records_appended: 32,
        authority_bytes_written: 1_048_576,
        backend_write_operations: 32,
        items_examined: 1_048_576,
        allocation_operations: 1_048_576,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

async fn exercise<P: StreamProvider>(provider: Arc<P>) -> Result<()> {
    use super::super::history::{HistoryIo, HistoryReadLimits, HistoryReader};
    use crate::core::{
        Action, AggregateKind, ApplyResult, AuthorityIssuer, Command, LifecycleState, Reducer,
        SchemaRegistry,
    };
    let ordinary = provider.clone();
    let authority = Authority {
        kind: AggregateKind::Agent,
        id: "native-only-reader".into(),
    };
    let issuer = AuthorityIssuer::new("native-only", [29; 32], authority.clone());
    let reducer = Reducer::new(
        authority.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    );
    let command = Command {
        operation_id: OperationId::from_bytes([31; 16]),
        idempotency_key: crate::IdempotencyKey("native-only-reader".into()),
        expected_revision: 0,
        scope: issuer
            .issue(
                "root",
                &crate::Capabilities::new(["lifecycle:manage"]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::TransitionLifecycle {
            to: LifecycleState::Active,
            reason: None,
        },
    };
    let ApplyResult::Applied { event } = reducer.plan(&command)? else {
        panic!("fresh lifecycle event")
    };
    let encoded = crate::wire_codec::encode_event(&authority, &event, &mut None)?;
    let event_bytes = encoded.len() as u64;
    let publication = IndexedPublication::new(&authority, &event, encoded, &mut None)?;
    let request = publication
        .request(
            &ordinary,
            IdempotencyKey::new(OwnedBytes::from_static(b"native-only-reader"))?,
            &mut None,
        )
        .await?
        .0;
    assert!(matches!(
        ordinary.commit(request).await?,
        CommitOutcome::Committed(_)
    ));
    let corrupt_message = uuid::Uuid::from_bytes([37; 16]);
    let corrupt_path = publication_path(
        &authority,
        "harness/v2/conversation-messages/",
        Some((&corrupt_message, 36)),
        &mut None,
    )?
    .0;
    assert!(matches!(
        ordinary
            .commit(CommitRequest {
                conditions: vec![CommitCondition::Absent {
                    path: corrupt_path.clone()
                }],
                mutations: vec![CommitMutation::Append {
                    path: corrupt_path,
                    records: vec![OwnedBytes::from_static(b"{{")],
                }],
                idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(
                    b"native-only-corrupt"
                ))?,
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    let client = Arc::new(NativeOnly(provider));
    let reader = HistoryReader::new(
        &client,
        &authority,
        &(issuer.verifier(&mut None).expect("verifier copy")),
        &mut None,
    )?;
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("finite original reader work");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original reader observer"),
    );
    let cursor = reader.pin(0, Some(&mut io)).await?;
    assert_eq!(cursor.through_revision, 1);
    let page = reader
        .read_page_with_work(
            &cursor,
            HistoryReadLimits {
                maximum_events: 1,
                maximum_bytes: event_bytes,
            },
            true,
            Some(&mut io),
        )
        .await?;
    assert_eq!(page.0.events.as_slice(), std::slice::from_ref(&event));
    assert_eq!(page.1.len(), 1);
    assert_eq!(native.receipt().authority_bytes_read, event_bytes);
    assert_eq!(native.receipt().source_bytes_read, event_bytes);
    let mut work: Option<&mut dyn HistoryIo> = Some(&mut io);
    let bounds = bounds_with_work(&client, &publication.path, &mut work)
        .await?
        .map_err(|error| crate::store::operations::stream_error_with_work(error, &work))?;
    assert_eq!(bounds.tail, 1);
    drop(page);
    drop(cursor);
    drop(work);
    drop(io.into_custody()?);

    let native = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original malformed locator work");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original malformed locator observer"),
    );
    let error = find_message_with_work(
        &client,
        &authority,
        &issuer.verifier(&mut None).expect("verifier copy"),
        corrupt_message,
        1,
        1_048_576,
        &mut Some(&mut io),
    )
    .await
    .expect_err("real raw locator reaches the original guarded wire decoder");
    assert!(
        matches!(&error, Error::NativeJson { custody: Some(custody), .. }
            if native.shares_ledger(custody.work()))
    );
    assert_eq!(native.receipt().authority_bytes_read, 2);
    drop(error);
    assert_eq!(io.into_custody()?.bytes(), 0);

    let native = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original checkpoint commit work");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original checkpoint observer"),
    );
    super::super::checkpoints::stage_immutable_record_with_work(
        &client,
        StreamPath::new("harness/v3/native-only-checkpoint")?,
        OwnedBytes::from_static(b"native-checkpoint"),
        &mut Some(&mut io),
    )
    .await?;
    assert_eq!(io.into_custody()?.bytes(), 0);

    for maximum_bytes in [1, event_bytes] {
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("separate finite denial work");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)
                .expect("original denial observer"),
        );
        let cursor = reader.pin(0, Some(&mut io)).await?;
        if maximum_bytes == 1 {
            assert!(
                reader
                    .read_page_with_work(
                        &cursor,
                        HistoryReadLimits {
                            maximum_events: 1,
                            maximum_bytes
                        },
                        false,
                        Some(&mut io)
                    )
                    .await
                    .is_err(),
                "raw encoded bound must reject before decode"
            );
        } else {
            cancellation.cancel();
            assert!(
                reader
                    .read_page_with_work(
                        &cursor,
                        HistoryReadLimits {
                            maximum_events: 1,
                            maximum_bytes
                        },
                        false,
                        Some(&mut io)
                    )
                    .await
                    .is_err(),
                "native cancellation must fail before provider I/O"
            );
        }
        drop(cursor);
        drop(io.into_custody()?);
    }
    Ok(())
}

#[tokio::test]
async fn real_memory_reader_never_uses_ordinary_provider_methods() -> Result<()> {
    exercise(Arc::new(acyclic_stream::MemoryStream::default())).await
}
