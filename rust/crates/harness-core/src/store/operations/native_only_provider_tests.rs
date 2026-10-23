use super::*;
use crate::store::checkpoints::native_staging_custody_tests::budget;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_native_runtime::NativeReadMeter;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitOutcome,
    ForkReceipt, ForkRequest, IdempotencyObservation, NativeProviderFuture, NativeRecordStream,
    ReadRequest, RecordStream, StreamBounds, StreamError,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct NativeOnly<P>(Arc<P>, AtomicBool);

impl<P> NativeOnly<P> {
    fn ordinary_error(&self) -> StreamError {
        self.1.store(true, Ordering::Relaxed);
        StreamError::Unsupported
    }
}

#[async_trait::async_trait]
impl<P: StreamProvider> StreamProvider for NativeOnly<P> {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.0.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        _: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        Err(self.ordinary_error())
    }
    async fn tail(&self, _: StreamPath) -> std::result::Result<u64, StreamError> {
        Err(self.ordinary_error())
    }
    async fn bounds(&self, _: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        Err(self.ordinary_error())
    }
    async fn append(&self, _: AppendRequest) -> std::result::Result<AppendOutcome, StreamError> {
        Err(self.ordinary_error())
    }
    async fn fork(&self, _: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
        Err(self.ordinary_error())
    }
    async fn read(&self, _: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        Err(self.ordinary_error())
    }
    async fn follow(
        &self,
        _: StreamPath,
        _: u64,
        _filter: acyclic_stream::FollowFilter,
    ) -> std::result::Result<RecordStream, StreamError> {
        Err(self.ordinary_error())
    }
    async fn children(&self, _: ChildrenRequest) -> std::result::Result<ChildStream, StreamError> {
        Err(self.ordinary_error())
    }
    async fn commit(&self, _: CommitRequest) -> std::result::Result<CommitOutcome, StreamError> {
        Err(self.ordinary_error())
    }
    async fn read_commit(
        &self,
        _: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        Err(self.ordinary_error())
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
        deadline: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.0.native_commit(request, deadline, meter)
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
        issuer.verifier(&mut None)?,
        SchemaRegistry::new(),
    );
    let command = Command {
        operation_id: OperationId::from_bytes([31; 16]),
        idempotency_key: crate::IdempotencyKey("native-only-reader".into()),
        expected_revision: 0,
        scope: issuer.issue(
            "root",
            &crate::Capabilities::new(["lifecycle:manage"]),
            None,
            None,
            &mut None,
        )?,
        causal_parent: None,
        action: Action::TransitionLifecycle {
            to: LifecycleState::Active,
            reason: None,
        },
    };
    let ApplyResult::Applied { event } = reducer.plan(&command)? else {
        return Err(Error::Invalid(
            "fresh lifecycle event was not applied".into(),
        ));
    };
    let encoded = crate::wire_codec::encode_event(&authority, &event, &mut None)?;
    let event_bytes = encoded.len() as u64;
    let publication = IndexedPublication::new(&authority, &event, encoded, None, &mut None)?;
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
    let client = Arc::new(NativeOnly(provider, AtomicBool::new(false)));
    let reader = HistoryReader::new(
        &client,
        &authority,
        &(issuer.verifier(&mut None)?),
        &mut None,
    )?;
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(budget(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let cursor = reader.pin(0, Some(&mut io)).await?;
    assert_eq!(cursor.through_revision, 1);
    let page = reader
        .read_page(
            &cursor,
            HistoryReadLimits {
                maximum_events: 1,
                maximum_bytes: event_bytes,
            },
            Some(&mut io),
        )
        .await?;
    assert_eq!(page.events.as_slice(), std::slice::from_ref(&event));
    assert_eq!(native.receipt().authority_bytes_read, event_bytes);
    assert_eq!(native.receipt().source_bytes_read, event_bytes);
    let mut work: Option<&mut dyn HistoryIo> = Some(&mut io);
    let bounds = bounds_with_work(&client, &publication.path, &mut work)
        .await?
        .map_err(|error| crate::store::operations::stream_error_with_work(error, &work))?;
    assert_eq!(bounds.tail, 1);
    drop(page);
    drop(cursor);
    drop(io.into_custody()?);

    let native = NativeMaterializationWork::new(budget(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let bytes = {
        let mut observer = Some(&mut io as &mut dyn crate::store::HistoryIo);
        let mut source = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
        let bytes = crate::core::copy_copy_vector(b"native-checkpoint", &mut source)?;
        source.transfer();
        bytes
    };
    super::super::checkpoints::stage_immutable(
        &client,
        (StreamPath::new("harness/v3/native-only-checkpoint")?, 0),
        Ok(bytes),
        &mut Some(&mut io),
    )
    .await?;
    assert_eq!(io.into_custody()?.bytes(), 0);
    assert!(
        !client.1.load(Ordering::Relaxed),
        "native operations called an ordinary provider method"
    );

    Ok(())
}

#[tokio::test]
async fn real_memory_reader_never_uses_ordinary_provider_methods() -> Result<()> {
    exercise(Arc::new(acyclic_stream::MemoryStream::default())).await
}
