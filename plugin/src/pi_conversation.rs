//! Canonical Pi task conversation binding, not a shadow conversation journal.
//! Projects and private volumes remain actual Filesystem resources supplied to
//! the existing parent controller; this module invents no resource projection.

use acyclic_fs::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use acyclic_harness::{Error, IdempotencyKey, JsonBounds, JsonCodecCharge, OperationId, Result,
    json_bytes_bounded_trusted,
    conversation::Limits,
    core::{Action, AggregateKind, Authority, AuthorityIssuer, AuthorityVerifier, Command,
        EventPayload, Reducer, SchemaRegistry, Scope},
    durable_host::TaskJournalOwner,
    store::{HistoryReadLimits, HistoryReader, StreamAggregate}};
use acyclic_stream::{StreamClient, StreamProvider};

struct Work { budget: WorkBudget, spent: WorkCounters, encoded_live: u64 }
impl Work {
    fn new(budget: WorkBudget) -> Self { Self { budget, spent: WorkCounters::default(), encoded_live: 0 } }
    fn failure(&self, error: impl std::fmt::Display) -> OperationFailure<Error> {
        OperationFailure::new(Error::Storage(error.to_string()), self.spent)
    }
    fn charge(&mut self, actual: WorkCounters) -> Result<()> {
        let next = self.spent.checked_add(actual).map_err(|error| Error::Invalid(error.to_string()))?;
        next.verify_within(&self.budget).map_err(|error| Error::Invalid(error.to_string()))?;
        self.spent = next;
        Ok(())
    }
    fn remaining(&self) -> MeasuredResult<WorkBudget, Error> {
        self.spent.remaining(self.budget).map_err(|error| self.failure(error))
    }
    fn accept<T>(&mut self, result: MeasuredResult<OperationReceipt<T>, Error>) -> MeasuredResult<T, Error> {
        match result {
            Ok(receipt) => { self.charge(receipt.work).map_err(|error| self.failure(error))?; Ok(receipt.value) }
            Err(failure) => {
                self.spent = self.spent.checked_add(*failure.work).map_err(|error| self.failure(error))?;
                Err(self.failure(failure.error))
            }
        }
    }
    fn codec(&mut self, charge: JsonCodecCharge) -> Result<()> {
        match charge {
            JsonCodecCharge::Work { bytes, items } => self.charge(WorkCounters {
                bytes_encoded: bytes, items_examined: items, ..WorkCounters::default() }),
            JsonCodecCharge::Copy { bytes } => self.charge(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }),
            JsonCodecCharge::CopyAdmission { bytes } => self.spent
                .admit(&WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }, &self.budget)
                .map_err(|error| Error::Invalid(error.to_string())),
            JsonCodecCharge::AllocationOperation { .. } =>
                self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() }),
            JsonCodecCharge::Retain { bytes } => {
                let live = self.encoded_live.checked_add(bytes).ok_or_else(|| Error::Invalid("Pi binding JSON custody overflows".into()))?;
                self.charge(WorkCounters { peak_allocation_bytes: live, ..WorkCounters::default() })?;
                self.encoded_live = live;
                Ok(())
            }
            JsonCodecCharge::Release { bytes } => {
                self.encoded_live = self.encoded_live.checked_sub(bytes).ok_or_else(|| Error::Invalid("Pi binding JSON custody differs".into()))?;
                Ok(())
            }
            _ => Err(Error::Invalid("Pi binding operation is an encoder-only boundary".into())),
        }
    }
    fn operation(&mut self, authority: &Authority, original_creation: OperationId,
        cancellation: &CancellationToken) -> MeasuredResult<OperationId, Error> {
        let remaining = self.remaining()?;
        let bytes = json_bytes_bounded_trusted(&("acyclic.pi/conversation-bind/v1", &authority.id, original_creation),
            JsonBounds { maximum_bytes: remaining.bytes_encoded.min(4096), maximum_items: remaining.items_examined, maximum_depth: 8 },
            &mut |charge| {
                if !matches!(charge, JsonCodecCharge::Release { .. }) {
                    cancellation.check().map_err(|error| Error::Invalid(error.to_string()))?;
                }
                self.codec(charge)
            }).map_err(|error| self.failure(error))?;
        self.charge(WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::default() }).map_err(|error| self.failure(error))?;
        let mut identity = [0; 16];
        identity.copy_from_slice(&blake3::hash(&bytes).as_bytes()[..16]);
        let length = bytes.len() as u64;
        drop(bytes);
        self.codec(JsonCodecCharge::Release { bytes: length }).map_err(|error| self.failure(error))?;
        Ok(OperationId::from_bytes(identity))
    }
}

fn validate(authority: &Authority, verifier: &AuthorityVerifier, scope: &Scope, budget: WorkBudget) -> Result<()> {
    if !budget.has_finite_allowances() || authority.kind != AggregateKind::Conversation
        || scope.agent().is_none() || !scope.capabilities().contains("conversation:bind") {
        return Err(Error::Unauthorized("Pi binding requires its actual finite conversation authority".into()));
    }
    verifier.verify_audience(authority)?;
    verifier.verify(scope)
}

/// Checks only actual authenticated committed events. No lease, append, source
/// acquisition or replacement of an unbound/differently-bound conversation.
pub(crate) async fn observe_task_conversation_measured<P: StreamProvider>(
    stream: &StreamClient<P>, authority: &Authority, verifier: &AuthorityVerifier,
    scope: &Scope, original_creation: OperationId, maximum_event_bytes: u64,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<bool>, Error> {
    observe_task_binding_measured(stream, authority, verifier, scope, original_creation,
        maximum_event_bytes, budget, cancellation).await.map(|receipt| OperationReceipt {
            value: receipt.value.is_some(), work: receipt.work,
        })
}

async fn observe_task_binding_measured<P: StreamProvider>(
    stream: &StreamClient<P>, authority: &Authority, verifier: &AuthorityVerifier,
    scope: &Scope, original_creation: OperationId, maximum_event_bytes: u64,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Option<u64>>, Error> {
    let mut work = Work::new(budget);
    validate(authority, verifier, scope, budget).map_err(|error| work.failure(error))?;
    cancellation.check().map_err(|error| work.failure(error))?;
    if maximum_event_bytes == 0 { return Err(work.failure("Pi binding history requires a finite positive event limit")); }
    let operation = work.operation(authority, original_creation, cancellation)?;
    let reader = HistoryReader::new(stream, authority, verifier.clone()).map_err(|error| work.failure(error))?;
    let mut cursor = work.accept(reader.pin_measured(0, work.remaining()?, cancellation).await)?;
    let mut reducer = Reducer::new(authority.clone(), verifier.clone(), SchemaRegistry::new());
    let mut original = None;
    while cursor.after_revision < cursor.through_revision {
        let remaining = work.remaining()?;
        let page = work.accept(reader.read_page_measured(&cursor, HistoryReadLimits {
            maximum_events: 1, maximum_bytes: maximum_event_bytes.min(remaining.authority_bytes_read),
        }, remaining, cancellation).await)?;
        cursor = page.cursor;
        for event in page.events {
            if let EventPayload::ConversationBound { agent } = &event.payload {
                if event.operation_id != operation || Some(*agent) != scope.agent()
                    || event.scope.agent() != scope.agent() || event.scope.id() != scope.id()
                    || event.scope.capabilities() != scope.capabilities() || event.causal_parent.is_some() {
                    return Err(work.failure("Pi conversation was bound outside its original creation"));
                }
                let previous_revision = event.revision.checked_sub(1)
                    .ok_or_else(|| work.failure("Pi binding event has no original command revision"))?;
                if original.replace(previous_revision).is_some() {
                    return Err(work.failure("Pi conversation has more than one binding event"));
                }
            } else if event.operation_id == operation {
                return Err(work.failure("Pi conversation binding operation belongs to another action"));
            }
            reducer.apply_committed(event).map_err(|error| work.failure(error))?;
        }
    }
    if reducer.conversation().and_then(|state| state.agent).is_some() != original.is_some() {
        return Err(work.failure("Pi conversation binding has no original committed event"));
    }
    Ok(OperationReceipt { value: original, work: work.spent })
}

/// Original task creator only. The SDK publication compares its exact lease,
/// cancellation state and Conversation tail atomically; an ACK retry reuses the
/// same operation and cannot bind another agent, task or future child audience.
#[allow(clippy::too_many_arguments, reason = "independent authority, original operation and bounded producer arguments")]
pub(crate) async fn bind_task_conversation_measured<P: StreamProvider + 'static>(
    stream: &StreamClient<P>, authority: &Authority, issuer: &AuthorityIssuer, scope: Scope,
    owner: &TaskJournalOwner<P>, original_creation: OperationId, limits: Limits,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<()>, Error> {
    let mut work = Work::new(budget);
    let verifier = issuer.verifier();
    validate(authority, &verifier, &scope, budget).map_err(|error| work.failure(error))?;
    if authority.id != owner.task_id().to_string() {
        return Err(work.failure("Pi conversation binding differs from the original task owner"));
    }
    limits.validate().map_err(|error| work.failure(error))?;
    cancellation.check().map_err(|error| work.failure(error))?;
    work.accept(owner.verify_dispatch_measured(work.remaining()?, cancellation).await)?;
    let original_revision = work.accept(observe_task_binding_measured(stream, authority, &verifier,
        &scope, original_creation, limits.file_bytes, work.remaining()?, cancellation).await)?;
    let operation = work.operation(authority, original_creation, cancellation)?;
    let mut aggregate = work.accept(StreamAggregate::open_measured(stream, authority.clone(), verifier,
        SchemaRegistry::new(), work.remaining()?, cancellation).await)?
        .with_limits(limits).map_err(|error| work.failure(error))?;
    if original_revision.is_none() && aggregate.reducer().conversation().and_then(|state| state.agent).is_some() {
        return Err(work.failure("Pi conversation changed during original binding admission"));
    }
    let command = Command { operation_id: operation,
        idempotency_key: IdempotencyKey::new(format!("acyclic.pi/conversation-bind/{}/{}", authority.id, original_creation))
            .map_err(|error| work.failure(error))?,
        expected_revision: original_revision.unwrap_or_else(|| aggregate.reducer().revision()), causal_parent: None,
        action: Action::BindConversation { agent: scope.agent().ok_or_else(|| work.failure("Pi conversation scope has no actual agent"))? },
        scope };
    // An already-bound reopen must still use the canonical SDK ACK verifier;
    // it alone can retain the opaque original binding proof on this owner.
    work.accept(aggregate.execute_task_command_measured(command, owner, work.remaining()?, cancellation).await)?;
    Ok(OperationReceipt { value: (), work: work.spent })
}

/// All three authority values come from the original protected issuer. Their
/// producer custody remains live until after the values and SDK command drop.
struct NativeConversationAuthority {
    authority: Authority,
    issuer: AuthorityIssuer,
    scope: Scope,
    _custody: acyclic_fs::NativeAllocation,
}

struct NativeOwned<T> {
    value: T,
    _custody: acyclic_fs::NativeAllocation,
}

fn native_invalid(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason }
}


/// Reads the original atomic operation index, including archived bindings.
/// The SDK reader authenticates the original audience, attestation and event;
/// no resident-revision assumption or reconstructed event enters this path.
async fn observe_binding_with_native_work<P: StreamProvider>(
    stream: &StreamClient<P>, authority: &Authority, verifier: &AuthorityVerifier,
    scope: &Scope, operation: OperationId, maximum_event_bytes: u64,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<Option<u64>> {
    if maximum_event_bytes == 0 {
        return Err(native_invalid("Pi binding requires a positive immutable event limit"));
    }
    let reader = HistoryReader::new_with_native_work(stream, authority, verifier, work)?;
    let reader = NativeOwned { value: reader.0, _custody: reader.1 };
    let original = reader.value.operation_event_bounded_with_native_work(
        operation, maximum_event_bytes, work).await?;
    let original = NativeOwned { value: original.0, _custody: original.1 };
    let Some(event) = original.value.as_ref() else { return Ok(None); };
    match &event.payload {
        EventPayload::ConversationBound { agent }
            if Some(*agent) == scope.agent()
                && event.scope.agent() == scope.agent()
                && event.scope.id() == scope.id()
                && event.scope.capabilities() == scope.capabilities()
                && event.causal_parent.is_none() =>
        {
            event.revision.checked_sub(1).map(Some)
                .ok_or_else(|| native_invalid("Pi binding event has no original command revision"))
        }
        _ => Err(native_invalid("Pi binding differs from its original committed creation")),
    }
}

/// Original finite native ledger; no ordinary scope clone, fresh meter, or
/// shadow binding cache. Historical ACK retries execute the same SDK command.
#[allow(clippy::too_many_arguments, reason = "original producer custody and guarded creator are explicit")]
pub(crate) async fn bind_task_conversation_with_native_work<P: StreamProvider + 'static>(
    stream: &StreamClient<P>, authority: (Authority, AuthorityIssuer, Scope),
    authority_custody: acyclic_fs::NativeAllocation, owner: &TaskJournalOwner<P>,
    exact_volume: &acyclic_harness::conversation::VolumeRef,
    original_creation: acyclic_fs::OperationId, limits: Limits,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let authority = NativeConversationAuthority { authority: authority.0, issuer: authority.1,
        scope: authority.2, _custody: authority_custody };
    if authority.authority.kind != AggregateKind::Conversation
        || authority.scope.agent().is_none()
        || !authority.scope.capabilities().contains("conversation:bind")
    {
        return Err(native_invalid("Pi binding requires its original conversation authority"));
    }
    let task = work.display(&format_args!("{}", owner.task_id()))
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let task = NativeOwned { value: task.0, _custody: task.1 };
    if task.value != authority.authority.id {
        return Err(native_invalid("Pi conversation binding differs from the original task owner"));
    }
    // Pi's task/audience association is checked above; the SDK owns the
    // canonical original Bind/reopen, current-fence guard and immutable ACK.
    // Its exact command bytes retain their intrinsic original custody until
    // after the genuine publication completes; they are never extracted.
    let encoded = acyclic_harness::filesystem::bind_original_root_conversation_with_native_work(
        stream, (authority.authority, authority.issuer, authority.scope),
        authority._custody, owner, exact_volume, original_creation, limits, work,
    ).await?;
    drop(encoded);
    Ok(())
}

/// Cold observation of the original binding uses the same authenticated
/// immutable operation query; it cannot populate a current owner's cache.
#[allow(clippy::too_many_arguments, reason = "original authority custody and immutable operation are explicit")]
pub(crate) async fn observe_task_conversation_with_native_work<P: StreamProvider>(
    stream: &StreamClient<P>, authority: (Authority, AuthorityIssuer, Scope),
    authority_custody: acyclic_fs::NativeAllocation,
    exact_volume: &acyclic_harness::conversation::VolumeRef,
    original_creation: acyclic_fs::OperationId,
    maximum_event_bytes: u64, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<bool> {
    let authority = NativeConversationAuthority { authority: authority.0, issuer: authority.1,
        scope: authority.2, _custody: authority_custody };
    if authority.authority.kind != AggregateKind::Conversation
        || authority.scope.agent().is_none()
        || !authority.scope.capabilities().contains("conversation:bind")
    {
        return Err(native_invalid("Pi binding requires its original conversation authority"));
    }
    let verifier = authority.issuer.verifier_with_native_work(work)?;
    let verifier = NativeOwned { value: verifier.0, _custody: verifier.1 };
    let operation = acyclic_harness::filesystem::native_root_output_binding_operation(
        &authority.authority, exact_volume, original_creation, work)?;
    Ok(observe_binding_with_native_work(stream, &authority.authority, &verifier.value,
        &authority.scope, operation, maximum_event_bytes, work).await?.is_some())
}
