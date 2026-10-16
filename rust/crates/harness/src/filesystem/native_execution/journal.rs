use super::super::{PreparationIo, native_decoded};
use super::preparation_record::invalid;
use super::*;
use acyclic_fs::{MaterializationJournal, NativeMaterializationWork, WorkCounters};

pub(super) fn operation(
    task: crate::TaskId,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::OperationId> {
    const DOMAIN: &[u8] = b"harness/native-preparation/v1\0";
    work.charge(WorkCounters {
        bytes_hashed: (DOMAIN.len() + 16) as u64,
        ..Default::default()
    })?;
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    hash.update(&task.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok(acyclic_fs::OperationId::from_bytes(bytes))
}

pub(super) async fn read<P: StreamProvider>(
    witness: &crate::durable_host::TaskAdmissionReadonlyWitness<P>,
    work: &NativeMaterializationWork,
) -> Result<Option<MaterializationJournal>> {
    witness.verify_with_native_work(work).await?;
    let result = read_record(witness.prepared_stream(), witness.task(), work).await;
    witness.verify_with_native_work(work).await?;
    result
}

/// Reads only the authenticated preparation's original task and fence.
pub(crate) fn preparation_owner<'a, P: StreamProvider>(
    witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
    work: &'a NativeMaterializationWork,
) -> Result<
    crate::NativeFutureBundle<
        'a,
        Option<(
            crate::TaskId,
            crate::scheduler::LeaseFence,
            NativeAllocation,
        )>,
    >,
> {
    use super::preparation_record::Intent;
    if !work.remaining()?.has_finite_allowances() {
        return Err(invalid());
    }
    crate::NativeFutureBundle::new(work, || async move {
        let Some(journal) = read(witness, work).await? else {
            return Ok(None);
        };
        if journal.custody.is_none() {
            return Err(invalid());
        }
        let io = PreparationIo::from_native_work(work)?;
        let acyclic_fs::MaterializationIntent::NativePreparation {
            selection,
            original,
        } = &journal.plan.intent
        else {
            return Err(invalid());
        };
        let (intent, mut custody) = native_decoded(io.decode(
            std::marker::PhantomData::<Intent>,
            original,
            crate::wire::ErrorCode::Storage,
        )?)?;
        intent.manifest.validate(Some(&io))?;
        if intent.task != witness.task()
            || selection.len() != intent.manifest.volumes.len()
            || journal.plan.edits.iter().any(|edit| {
                !matches!(edit, acyclic_fs::MaterializationEdit::Install { image, .. }
                if image.as_slice() == acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)
            })
        {
            return Err(invalid());
        }
        for (selected, mapping) in selection.iter().zip(&intent.manifest.volumes) {
            let provider = io.encode(
                mapping.volume.provider(),
                work.remaining()?.bytes_encoded,
                true,
            )?;
            if provider.as_slice() != selected.provider_identity
                || selected.generation.digest().as_bytes().as_slice()
                    != mapping.generation.as_resource().key()
            {
                return Err(invalid());
            }
        }
        if let Some(scratch) = witness.scratch_proof()
            && (crate::runtime::task_id_for_admission(scratch.origin().intended_child)
                != intent.task
                || !intent
                    .manifest
                    .volumes
                    .iter()
                    .any(|mapping| mapping.writable && &mapping.volume == scratch.volume()))
        {
            return Err(invalid());
        }
        witness.verify_with_native_work(work).await?;
        drop(journal);
        let Intent {
            task,
            original_fence,
            ..
        } = intent;
        let bytes = (std::mem::size_of::<(
            crate::TaskId,
            crate::scheduler::LeaseFence,
            NativeAllocation,
        )>() as u64)
            .checked_add(original_fence.reservation_id.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(original_fence.placement.capacity() as u64))
            .ok_or_else(invalid)?;
        custody.resize(bytes)?;
        Ok(Some((task, original_fence, custody)))
    })
}

pub(super) fn path(
    task: crate::TaskId,
    work: &NativeMaterializationWork,
) -> Result<acyclic_stream::StreamPath> {
    let operation = operation(task, work)?;
    let (name, _name) = work.display(&format_args!(
        "harness/v2/materialization/{task}/{}",
        crate::OperationId::from_bytes(operation.into_bytes())
    ))?;
    acyclic_stream::StreamPath::new_native(&name, &work.runtime_meter())
        .map_err(|error| Error::from(work.runtime_error(error)))?
        .map_err(Error::from)
}

pub(super) async fn read_record<P: StreamProvider>(
    client: &Arc<P>,
    task: crate::TaskId,
    work: &NativeMaterializationWork,
) -> Result<Option<MaterializationJournal>> {
    let operation = operation(task, work)?;
    let Some(record) = read_payload(client, task, work).await? else {
        return Ok(None);
    };
    let io = PreparationIo::from_native_work(work)?;
    let (mut journal, mut custody) = native_decoded(io.decode(
        std::marker::PhantomData::<MaterializationJournal>,
        &record.value,
        crate::wire::ErrorCode::Storage,
    )?)?;
    custody.resize(acyclic_fs::materializer::journal_owned_bytes(&journal)?)?;
    if record.sequence.checked_add(1) != Some(journal.revision) || !journal.is_valid_for(operation)
    {
        return Err(invalid());
    }
    journal.custody = Some(custody);
    Ok(Some(journal))
}

async fn read_payload<P: StreamProvider>(
    client: &Arc<P>,
    task: crate::TaskId,
    work: &NativeMaterializationWork,
) -> Result<Option<crate::store::operations::ObservedRecord>> {
    let path = path(task, work)?;
    let io = PreparationIo::from_native_work(work)?;
    let mut history = &io;
    let mut observer = Some(&mut history as &mut dyn crate::store::history::HistoryIo);
    let bounds =
        match crate::store::operations::bounds_with_work(client, &path, &mut observer).await? {
            Ok(bounds) => bounds,
            Err(error) if matches!(&error, acyclic_stream::StreamError::NotFound) => {
                return Ok(None);
            }
            Err(error) => {
                return Err(crate::store::operations::stream_error_with_work(
                    error, &observer,
                ));
            }
        };
    let Some(sequence) = bounds.tail.checked_sub(1) else {
        return Ok(None);
    };
    let record = crate::store::operations::one_record(client, &path, sequence, &mut observer)
        .await?
        .ok_or_else(invalid)?;
    if record.sequence != sequence {
        return Err(invalid());
    }
    Ok(Some(record))
}

pub(super) fn key_from_digest(
    digest: &[u8; 32],
    work: &NativeMaterializationWork,
) -> Result<acyclic_stream::IdempotencyKey> {
    let (bytes, custody) = work.copy_bytes(digest)?;
    let bytes =
        acyclic_stream::OwnedBytes::from_vec_with_guard(bytes, custody, work.runtime_meter())
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                    Error::from(work.runtime_error(error))
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout.into()
                }
            })?;
    acyclic_stream::IdempotencyKey::new(bytes).map_err(Error::from)
}

pub(super) struct Store<P: StreamProvider> {
    pub(super) owner: Arc<TaskJournalOwner<P>>,
    pub(super) work: NativeMaterializationWork,
    pub(super) original: Option<(
        Arc<super::preparation_record::Intent>,
        Vec<u8>,
        NativeAllocation,
    )>,
}

impl<P: StreamProvider> acyclic_fs::MaterializationJournalStore for Store<P> {
    type Error = Error;
    type ActionCompletion<'a>
        = crate::NativeFutureBundle<'static, MaterializationJournal>
    where
        Self: 'a;

    async fn load(
        &self,
        operation: acyclic_fs::OperationId,
    ) -> Result<Option<MaterializationJournal>> {
        if operation != self::operation(self.owner.task_binding().0, &self.work)? {
            return Err(invalid());
        }
        self.owner
            .verify(
                false,
                &mut Some(&mut crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(&self.work)?,
                )),
            )
            .await?;
        let journal = read_record(
            &self.owner.stream(),
            self.owner.task_binding().0,
            &self.work,
        )
        .await?;
        self.owner
            .verify(
                false,
                &mut Some(&mut crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(&self.work)?,
                )),
            )
            .await?;
        self.work.set_original_work(
            journal
                .as_ref()
                .map_or_else(WorkCounters::default, |journal| journal.work),
        );
        Ok(journal)
    }

    async fn compare_and_swap(
        &self,
        operation: acyclic_fs::OperationId,
        expected: u64,
        mut replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal)> {
        if operation != self::operation(self.owner.task_binding().0, &self.work)?
            || replacement.plan.operation_id != operation
            || expected.checked_add(1) != Some(replacement.revision)
            || !replacement.is_valid_for(operation)
            || !replacement
                .custody
                .as_ref()
                .is_some_and(|custody| custody.work().shares_allocation_context(&self.work))
        {
            return Err(invalid());
        }
        replacement.work = self.work.journal_work()?;
        let io = PreparationIo::from_native_work(&self.work)?;
        let bytes = io.encode(
            &super::journal_encoding::Canonical(&replacement),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            false,
        )?;
        let committed = self
            .owner
            .append(
                path(self.owner.task_binding().0, &self.work)?,
                expected,
                None,
                bytes,
                crate::distributed::JournalWrite::Fresh,
                &mut Some(&mut io.history_io()),
            )
            .await?;
        Ok((committed, replacement))
    }

    async fn prepare_action<'a>(
        &'a self,
        journal: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>> {
        let io = PreparationIo::from_native_work(&self.work)?;
        let (original, encoded, _) = self.original.as_ref().ok_or_else(invalid)?;
        let acyclic_fs::MaterializationIntent::NativePreparation {
            original: retained_original,
            ..
        } = &journal.plan.intent
        else {
            return Err(invalid());
        };
        self.work.charge(WorkCounters {
            items_examined: encoded.len() as u64,
            ..Default::default()
        })?;
        if retained_original != encoded {
            return Err(invalid());
        }
        if journal.plan.operation_id != operation(self.owner.task_binding().0, &self.work)? {
            return Err(invalid());
        }
        let identity = io
            .digest(
                &super::journal_encoding::Canonical(&journal),
                acyclic_stream::MAX_RECORD_BYTES as u64,
                None,
            )?
            .ok_or_else(invalid)?;
        self.work.set_original_work(journal.work);
        let template = super::completion::NativePreparationCompletionTemplate::new(
            journal,
            self.owner.as_ref(),
            &self.work,
            identity,
            original,
        )?;
        // This private adapter receives journals only from load, successful CAS,
        // or an accepted completion. Immutable records cannot change under the
        // same tail. Acceptance checks that tail and the live owner atomically
        // before acknowledging any physical action.
        crate::distributed::prepare_materialization_commit(
            self.owner.as_ref(),
            template,
            &self.work,
        )
        .await
    }
}
