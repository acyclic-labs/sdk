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
    let path = path(task, work)?;
    let io = PreparationIo::from_native_work(work)?;
    let mut history = &io;
    let mut observer = Some(&mut history as &mut dyn crate::store::history::HistoryIo);
    let bounds =
        match crate::store::operations::bounds_with_work(client, &path, &mut observer).await? {
            Ok(bounds) => bounds,
            Err(error)
                if matches!(
                    &error,
                    acyclic_stream::StreamError::NotFound
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(crate::store::operations::stream_error_with_work(error, &observer)),
        };
    let Some(sequence) = bounds.tail.checked_sub(1) else {
        return Ok(None);
    };
    let stream = crate::store::operations::stream(client, path.as_str(), &mut observer)?;
    let mut records =
        crate::store::operations::read_records(client, stream.0.path(), sequence, 1, &mut observer)
            .await?
            .map_err(|error| crate::store::operations::stream_error_with_work(error, &observer))?;
    let record = records.next(&mut observer).await?.ok_or_else(invalid)?;
    if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(invalid());
    }
    let (mut journal, mut custody) = native_decoded(io.decode(
        std::marker::PhantomData::<MaterializationJournal>,
        &record.value,
        crate::wire::ErrorCode::Storage,
    )?)?;
    custody.substitute(acyclic_fs::materializer::journal_owned_bytes(&journal)?)?;
    if record.sequence != sequence
        || journal.revision != bounds.tail
        || !journal.is_valid_for(operation)
    {
        return Err(invalid());
    }
    journal.custody = Some(custody);
    Ok(Some(journal))
}

pub(in crate::filesystem) fn key(
    bytes: &[u8],
    work: &NativeMaterializationWork,
) -> Result<acyclic_stream::IdempotencyKey> {
    work.charge(WorkCounters {
        bytes_hashed: bytes.len() as u64,
        ..Default::default()
    })?;
    let (bytes, custody) = work.copy_bytes(blake3::hash(bytes).as_bytes())?;
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
}

impl<P: StreamProvider> acyclic_fs::MaterializationJournalStore for Store<P> {
    type Error = Error;
    type ActionCompletion<'a>
        = crate::NativeFutureBundle<'a, MaterializationJournal>
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
                .is_some_and(|custody| custody.matches_work(&self.work))
        {
            return Err(invalid());
        }
        replacement.work = self.work.journal_work()?;
        let io = PreparationIo::from_native_work(&self.work)?;
        let bytes = io.encode(&replacement, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
        let key = key(&bytes, &self.work)?;
        let committed = self
            .owner
            .append(
                path(self.owner.task_binding().0, &self.work)?,
                expected,
                &key,
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
        let retained = self
            .load(journal.plan.operation_id)
            .await?
            .ok_or_else(invalid)?;
        if retained != journal {
            return Err(invalid());
        }
        drop(retained);
        let template = super::completion::NativePreparationCompletionTemplate::new(
            journal,
            self.owner.as_ref(),
            &self.work,
        )?;
        crate::distributed::prepare_materialization_commit(
            self.owner.as_ref(),
            template,
            &self.work,
        )
        .await
    }
}
