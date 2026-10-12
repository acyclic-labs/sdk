use super::*;

pub(super) async fn checkpoint_preparation<P: StreamProvider>(
    store: &NativePreparationStore<'_, P>,
    mut journal: acyclic_fs::MaterializationJournal,
) -> Result<()> {
    let expected = journal.revision;
    journal.revision = expected.checked_add(1).ok_or(boundary(
        crate::wire::ErrorCode::Storage,
        "native preparation revision overflow",
    ))?;
    let operation = journal.plan.operation_id;
    let (committed, _) = store.compare_and_swap(operation, expected, journal).await?;
    if !committed {
        return Err(boundary(
            crate::wire::ErrorCode::Conflict,
            "native preparation changed before attachment checkpoint",
        ));
    }
    Ok(())
}

pub(super) enum NativePreparationAuthority<'a, P: StreamProvider> {
    Owner {
        owner: &'a TaskJournalOwner<P>,
        settlement: bool,
    },
    Admission(&'a crate::durable_host::TaskAdmissionReadonlyWitness<P>),
}

/// Authenticated pre-action packet for one accepted task-preparation completion.
pub(crate) struct NativePreparationCompletionTemplate {
    pub(super) journal: acyclic_fs::MaterializationJournal,
    pub(super) binding: (crate::TaskId, crate::scheduler::LeaseFence),
    pub(super) volumes: Vec<NativeVolumeMapping>,
    pub(super) path: acyclic_stream::StreamPath,
    pub(super) key: acyclic_stream::IdempotencyKey,
    pub(super) value: serde_json::Value,
    pub(super) output: acyclic_stream::PreparedOwnedBytes<acyclic_fs::NativeAllocation>,
    pub(super) maximum_bytes: u64,
    pub(super) custody: acyclic_fs::NativeAllocation,
}

impl NativePreparationCompletionTemplate {
    pub(crate) fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence) {
        (self.binding.0, &self.binding.1)
    }
    pub(crate) fn operation(&self) -> acyclic_fs::OperationId {
        self.journal.plan.operation_id
    }
    pub(crate) fn expected_revision(&self) -> u64 {
        self.journal.revision
    }
    pub(crate) fn path(&self) -> &acyclic_stream::StreamPath {
        &self.path
    }
    pub(crate) fn key(&self) -> &acyclic_stream::IdempotencyKey {
        &self.key
    }
    pub(crate) fn matches_work(&self, work: &NativeMaterializationWork) -> bool {
        self.custody.work().shares_allocation_context(work)
            && self
                .journal
                .custody
                .as_ref()
                .is_some_and(|guard| guard.work().shares_allocation_context(work))
    }
    /// Checks sealed packet associations only.
    pub(crate) fn verify_owner<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.matches_work(work)
            || self.task_binding() != owner.task_binding()
            || self.journal.phase != MaterializationPhase::Preparing
            || !self.journal.is_valid_for(self.operation())
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "native completion differs from its original protected preparation",
            ));
        }
        for mapping in &self.volumes {
            owner.require_volume_grant(
                &mapping.volume,
                VolumeOperation::Read,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )?;
            if mapping.writable {
                owner.require_volume_grant(
                    &mapping.volume,
                    VolumeOperation::Write,
                    &mut Some(&mut crate::store::history::native_work_io(work)?),
                )?;
            }
        }
        Ok(())
    }

    /// Only the revision and existing unsigned work slots may change after the action.
    pub(crate) fn set_completed_work(&mut self, counters: acyclic_fs::WorkCounters) -> Result<()> {
        Self::set_value_work(&mut self.value, self.journal.revision, counters)?;
        self.journal.work = counters;
        Ok(())
    }

    pub(super) fn set_value_work(
        value: &mut serde_json::Value,
        expected_revision: u64,
        counters: acyclic_fs::WorkCounters,
    ) -> Result<()> {
        let revision = expected_revision.checked_add(1).ok_or(boundary(
            crate::wire::ErrorCode::Storage,
            "native completion revision overflow",
        ))?;
        let root = value.as_object_mut().expect("serialized journal object");
        *root.get_mut("revision").expect("serialized revision slot") =
            serde_json::Value::Number(revision.into());
        let work = root
            .get_mut("work")
            .and_then(serde_json::Value::as_object_mut)
            .expect("serialized work object");
        macro_rules! replace {
            ($field:ident, $key:literal) => {
                *work.get_mut($key).expect("serialized counter slot") =
                    serde_json::Value::Number(counters.$field.into());
            };
        }
        replace!(authority_records_read, "authorityRecordsRead");
        replace!(authority_records_appended, "authorityRecordsAppended");
        replace!(authority_bytes_read, "authorityBytesRead");
        replace!(authority_bytes_written, "authorityBytesWritten");
        replace!(object_probes, "objectProbes");
        replace!(backend_read_operations, "backendReadOperations");
        replace!(backend_write_operations, "backendWriteOperations");
        replace!(durability_operations, "durabilityOperations");
        replace!(page_reads, "pageReads");
        replace!(page_writes, "pageWrites");
        replace!(object_bytes_read, "objectBytesRead");
        replace!(object_bytes_written, "objectBytesWritten");
        replace!(bytes_hashed, "bytesHashed");
        replace!(bytes_copied, "bytesCopied");
        replace!(bytes_encoded, "bytesEncoded");
        replace!(source_bytes_read, "sourceBytesRead");
        replace!(source_path_components, "sourcePathComponents");
        replace!(source_entries_visited, "sourceEntriesVisited");
        replace!(output_bytes, "outputBytes");
        replace!(items_examined, "itemsExamined");
        replace!(items_returned, "itemsReturned");
        replace!(allocation_operations, "allocationOperations");
        replace!(peak_allocation_bytes, "peakAllocationBytes");
        replace!(materializations, "materializations");
        Ok(())
    }

    /// Borrows the fixed source and prepaid output without reserving.
    pub(crate) fn codec_parts(&mut self) -> (&serde_json::Value, &mut [u8], u64) {
        (&self.value, self.output.writable(), self.maximum_bytes)
    }
    pub(crate) fn into_completed_parts(
        self,
    ) -> (
        acyclic_fs::MaterializationJournal,
        acyclic_stream::PreparedOwnedBytes<acyclic_fs::NativeAllocation>,
        acyclic_fs::NativeAllocation,
    ) {
        let Self {
            mut journal,
            binding,
            volumes,
            path,
            key,
            value,
            output,
            maximum_bytes: _,
            custody,
        } = self;
        journal.revision += 1;
        drop((binding, volumes, path, key, value));
        (journal, output, custody)
    }
}

/// Fenced Stream adapter for the existing materializer journal, not a second log.
pub(super) struct NativePreparationStore<'a, P: StreamProvider> {
    pub(super) authority: NativePreparationAuthority<'a, P>,
    pub(super) work: NativeMaterializationWork,
}

impl<'a, P: StreamProvider> NativePreparationStore<'a, P> {
    pub(super) fn task(&self) -> crate::TaskId {
        match &self.authority {
            NativePreparationAuthority::Owner { owner, .. } => owner.task_binding().0,
            NativePreparationAuthority::Admission(witness) => witness.task(),
        }
    }

    pub(super) fn read_client(&self) -> std::sync::Arc<P> {
        match &self.authority {
            NativePreparationAuthority::Owner { owner, .. } => owner.stream(),
            NativePreparationAuthority::Admission(witness) => Arc::clone(witness.prepared_stream()),
        }
    }

    pub(super) fn path(
        &self,
        operation: acyclic_fs::OperationId,
    ) -> Result<acyclic_stream::StreamPath> {
        if operation != NativePreparationRoot::operation(self.task(), &self.work)? {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "native materializer operation differs from original task",
            ));
        }
        const PREFIX: &[u8] = b"harness/v2/materialization/";
        let mut bytes = [0_u8; PREFIX.len() + 36 + 1 + 36];
        let completed = acyclic_fs::WorkCounters {
            bytes_copied: PREFIX.len() as u64 + 1,
            bytes_encoded: 72,
            items_examined: bytes.len() as u64,
            ..Default::default()
        };
        self.work.admit(completed)?;
        bytes[..PREFIX.len()].copy_from_slice(PREFIX);
        let separator = PREFIX.len() + 36;
        uuid::Uuid::from_bytes(self.task().into_bytes())
            .hyphenated()
            .encode_lower(&mut bytes[PREFIX.len()..separator]);
        bytes[separator] = b'/';
        uuid::Uuid::from_bytes(operation.into_bytes())
            .hyphenated()
            .encode_lower(&mut bytes[separator + 1..]);
        let path = std::str::from_utf8(&bytes).map_err(|_| {
            boundary(
                crate::wire::ErrorCode::Storage,
                "native preparation UUID encoding is invalid",
            )
        })?;
        self.work.spent(completed)?;
        acyclic_stream::StreamPath::new_native(path, &self.work.runtime_meter())
            .map_err(|error| Error::from(self.work.runtime_error(error)))?
            .map_err(preparation_backend)
    }

    pub(super) async fn verify_read_owner(&self) -> Result<()> {
        match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement } => {
                let (outcome, custody) = owner.outcome_with_native_work(&self.work).await?;
                if outcome.is_none() {
                    owner
                        .verify(
                            *settlement,
                            &mut Some(&mut crate::store::history::native_work_io(&self.work)?),
                        )
                        .await?;
                }
                drop(outcome);
                drop(custody);
                Ok(())
            }
            NativePreparationAuthority::Admission(witness) => {
                witness.verify_with_native_work(&self.work).await?;
                Ok(())
            }
        }
    }

    pub(super) async fn load_record(
        &self,
        operation: acyclic_fs::OperationId,
        adopt_original_work: bool,
    ) -> Result<Option<MaterializationJournal>> {
        let path = self.path(operation)?;
        self.verify_read_owner().await?;
        let mut native_io = crate::store::history::native_work_io(&self.work)?;
        let mut observer = Some(&mut native_io as &mut dyn crate::store::history::HistoryIo);
        let bounds = match crate::store::operations::bounds_with_work(
            &self.read_client(),
            &path,
            &mut observer,
        )
        .await?
        {
            Ok(bounds) => bounds,
            Err(error)
                if matches!(
                    crate::distributed::sdk_source(&error),
                    acyclic_stream::StreamError::NotFound
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(crate::distributed::sdk_error(error, &observer)),
        };
        let sequence = match bounds.tail.checked_sub(1) {
            Some(sequence) => sequence,
            None if !adopt_original_work => return Ok(None),
            None => {
                return Err(boundary(
                    crate::wire::ErrorCode::Storage,
                    "empty materializer journal",
                ));
            }
        };
        self.verify_read_owner().await?;
        let stream =
            crate::store::operations::stream(&self.read_client(), (&path).as_str(), &mut observer)?;
        let mut records = crate::store::operations::read_records_with_work(
            &self.read_client(),
            &stream.0,
            sequence,
            1,
            &mut observer,
        )
        .await?
        .map_err(|error| crate::distributed::sdk_error(error, &observer))?;
        let record = records.next(&mut observer).await?.ok_or(boundary(
            crate::wire::ErrorCode::Storage,
            "missing materializer journal",
        ))?;
        if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "materializer journal exceeds Stream record limit",
            ));
        }
        self.verify_read_owner().await?;
        let (mut journal, custody): (MaterializationJournal, _) = decode_record(
            &record.value,
            [
                std::mem::size_of::<acyclic_fs::MaterializationEdit>(),
                std::mem::size_of::<acyclic_fs::MaterializationPreparationSelection>(),
                std::mem::size_of::<acyclic_fs::MaterializationPreimage>(),
                std::mem::size_of::<u8>(),
            ]
            .into_iter()
            .max()
            .expect("nonempty concrete journal schema"),
            &self.work,
            |journal| acyclic_fs::materializer::journal_owned_bytes(journal).map_err(Into::into),
        )?;
        if record.sequence != sequence
            || journal.revision != bounds.tail
            || journal.plan.operation_id != operation
        {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "materializer journal revision or operation changed",
            ));
        }
        if !journal.is_valid_for(operation) {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "materializer journal has incompatible structural progress",
            ));
        }
        if adopt_original_work {
            self.work.set_original_work(journal.work);
        }
        journal.custody = Some(custody);
        Ok(Some(journal))
    }
}

impl<P: StreamProvider> MaterializationJournalStore for NativePreparationStore<'_, P> {
    type Error = Error;

    type ActionCompletion<'a>
        = crate::NativeFutureBundle<'a, acyclic_fs::MaterializationJournal>
    where
        Self: 'a;

    async fn prepare_action<'a>(
        &'a self,
        journal: acyclic_fs::MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>> {
        let operation = journal.plan.operation_id;
        let owner = match &self.authority {
            NativePreparationAuthority::Owner {
                owner,
                settlement: false,
            } => *owner,
            _ => {
                return Err(boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "native action acceptance requires the original running owner",
                ));
            }
        };
        owner
            .verify(
                false,
                &mut Some(&mut crate::store::history::native_work_io(&self.work)?),
            )
            .await?;
        let loaded = self.load_record(operation, false).await?.ok_or(boundary(
            crate::wire::ErrorCode::Storage,
            "native completion original preparation is missing",
        ))?;
        if loaded != journal {
            return Err(boundary(
                crate::wire::ErrorCode::Conflict,
                "native completion original packet differs from its durable Preparing record",
            ));
        }
        drop(loaded);
        if journal.phase != MaterializationPhase::Preparing || journal.revision == 0 {
            return Err(boundary(
                crate::wire::ErrorCode::Conflict,
                "native completion source is not Preparing",
            ));
        }
        let (intent, mut intent_custody) = preparation_intent(&journal, &self.work)?;
        if (intent.task, &intent.original_fence) != owner.task_binding() {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "native completion source belongs to another original task fence",
            ));
        }
        for mapping in &intent.manifest.volumes {
            owner.require_volume_grant(
                &mapping.volume,
                VolumeOperation::Read,
                &mut Some(&mut crate::store::history::native_work_io(&self.work)?),
            )?;
            if mapping.writable {
                owner.require_volume_grant(
                    &mapping.volume,
                    VolumeOperation::Write,
                    &mut Some(&mut crate::store::history::native_work_io(&self.work)?),
                )?;
            }
        }
        let retained = intent_custody.bytes()
            - intent.operation_directory.capacity() as u64
            - intent.manifest.options.root.capacity() as u64;
        let NativePreparationIntent {
            task,
            original_fence,
            manifest,
            operation_directory,
        } = intent;
        let NativeViewManifest { options, volumes } = manifest;
        drop((options, operation_directory));
        intent_custody.substitute(retained)?;
        let (encoded, encoded_custody) = encode_record(&journal, &self.work)?;
        self.work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: encoded.len() as u64,
            ..acyclic_fs::WorkCounters::default()
        })?;
        let association = *blake3::hash(&encoded).as_bytes();
        drop(encoded);
        drop(encoded_custody);
        let (key_bytes, key_custody) = self.work.copy_bytes(&association)?;
        let key_bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            key_bytes,
            key_custody,
            self.work.runtime_meter(),
        )
        .map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let key = acyclic_stream::IdempotencyKey::new(key_bytes)?;
        let path = self.path(operation)?;
        let io = crate::filesystem::PreparationIo::from_native_work(&self.work)?;
        let mut value = io.value_json(&journal, self.work.remaining()?.bytes_encoded)?;
        let mut custody = self.work.claim(
            std::mem::size_of::<NativePreparationCompletionTemplate>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        custody.absorb(&mut intent_custody)?;
        let held = io.owned_bytes()?;
        custody.absorb(&mut io.into_native_custody(held)?)?;
        NativePreparationCompletionTemplate::set_value_work(
            &mut value,
            journal.revision,
            acyclic_fs::WorkCounters::UNBOUNDED,
        )?;
        let (largest, largest_custody) = encode_record(&value, &self.work)?;
        let maximum_bytes = largest.len();
        drop(largest);
        drop(largest_custody);
        NativePreparationCompletionTemplate::set_value_work(
            &mut value,
            journal.revision,
            journal.work,
        )?;
        if maximum_bytes > acyclic_stream::MAX_RECORD_BYTES {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "native completion scalar envelope exceeds record limit",
            ));
        }
        let (mut output, output_custody) = allocate(&self.work, |io| {
            crate::filesystem::fork_preparer_io::vector::<u8>(maximum_bytes, Some(io))
        })?;
        let initialized = acyclic_fs::WorkCounters {
            bytes_copied: output.capacity() as u64,
            ..Default::default()
        };
        self.work.admit(initialized)?;
        output.resize(output.capacity(), 0);
        self.work.spent(initialized)?;
        let output = acyclic_stream::PreparedOwnedBytes::new(
            output,
            output_custody,
            self.work.runtime_meter(),
        )
        .map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let template = NativePreparationCompletionTemplate {
            journal,
            binding: (task, original_fence),
            volumes,
            path,
            key,
            value,
            output,
            maximum_bytes: maximum_bytes as u64,
            custody,
        };
        owner
            .verify(
                false,
                &mut Some(&mut crate::store::history::native_work_io(&self.work)?),
            )
            .await?;
        template.verify_owner(owner, &self.work)?;
        crate::distributed::prepare_materialization_commit(owner, template, &self.work).await
    }

    async fn load(
        &self,
        operation: acyclic_fs::OperationId,
    ) -> Result<Option<acyclic_fs::MaterializationJournal>> {
        self.load_record(operation, true).await
    }

    async fn compare_and_swap(
        &self,
        operation: acyclic_fs::OperationId,
        expected_revision: u64,
        mut replacement: acyclic_fs::MaterializationJournal,
    ) -> Result<(bool, acyclic_fs::MaterializationJournal)> {
        let (owner, settlement) = match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement } => (*owner, *settlement),
            NativePreparationAuthority::Admission(_) => {
                return Err(boundary(
                    crate::wire::ErrorCode::Unsupported,
                    "read-only native admission cannot append a materializer journal",
                ));
            }
        };
        if !replacement
            .custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_allocation_context(&self.work))
        {
            return Err(boundary(
                crate::wire::ErrorCode::Conflict,
                "native journal requires its original buffer custody",
            ));
        }
        if replacement.plan.operation_id != operation
            || replacement.revision
                != expected_revision.checked_add(1).ok_or(boundary(
                    crate::wire::ErrorCode::Storage,
                    "materializer revision overflow",
                ))?
        {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "invalid native materializer replacement",
            ));
        }
        replacement.work = self.work.journal_work()?;
        let (encoded, encoded_custody) = encode_record(&replacement, &self.work)?;
        let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            encoded,
            encoded_custody,
            self.work.runtime_meter(),
        )
        .map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "materializer journal exceeds Stream record limit",
            ));
        }
        self.work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..acyclic_fs::WorkCounters::default()
        })?;
        let (key_bytes, key_custody) = self.work.copy_bytes(blake3::hash(&bytes).as_bytes())?;
        let key_bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            key_bytes,
            key_custody,
            self.work.runtime_meter(),
        )
        .map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let key = acyclic_stream::IdempotencyKey::new(key_bytes)?;
        let path = self.path(operation)?;
        let mut io = crate::store::history::native_work_io(&self.work)?;
        let committed = owner
            .append(
                path,
                expected_revision,
                &key,
                bytes,
                if settlement {
                    crate::distributed::JournalWrite::Settlement
                } else {
                    crate::distributed::JournalWrite::Fresh
                },
                &mut Some(&mut io),
            )
            .await?;
        Ok((committed, replacement))
    }
}

pub(super) fn native_intent_control_bytes(intent: &NativePreparationIntent) -> Result<u64> {
    let overflow = || {
        boundary(
            crate::wire::ErrorCode::Storage,
            "native intent payload overflow",
        )
    };
    let fixed = intent
        .operation_directory
        .capacity()
        .checked_add(intent.original_fence.reservation_id.capacity())
        .and_then(|n| n.checked_add(intent.original_fence.placement.capacity()))
        .and_then(|n| n.checked_add(intent.manifest.options.root.capacity()))
        .and_then(|n| {
            intent
                .manifest
                .volumes
                .capacity()
                .checked_mul(std::mem::size_of::<NativeVolumeMapping>())
                .and_then(|slots| n.checked_add(slots))
        })
        .ok_or_else(overflow)? as u64;
    intent
        .manifest
        .volumes
        .iter()
        .try_fold(fixed, |bytes, mapping| {
            let volume = mapping.volume.logical_owned_bytes()?;
            let generation = mapping.generation.logical_owned_bytes()?;
            bytes
                .checked_add(mapping.path.capacity() as u64)
                .and_then(|n| n.checked_add(volume))
                .and_then(|n| n.checked_add(generation))
                .ok_or_else(overflow)
        })
}

pub(super) fn decode_record<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    maximum_slot: usize,
    work: &NativeMaterializationWork,
    retained_bytes: impl FnOnce(&T) -> Result<u64>,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let remaining = work.remaining()?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: bytes.len() as u64,
        maximum_items: remaining.items_examined,
        maximum_depth: 256,
    };
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let value = crate::contract::json_from_slice_bounded(
        std::marker::PhantomData::<T>,
        bytes,
        bounds,
        0,
        Some(crate::wire::ErrorCode::Storage),
        Some(crate::contract::JsonValueSchema::None),
        &mut |charge| {
            if let crate::contract::JsonCodecCharge::Admission {
                maximum_owned_bytes: scratch,
                value_shape,
            } = charge
            {
                let owned = value_shape
                    .lexical_items()
                    .checked_mul(10)
                    .and_then(|slots| slots.checked_mul(maximum_slot as u64))
                    .and_then(|slots| slots.checked_add(value_shape.input_bytes()))
                    .and_then(|owned| owned.checked_add(scratch))
                    .ok_or(acyclic_fs::WorkError::Overflow)
                    .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                return io.retain_owned(owned);
            }
            crate::store::history::HistoryIo::codec_charge(&mut &io, charge)
        },
    );
    match value {
        Ok(value) => {
            let custody = io.into_native_custody(retained_bytes(&value)?)?;
            Ok((value, custody))
        }
        Err(error) => {
            let held = io.owned_bytes()?;
            Err(native_codec_error_custody(
                error,
                io.into_native_custody(held)?,
            ))
        }
    }
}

/// Copies one admitted mapping while retaining its actual original-ledger storage.
pub fn copy_native_mapping_with_work(
    mapping: &NativeVolumeMapping,
    work: &NativeMaterializationWork,
) -> Result<(NativeVolumeMapping, acyclic_fs::NativeAllocation)> {
    allocate(work, |io| {
        let volume = mapping.volume.copy(&mut Some(&mut io.history_io()))?;
        let generation = mapping.generation.clone_with_io(io)?;
        let (path, custody) = work.path(&mapping.path, None)?;
        io.absorb_native_custody(custody)?;
        Ok(NativeVolumeMapping {
            volume,
            generation,
            path,
            writable: mapping.writable,
            namespace: mapping.namespace.clone(),
            authority_revision: mapping.authority_revision,
        })
    })
}
