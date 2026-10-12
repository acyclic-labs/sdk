use super::*;

/// Replay-safe host for pinned machines and their atomic journals.
pub struct DurableWorkflowHost {
    registry: MachineRegistry,
    checkpoint: MachineCheckpoint,
    journal: Arc<dyn WorkflowJournal>,
    intents: BTreeMap<OperationId, (u64, [u8; 32])>,
    reserved_operations: BTreeSet<OperationId>,
    terminal: bool,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    checkpoint_custody: NativeCustody,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    index_custody: NativeCustody,
}
impl DurableWorkflowHost {
    /// Restores and verifies every committed transition.
    pub async fn open(
        registry: MachineRegistry,
        initial: MachineCheckpoint,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<Self> {
        Self::recover(registry, initial, journal, None).await
    }
    /// Restores native machine and journal callbacks on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn open_with_native_work(
        registry: MachineRegistry,
        initial: MachineCheckpoint,
        journal: Arc<dyn WorkflowJournal>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        registry.require_native_work(&initial.machine)?;
        if !journal.supports_native_work() {
            return Err(unsupported("journal has no native workflow callbacks"));
        }
        let io = PreparationIo::from_native_work(work)?;
        let result = Self::recover(registry, initial, journal, Some(&io)).await;
        Ok(finish_native_io(result, io, 0)?.0)
    }
    async fn recover(
        registry: MachineRegistry,
        initial: MachineCheckpoint,
        journal: Arc<dyn WorkflowJournal>,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let mut observed = journal::admission(journal.as_ref(), io).await?;
        let admission = observed
            .0
            .take()
            .ok_or_else(|| error(io, Conflict, "workflow has no retained admission"))?;
        admission.validate(io)?;
        if admission.initial != initial {
            return Err(error(
                io,
                Conflict,
                "workflow initial checkpoint differs from admission",
            ));
        }
        registry.validate_checkpoint(&initial, io)?;
        let mut host = Self {
            registry,
            checkpoint: if io.is_some() {
                admission.initial
            } else {
                initial
            },
            journal,
            intents: BTreeMap::new(),
            reserved_operations: BTreeSet::new(),
            terminal: false,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            checkpoint_custody: observed.1.take(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            index_custody: None,
        };
        let mut keys = BTreeSet::new();
        loop {
            let page = journal::page(
                host.journal.as_ref(),
                host.checkpoint.revision,
                WORKFLOW_REPLAY_PAGE_RECORDS,
                io,
            )
            .await?;
            if page.0.len() > WORKFLOW_REPLAY_PAGE_RECORDS as usize {
                return Err(error(io, Storage, "workflow provider exceeded page bound"));
            }
            if page.0.is_empty() {
                break;
            }
            for record in page.0 {
                validate_workflow_next(
                    &host.checkpoint,
                    host.terminal,
                    &host.reserved_operations,
                    &keys,
                    &record,
                    io,
                )?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(io) = io {
                    host.reserve_native_index(record.transition.commands.len(), io)?;
                }
                keys.insert(IdempotencyKey(crate::executor::stock_string_copy(
                    &record.idempotency_key.0,
                    io,
                )?));
                host.reserved_operations.extend(record.operation_ids());
                host.intents.insert(
                    record.operation_id,
                    (
                        record.prior.revision,
                        crate::executor::preparation_digest(&record, io)?,
                    ),
                );
                let restored = host
                    .registry
                    .replayed_checkpoint(&host.checkpoint, &record, io)?;
                host.checkpoint = restored.0;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                {
                    host.checkpoint_custody = restored.1;
                }
                host.terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
            }
            // Page payloads drop before custody and the next read.
        }
        Ok(host)
    }
    /// Latest committed checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        &self.checkpoint
    }
    /// Reads the latest outbox and status without retaining history.
    pub async fn latest_transition(&self) -> Result<Option<MachineTransition>> {
        Ok(self.latest(None).await?.0)
    }
    /// Returns the actual latest transition with its native custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn latest_transition_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<MachineTransition>, acyclic_fs::NativeAllocation)> {
        self.require_original_native_work(work)?;
        let io = PreparationIo::from_native_work(work)?;
        let result = self.latest(Some(&io)).await;
        let (mut latest, mut custody) = finish_native_io(result, io, 0)?;
        if let Some(mut incoming) = latest.1.take() {
            custody
                .absorb(&mut incoming)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: Some(incoming),
                })?;
        }
        Ok((latest.0, custody))
    }
    async fn latest(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<(Option<MachineTransition>, NativeCustody)> {
        if self.checkpoint.revision == 0 {
            return Ok((None, None));
        }
        let mut page =
            journal::page(self.journal.as_ref(), self.checkpoint.revision - 1, 1, io).await?;
        let [record] = page.0.as_slice() else {
            return Err(error(io, Storage, "latest workflow record is unavailable"));
        };
        if record.next != self.checkpoint
            || self.intents.get(&record.operation_id)
                != Some(&(
                    record.prior.revision,
                    crate::executor::preparation_digest(record, io)?,
                ))
        {
            return Err(error(io, Storage, "latest workflow record changed"));
        }
        Ok((
            Some(
                page.0
                    .pop()
                    .expect("checked single workflow record")
                    .transition,
            ),
            page.1.take(),
        ))
    }
    /// Validates a proposed transition before committing its checkpoint and outbox.
    pub async fn step(
        &mut self,
        operation_id: OperationId,
        key: IdempotencyKey,
        input: Value,
        check: impl FnOnce(&MachineTransition) -> Result<()>,
    ) -> Result<MachineTransition> {
        self.advance(operation_id, key, input, check, None)
            .await
            .map(|(transition, _, _)| transition)
    }
    /// Rejects native-incompatible composition before provider IO.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn require_native_work(&self) -> Result<()> {
        self.registry
            .require_native_work(&self.checkpoint.machine)?;
        if !self.journal.supports_native_work() {
            return Err(unsupported("journal has no native workflow callbacks"));
        }
        Ok(())
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_original_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_native_work()?;
        let held = self
            .checkpoint_custody
            .as_ref()
            .ok_or_else(|| unsupported("workflow was not opened with native custody"))?;
        if !held.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "workflow checkpoint belongs to another native ledger",
            });
        }
        Ok(())
    }
    /// Executes the checked transition on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn step_checked_with_native_work(
        &mut self,
        operation_id: OperationId,
        key: IdempotencyKey,
        input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
        check: impl FnOnce(&MachineTransition) -> Result<()>,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        self.require_original_native_work(work)?;
        let io = PreparationIo::from_native_work(work)?;
        let original_index_bytes = self
            .index_custody
            .as_ref()
            .map(acyclic_fs::NativeAllocation::bytes);
        let result = self
            .advance(operation_id, key, input, check, Some(&io))
            .await;
        if result.is_err() {
            match (self.index_custody.as_mut(), original_index_bytes) {
                (Some(previous), Some(bytes)) => {
                    let _ = previous
                        .bytes()
                        .checked_sub(bytes)
                        .ok_or(acyclic_fs::WorkError::Overflow.into())
                        .and_then(|bytes| previous.release(bytes));
                }
                _ => self.index_custody = None,
            }
        }
        let retained = result.as_ref().map_or(0, |(_, checkpoint, transition)| {
            checkpoint.saturating_add(*transition)
        });
        let ((transition, checkpoint_retained, _), mut custody) =
            finish_native_io(result, io, retained)?;
        if checkpoint_retained != 0 {
            self.checkpoint_custody =
                Some(custody.split_off(checkpoint_retained).map_err(|cause| {
                    Error::NativeWork {
                        cause,
                        custody: None,
                    }
                })?);
        }
        Ok((transition, custody))
    }
    async fn advance(
        &mut self,
        operation_id: OperationId,
        key: IdempotencyKey,
        input: Value,
        check: impl FnOnce(&MachineTransition) -> Result<()>,
        io: Option<&PreparationIo>,
    ) -> Result<(MachineTransition, u64, u64)> {
        validate_key(&key, io)?;
        if let Some((revision, digest)) = self.intents.get(&operation_id) {
            let page = journal::page(self.journal.as_ref(), *revision, 1, io).await?;
            let [record] = page.0.as_slice() else {
                return Err(error(io, Storage, "workflow retry record is unavailable"));
            };
            if crate::executor::preparation_digest(record, io)? != *digest {
                return Err(error(io, Storage, "workflow retry record changed"));
            }
            if record.idempotency_key != key || record.input != input {
                return Err(error(
                    io,
                    Conflict,
                    "workflow operation identity is bound to another input",
                ));
            }
            check(&record.transition)?;
            let before = workflow_owned_bytes(io)?;
            let transition = record.transition.copy(io)?;
            return Ok((transition, 0, workflow_owned_bytes(io)? - before));
        }
        if self.terminal {
            return Err(error(
                io,
                Conflict,
                "workflow cannot advance after a terminal transition",
            ));
        }
        if self.reserved_operations.contains(&operation_id) {
            return Err(error(
                io,
                Conflict,
                "workflow step reuses a command identity",
            ));
        }
        let retained_input;
        let machine_input =
            if cfg!(all(feature = "filesystem", not(target_arch = "wasm32"))) && io.is_some() {
                retained_input = crate::model::clone_value(&input, io)?;
                Cow::Owned(input)
            } else {
                retained_input = input;
                Cow::Borrowed(&retained_input)
            };
        let (next, transition, checkpoint_retained, transition_retained) =
            self.registry.step(&self.checkpoint, machine_input, io)?;
        let input = retained_input;
        check(&transition)?;
        let record = WorkflowRecord {
            operation_id,
            idempotency_key: IdempotencyKey(crate::executor::stock_string_copy(&key.0, io)?),
            input_digest: crate::executor::preparation_digest(&input, io)?,
            prior: self.checkpoint.copy(io)?,
            input,
            transition: transition.copy(io)?,
            next: next.copy(io)?,
        };
        record.validate(io)?;
        if record
            .transition
            .commands
            .iter()
            .any(|command| self.reserved_operations.contains(&command.operation_id))
        {
            return Err(error(
                io,
                Conflict,
                "workflow command identity was already committed",
            ));
        }
        let digest = if io.is_some() {
            Some(crate::executor::preparation_digest(&record, io)?)
        } else {
            None
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            self.reserve_native_index(record.transition.commands.len(), io)?;
        }
        let committed_record = record.copy(io)?;
        let commit_key = IdempotencyKey(crate::executor::stock_string_copy(&key.0, io)?);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let observed = if let Some(io) = io {
            let work = io
                .original_native_work()
                .ok_or_else(|| unsupported("native commit requires the original ledger"))?;
            let (observed, custody) = self
                .journal
                .commit_with_native_work(
                    self.checkpoint.revision,
                    commit_key,
                    committed_record,
                    work,
                )?
                .await?;
            io.absorb_native_custody(custody)?;
            observed
        } else {
            self.journal
                .commit(self.checkpoint.revision, commit_key, committed_record)
                .await?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let observed = self
            .journal
            .commit(self.checkpoint.revision, commit_key, committed_record)
            .await?;
        let committed = observed;
        if committed != record {
            return Err(error(
                io,
                Conflict,
                "workflow retry identity is bound to another transition",
            ));
        }
        self.checkpoint = next;
        self.terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
        self.reserved_operations.extend(record.operation_ids());
        self.intents.insert(
            operation_id,
            (
                record.prior.revision,
                match digest {
                    Some(digest) => digest,
                    None => crate::contract::canonical_json_digest(&record, u64::MAX, &mut None)?,
                },
            ),
        );
        Ok((transition, checkpoint_retained, transition_retained))
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reserve_native_index(&mut self, commands: usize, io: &PreparationIo) -> Result<()> {
        let work = io
            .original_native_work()
            .ok_or_else(|| unsupported("native index requires original work"))?;
        let bytes = (commands as u64)
            .checked_add(1)
            .and_then(|count| count.checked_mul(std::mem::size_of::<OperationId>() as u64))
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<(OperationId, (u64, [u8; 32]))>() as u64)
            })
            .ok_or(Error::NativeBoundary {
                code: Invalid,
                reason: "native workflow index slot overflow",
            })?;
        let mut incoming = work.claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        if let Some(previous) = &mut self.index_custody {
            previous
                .absorb(&mut incoming)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: Some(incoming),
                })?;
        } else {
            self.index_custody = Some(incoming);
        }
        Ok(())
    }
}
