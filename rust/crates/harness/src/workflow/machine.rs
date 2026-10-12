use super::*;
use records::validate_commands;

/// Exact identity of a resumable implementation and its schemas.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineIdentity {
    /// Namespaced implementation name.
    pub name: String,
    /// Implementation version.
    pub version: String,
    /// Nonzero implementation and schema digest.
    pub digest: [u8; 32],
}
impl MachineIdentity {
    /// Validates pinned implementation metadata.
    pub fn validate(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        for label in [&self.name, &self.version] {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            }
            if !crate::contract::is_valid_component_label(label) {
                return Err(crate::scheduler::diagnostic(work, Invalid, "machine component label is invalid"));
            }
        }
        if self.digest == [0; 32] {
            return Err(crate::scheduler::diagnostic(work, Invalid, "machine implementation digest must be a nonzero 32-byte value"));
        }
        Ok(())
    }
}

/// Synchronous deterministic machine behind a pinned registry entry.
pub trait ResumableMachine: acyclic_stream::ProviderPlatform {
    /// Immutable implementation identity.
    fn identity(&self) -> &MachineIdentity;
    /// Durable state schema.
    fn state_schema(&self) -> &Value;
    /// Initial durable state.
    fn initialize(&self, input: &Value) -> Result<Value>;
    /// Applies one admitted input.
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition>;
    /// Whether original-ledger callbacks are supported.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_work(&self) -> bool { false }
    /// Borrows metadata through the native callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work<'a>(&'a self, _: &acyclic_fs::NativeMaterializationWork) -> Result<&'a MachineIdentity> {
        Err(unsupported("machine has no native identity callback"))
    }
    /// Constructs initial state with original-ledger custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn initialize_with_native_work(&self, _: &Value, _: &acyclic_fs::NativeMaterializationWork)
        -> Result<(Value, acyclic_fs::NativeAllocation)> {
        Err(unsupported("machine has no native initialization callback"))
    }
    /// Consumes admitted input and returns guarded native state.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn transition_with_native_work(&self, _: &Value, _: Value, _: &acyclic_fs::NativeMaterializationWork)
        -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        Err(unsupported("machine has no native state callback"))
    }
}

/// Registry of exactly pinned machine implementations.
#[derive(Clone, Default)]
pub struct MachineRegistry(BTreeMap<(String, String, [u8; 32]), Arc<dyn ResumableMachine>>);
impl MachineRegistry {
    /// Copies key metadata on the original ledger, sharing implementations.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn copy(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut observer = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let bytes = (self.0.len() as u64)
            .checked_mul(
                std::mem::size_of::<((String, String, [u8; 32]), Arc<dyn ResumableMachine>)>()
                    as u64,
            )
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() as u64))
            .ok_or(Error::NativeBoundary {
                code: Invalid,
                reason: "registry metadata ownership overflows",
            })?;
        let mut observed = Some(&mut observer as &mut dyn crate::store::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, bytes)?;
        let mut copied = BTreeMap::new();
        for ((name, version, digest), machine) in &self.0 {
            let name = crate::core::copy_text(name, &mut quota)?;
            let version = crate::core::copy_text(version, &mut quota)?;
            copied.insert((name, version, *digest), Arc::clone(machine));
        }
        work.remaining()
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        quota.transfer();
        drop(quota);
        Ok((Self(copied), observer.into_custody()?))
    }

    /// Registers a validated exact implementation.
    pub fn register(&mut self, machine: Arc<dyn ResumableMachine>, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() && !machine.supports_native_work() { return Err(unsupported("machine has no native state callbacks")); }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let identity = if let Some(observer) = work.as_deref() {
            let native = observer.original_native_work().ok_or_else(|| unsupported("native machine metadata requires original work"))?;
            machine.identity_with_native_work(native)?
        } else { machine.identity() };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let identity = machine.identity();
        identity.validate(work)?;
        crate::contract::compile_json_schema_with_work(machine.state_schema(), "machine state", work)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let key = (crate::core::copy_text(&identity.name, &mut quota)?, crate::core::copy_text(&identity.version, &mut quota)?, identity.digest);
        if self.0.contains_key(&key) {
            return Err(crate::scheduler::diagnostic(quota.work(), Conflict, "machine identity is already registered"));
        }
        self.0.insert(key, machine);
        quota.transfer();
        Ok(())
    }

    /// Registers through the native engine and retains key custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn register_with_native_work(&mut self, machine: Arc<dyn ResumableMachine>, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<acyclic_fs::NativeAllocation> {
        let io = PreparationIo::from_native_work(work)?;
        let result = with_history_io(Some(&io), |work| self.register(machine, work));
        let retained = io.owned_bytes()?;
        Ok(finish_native_io(result, io, retained)?.1)
    }

    pub(crate) fn resolve(&self, identity: &MachineIdentity, work: &mut Option<&mut dyn crate::store::HistoryIo>)
        -> Result<Option<&Arc<dyn ResumableMachine>>> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Work {
                bytes: identity.name.len() as u64 + identity.version.len() as u64 + 32, items: self.0.len() as u64,
            })?;
        }
        Ok(self.0.iter().find_map(|((name, version, digest), machine)|
            (name == &identity.name && version == &identity.version && digest == &identity.digest).then_some(machine)))
    }

    fn machine(&self, identity: &MachineIdentity, native: bool) -> Result<&Arc<dyn ResumableMachine>> {
        let machine = self.resolve(identity, &mut None)?.ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native { return unsupported("pinned native machine is unavailable"); }
            Error::Unsupported("pinned machine implementation is unavailable".into())
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if native && !machine.supports_native_work() { return Err(unsupported("machine has no native state callback")); }
        let _ = native;
        Ok(machine)
    }

    /// Verifies native capability before provider IO.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn require_native_work(&self, identity: &MachineIdentity) -> Result<()> { self.machine(identity, true).map(|_| ()) }

    /// Validates the pinned checkpoint schema.
    pub(crate) fn validate_checkpoint(&self, checkpoint: &MachineCheckpoint, io: Option<&PreparationIo>) -> Result<()> {
        let machine = self.machine(&checkpoint.machine, cfg!(all(feature = "filesystem", not(target_arch = "wasm32"))) && io.is_some())?;
        crate::contract::validate_json_schema_value(machine.state_schema(), &checkpoint.state, "machine state", io)
    }

    /// Validates the checkpoint on its original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_checkpoint_with_native_work(&self, checkpoint: &MachineCheckpoint, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        let io = PreparationIo::from_native_work(work)?;
        let result = self.validate_checkpoint(checkpoint, Some(&io));
        let _ = finish_native_io(result, io, 0)?;
        Ok(())
    }

    /// Initializes schema-checked state through the native callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn initialize_with_native_work(&self, identity: &MachineIdentity, input: &Value, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let machine = self.machine(identity, true)?;
        let io = PreparationIo::from_native_work(work)?;
        let result = (|| {
            let (state, custody) = machine.initialize_with_native_work(input, work)?;
            let retained = custody.bytes();
            io.absorb_native_custody(custody)?;
            crate::contract::validate_json_schema_value(machine.state_schema(), &state, "machine state", Some(&io))?;
            Ok((state, retained))
        })();
        let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
        let ((state, _), custody) = finish_native_io(result, io, retained)?;
        Ok((state, custody))
    }

    /// Applies one deterministic input and returns its checkpoint and transition.
    pub fn step(&self, checkpoint: &MachineCheckpoint, input: &Value) -> Result<(MachineCheckpoint, MachineTransition)> {
        self.step_with_io(checkpoint, Cow::Borrowed(input), None).map(|(next, transition, _, _)| (next, transition))
    }

    /// Consumes native input and returns the paid checkpoint and transition.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn step_with_native_work(&self, checkpoint: &MachineCheckpoint, input: Value, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<(MachineCheckpoint, MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let result = self.step_with_io(checkpoint, Cow::Owned(input), Some(&io));
        let retained = result.as_ref().map_or(0, |(_, _, checkpoint, transition)| checkpoint.saturating_add(*transition));
        let ((next, transition, _, _), custody) = finish_native_io(result, io, retained)?;
        Ok((next, transition, custody))
    }

    pub(super) fn step_with_io(&self, checkpoint: &MachineCheckpoint, input: Cow<'_, Value>, io: Option<&PreparationIo>)
        -> Result<(MachineCheckpoint, MachineTransition, u64, u64)> {
        let machine = self.machine(&checkpoint.machine, cfg!(all(feature = "filesystem", not(target_arch = "wasm32"))) && io.is_some())?;
        crate::contract::validate_json_schema_value(machine.state_schema(), &checkpoint.state, "machine state", io)?;
        #[allow(unused_mut)]
        let mut transition_retained = 0;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let transition = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| unsupported("native state requires the original ledger"))?;
            let Cow::Owned(input) = input else { return Err(unsupported("native state requires owned input")); };
            let (transition, custody) = machine.transition_with_native_work(&checkpoint.state, input, work)?;
            transition_retained = custody.bytes();
            io.absorb_native_custody(custody)?;
            transition
        } else { machine.transition(&checkpoint.state, &input)? };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let transition = machine.transition(&checkpoint.state, &input)?;
        crate::contract::validate_json_schema_value(machine.state_schema(), &transition.state, "machine state", io)?;
        validate_commands(&transition.commands, io)?;
        let before = workflow_owned_bytes(io)?;
        let next = MachineCheckpoint { machine: workflow_copy_identity(&checkpoint.machine, io)?, revision: next_revision(checkpoint.revision, io)?,
            state: crate::model::clone_value(&transition.state, io)? };
        Ok((next, transition, workflow_owned_bytes(io)? - before, transition_retained))
    }

    pub(super) fn replayed_checkpoint(&self, checkpoint: &MachineCheckpoint, record: &WorkflowRecord, io: Option<&PreparationIo>)
        -> Result<(MachineCheckpoint, NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let proof_io = io.map(|io| PreparationIo::from_native_work(io.original_native_work()
            .ok_or_else(|| unsupported("native replay requires original work"))?)).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = proof_io.as_ref().or(io);
        let result = (|| {
            let input = if cfg!(all(feature = "filesystem", not(target_arch = "wasm32"))) && io.is_some() {
                Cow::Owned(crate::model::clone_value(&record.input, io)?)
            } else { Cow::Borrowed(&record.input) };
            let (next, transition, retained, _) = self.step_with_io(checkpoint, input, io)?;
            if next != record.next || transition != record.transition {
                return Err(error(io, Conflict, "workflow history disagrees with its pinned machine"));
            }
            drop(transition);
            Ok((next, retained))
        })();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(proof_io) = proof_io {
            let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
            let ((checkpoint, _), custody) = finish_native_io(result, proof_io, retained)?;
            return Ok((checkpoint, Some(custody)));
        }
        Ok((result?.0, None))
    }
}
