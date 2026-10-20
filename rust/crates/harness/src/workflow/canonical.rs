use super::*;
use crate::conversation::{FileDescriptor, VolumeOwner, VolumeRef};
use crate::resources::ProviderRef;
use serde::{
    Serializer,
    ser::{SerializeMap, SerializeSeq},
};

// These DTOs have closed, fixed keys. Borrow them in canonical order instead
// of encoding, parsing and copying a second owned tree just to compute a hash.
pub(super) struct Canonical<'a, T: ?Sized>(pub(super) &'a T);

macro_rules! object {
    ($ty:ty, $count:expr, $value:ident, {$($key:literal => $field:expr),+ $(,)?}) => {
        impl Serialize for Canonical<'_, $ty> {
            fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                let $value = self.0;
                let mut object = serializer.serialize_map(Some($count))?;
                $(object.serialize_entry($key, &$field)?;)+
                object.end()
            }
        }
    };
}

object!(WorkflowRecord, 5, value, {
    "idempotency_key" => value.idempotency_key,
    "input" => value.input,
    "operation_id" => value.operation_id,
    "prior" => Canonical(&value.prior),
    "transition" => Canonical(&value.transition),
});
object!(MachineCheckpoint, 3, value, {
    "machine" => Canonical(&value.machine),
    "revision" => value.revision,
    "state" => value.state,
});
object!(MachineIdentity, 3, value, {
    "digest" => value.digest,
    "name" => value.name,
    "version" => value.version,
});
object!(MachineTransition, 3, value, {
    "commands" => Canonical(value.commands.as_slice()),
    "state" => value.state,
    "status" => value.status,
});
object!(WorkflowCommand, 3, value, {
    "kind" => value.kind,
    "operation_id" => value.operation_id,
    "payload" => Canonical(&value.payload),
});
object!(FileRef, 5, value, {
    "descriptor" => Canonical(value.descriptor()),
    "display_name" => value.display_name(),
    "path" => value.path(),
    "version" => value.version(),
    "volume" => Canonical(value.volume()),
});
object!(FileDescriptor, 3, value, {
    "byte_length" => value.byte_length(),
    "media_type" => value.media_type(),
    "sha256" => value.sha256(),
});
object!(VolumeRef, 4, value, {
    "class" => value.class(),
    "id" => value.id(),
    "owner" => Canonical(value.owner()),
    "provider" => Canonical(value.provider()),
});
object!(ProviderRef, 3, value, {
    "family" => value.family(),
    "namespace" => value.namespace(),
    "version" => value.version(),
});

impl Serialize for Canonical<'_, VolumeOwner> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut object = serializer.serialize_map(Some(2))?;
        let kind = match self.0 {
            VolumeOwner::Project(id) => {
                object.serialize_entry("id", id)?;
                "project"
            }
            VolumeOwner::Agent(id) => {
                object.serialize_entry("id", id)?;
                "agent"
            }
            VolumeOwner::Session(id) => {
                object.serialize_entry("id", id)?;
                "session"
            }
        };
        object.serialize_entry("kind", kind)?;
        object.end()
    }
}

impl Serialize for Canonical<'_, [WorkflowCommand]> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for command in self.0 {
            sequence.serialize_element(&Canonical(command))?;
        }
        sequence.end()
    }
}

// serde_json normally stores sorted maps, but a downstream preserve_order
// feature may change their iteration order. Only use the borrowed fast path
// after checking every nested object on the original work ledger. Unsorted
// values continue through the generic canonical sorter.
pub(super) fn ordered(record: &WorkflowRecord, io: &PreparationIo) -> Result<bool> {
    fn value_ordered(value: &Value, depth: u32, io: &PreparationIo) -> Result<bool> {
        io.check_cancellation()?;
        io.codec_charge(
            crate::executor::json::JsonCodecCharge::Inspect { items: 1 },
            true,
        )?;
        if depth >= 256 {
            return Ok(false);
        }
        match value {
            Value::Object(fields) => {
                let mut previous = None;
                for (key, value) in fields {
                    if previous.is_some_and(|previous| previous >= key.as_str()) {
                        return Ok(false);
                    }
                    previous = Some(key.as_str());
                    if !value_ordered(value, depth + 1, io)? {
                        return Ok(false);
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    if !value_ordered(value, depth + 1, io)? {
                        return Ok(false);
                    }
                }
            }
            _ => {}
        }
        Ok(true)
    }
    Ok(value_ordered(&record.input, 0, io)?
        && value_ordered(&record.prior.state, 0, io)?
        && value_ordered(&record.transition.state, 0, io)?
        && match &record.transition.status {
            MachineStatus::Completed { value } => value_ordered(value, 0, io)?,
            _ => true,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentId, conversation::VolumeClass};
    use serde_json::json;

    #[test]
    fn workflow_digest_borrows_canonical_records_without_copying() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
        budget.bytes_copied = 0;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let mut hashed = 0;
        let mut last = None;
        for owner in [
            VolumeOwner::Project("project".into()),
            VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
            VolumeOwner::Session("session".into()),
        ] {
            let payload = FileRef::new(
                VolumeRef::new(
                    ProviderRef::new(
                        "workflow-test".into(),
                        "filesystem".into(),
                        "2".into(),
                        &mut None,
                    )?,
                    "private".into(),
                    VolumeClass::AgentPrivate,
                    owner,
                    &mut None,
                )?,
                "commands/first.json".into(),
                "immutable-version".into(),
                FileDescriptor::from_bytes(b"{}", "application/json", &mut None)?,
                "first.json".into(),
                &mut None,
            )?;
            let mut record = WorkflowRecord {
                operation_id: OperationId::from_bytes([1; 16]),
                idempotency_key: IdempotencyKey::new("canonical-record")?,
                prior: MachineCheckpoint {
                    machine: MachineIdentity {
                        name: "test.canonical".into(),
                        version: "1".into(),
                        digest: [3; 32],
                    },
                    revision: 12,
                    state: json!({"z": [true, null, 1.25], "a": "quoted\"\\\n"}),
                },
                input: json!({"z": {"b": -7, "a": "unicode λ"}, "a": "q".repeat(128 * 1024)}),
                transition: MachineTransition {
                    state: json!({"reply": "q".repeat(128 * 1024)}),
                    commands: vec![WorkflowCommand {
                        operation_id: OperationId::from_bytes([2; 16]),
                        kind: "test.publish".into(),
                        payload,
                    }],
                    status: MachineStatus::Suspended,
                },
            };
            record.input.sort_all_objects();
            record.prior.state.sort_all_objects();
            record.transition.state.sort_all_objects();
            for mut status in [
                MachineStatus::Suspended,
                MachineStatus::Completed {
                    value: json!({"z": [0, {"b": 2, "a": 1}], "a": "complete"}),
                },
                MachineStatus::Failed {
                    message: "quoted\" failure\n".into(),
                },
            ] {
                if let MachineStatus::Completed { value } = &mut status {
                    value.sort_all_objects();
                }
                record.transition.status = status;
                let canonical = crate::contract::canonical_json_bytes(&record)?;
                let borrowed = serde_json::to_vec(&Canonical(&record))
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert_eq!(borrowed, canonical);
                assert!(ordered(&record, &io)?);
                assert_eq!(
                    record.digest(Some(&io))?,
                    *blake3::hash(&canonical).as_bytes()
                );
                hashed += canonical.len() as u64;
            }
            last = Some(record);
        }
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(work.receipt().bytes_hashed, hashed);
        assert!(work.receipt().items_examined > 0);
        let mut record = last.expect("canonical fixture");
        // With serde_json/preserve_order this takes the generic sorter; with
        // the default map it exercises the same record through the fast path.
        record.input = json!({"z": {"b": 2, "a": 1}, "a": 0});
        let fallback = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let fallback_io = PreparationIo::from_native_work(&fallback)?;
        let uses_borrowed = ordered(&record, &fallback_io)?;
        let expected = crate::contract::canonical_json_bytes(&record)?;
        assert_eq!(
            record.digest(Some(&fallback_io))?,
            *blake3::hash(&expected).as_bytes()
        );
        if !uses_borrowed {
            assert!(fallback.receipt().bytes_copied > 0);
        }
        cancellation.cancel();
        assert_eq!(
            record.digest(Some(&io)).unwrap_err().code(),
            crate::wire::ErrorCode::Cancelled
        );
        Ok(())
    }
}
