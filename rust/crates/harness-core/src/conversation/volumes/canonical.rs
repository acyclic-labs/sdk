//! Closed volume identity fields serialized by reference in canonical key order.
use super::*;
use serde::{Serializer, ser::SerializeMap};

pub(in crate::conversation) struct Canonical<'a, T: ?Sized>(pub(in crate::conversation) &'a T);

impl Serialize for Canonical<'_, VolumeRef> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_map(Some(4))?;
        fields.serialize_entry("class", &self.0.class)?;
        fields.serialize_entry("id", &self.0.id)?;
        fields.serialize_entry("owner", &Canonical(&self.0.owner))?;
        fields.serialize_entry("provider", &Canonical(&self.0.provider))?;
        fields.end()
    }
}

impl Serialize for Canonical<'_, ProviderRef> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_map(Some(3))?;
        fields.serialize_entry("family", self.0.family())?;
        fields.serialize_entry("namespace", self.0.namespace())?;
        fields.serialize_entry("version", self.0.version())?;
        fields.end()
    }
}

impl Serialize for Canonical<'_, VolumeOwner> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_map(Some(2))?;
        let kind = match self.0 {
            VolumeOwner::Project(id) => {
                fields.serialize_entry("id", id)?;
                "project"
            }
            VolumeOwner::Agent(id) => {
                fields.serialize_entry("id", id)?;
                "agent"
            }
            VolumeOwner::Session(id) => {
                fields.serialize_entry("id", id)?;
                "session"
            }
        };
        fields.serialize_entry("kind", kind)?;
        fields.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn volumes() -> Result<Vec<VolumeRef>> {
        let mut values = Vec::new();
        for class in [
            VolumeClass::Project,
            VolumeClass::AgentPrivate,
            VolumeClass::SessionShared,
        ] {
            for owner in [
                VolumeOwner::Project("project".into()),
                VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
                VolumeOwner::Session("session".into()),
            ] {
                values.push(VolumeRef::new(
                    ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
                    "scratch".into(),
                    class,
                    owner,
                    &mut None,
                )?);
            }
        }
        Ok(values)
    }

    fn previous_labels(volume: &VolumeRef) -> Result<[String; 4]> {
        let digest = crate::executor::json::canonical_json_digest(volume, u64::MAX, &mut None)?;
        let directory =
            crate::executor::json::canonical_json_digest(&(volume, ""), u64::MAX, &mut None)?;
        Ok([
            content_label("harness-", digest, &mut None)?,
            content_label(
                acyclic_fs::access::DIRECTORY_READ_PREFIX,
                directory,
                &mut None,
            )?,
            content_label(VolumeOperation::Read.capability_prefix(), digest, &mut None)?,
            content_label(
                VolumeOperation::Write.capability_prefix(),
                digest,
                &mut None,
            )?,
        ])
    }

    fn labels(
        volume: &VolumeRef,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<[String; 4]> {
        Ok([
            volume.storage_name(work)?,
            volume.directory_read_capability("", work)?,
            volume.capability(VolumeOperation::Read, work)?,
            volume.capability(VolumeOperation::Write, work)?,
        ])
    }

    #[test]
    fn volume_labels_borrow_all_closed_identity_fields() -> Result<()> {
        for mut volume in volumes()? {
            // Preserve escaped and Unicode strings without a generic Value tree.
            volume.id.push_str("-\"\\\n\u{03b1}");
            assert_eq!(
                serde_json::to_vec(&Canonical(&volume)).unwrap(),
                crate::contract::canonical_json_bytes(&volume)?
            );
            for prefix in ["", "nested/path"] {
                let bytes = crate::contract::canonical_json_bytes(&(&volume, prefix))?;
                let expected = content_label(
                    acyclic_fs::access::DIRECTORY_READ_PREFIX,
                    *blake3::hash(&bytes).as_bytes(),
                    &mut None,
                )?;
                assert_eq!(
                    volume.directory_read_capability(prefix, &mut None)?,
                    expected
                );
            }
            let mut old_labels = None;
            let old = acyclic_test_support::measure(|| old_labels = Some(previous_labels(&volume)));
            let mut new_labels = None;
            let new =
                acyclic_test_support::measure(|| new_labels = Some(labels(&volume, &mut None)));
            assert_eq!(new_labels.unwrap()?, old_labels.unwrap()?);
            eprintln!(
                "VOLUME_LABEL_ALLOCS new={} old={}",
                new.count_total, old.count_total
            );
            assert!(
                new.count_total * 2 < old.count_total,
                "volume identity rebuilt owned JSON: new={new:?}, old={old:?}"
            );
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn restored_owned_volume_hash_exceeds_the_same_copy_allowance() -> Result<()> {
        use acyclic_fs::{
            CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError,
        };
        for volume in volumes()? {
            let expected = previous_labels(&volume)?[0].clone();
            let cancellation = CancellationToken::new();
            let mut budget = acyclic_fs::performance::work_budget(10_000, 1_048_576);
            budget.bytes_copied = expected.len() as u64;
            let positive = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut positive_io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&positive)?,
            );
            assert_eq!(volume.storage_name(&mut Some(&mut positive_io))?, expected);
            let restored = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut restored_io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&restored)?,
            );
            // This is the production hashing call used before the borrowed path.
            // Its temporary encoded document already exceeds the label budget.
            let restored_result = crate::executor::json::canonical_json_digest(
                &volume,
                crate::store::default_projection_read_limits().maximum_bytes,
                &mut Some(&mut restored_io),
            );
            assert!(
                matches!(
                    &restored_result,
                    Err(Error::NativeFilesystem { source, .. })
                        if matches!(source.as_ref(),
                            acyclic_fs::FsError::NativeKernel(
                                NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                                    counter: "bytes_copied",
                                    ..
                                })))
                ),
                "restored result: {restored_result:?}; receipt: {:?}",
                restored.receipt()
            );
            assert_eq!(restored.receipt().bytes_hashed, 0);
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_volume_labels_only_copy_the_returned_labels() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        for volume in volumes()? {
            let expected = previous_labels(&volume)?;
            let cancellation = CancellationToken::new();
            let mut budget = acyclic_fs::performance::work_budget(10_000, 1_048_576);
            budget.bytes_copied = expected.iter().map(|label| label.len() as u64).sum();
            let work = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&work)?,
            );
            assert_eq!(labels(&volume, &mut Some(&mut io))?, expected);
            let bytes = 3 * crate::contract::canonical_json_bytes(&volume)?.len()
                + crate::contract::canonical_json_bytes(&(&volume, ""))?.len();
            assert_eq!(work.receipt().bytes_hashed, bytes as u64);
            assert_eq!(work.receipt().bytes_copied, budget.bytes_copied);
            cancellation.cancel();
            assert!(volume.storage_name(&mut Some(&mut io)).is_err());
            assert!(
                volume
                    .directory_read_capability("", &mut Some(&mut io))
                    .is_err()
            );
            assert!(
                volume
                    .capability(VolumeOperation::Read, &mut Some(&mut io))
                    .is_err()
            );
            assert!(
                volume
                    .capability(VolumeOperation::Write, &mut Some(&mut io))
                    .is_err()
            );

            let cancellation = CancellationToken::new();
            budget.bytes_hashed = 0;
            let denied = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut denied_io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&denied)?,
            );
            assert!(volume.storage_name(&mut Some(&mut denied_io)).is_err());
            assert!(
                volume
                    .directory_read_capability("", &mut Some(&mut denied_io))
                    .is_err()
            );
            assert!(
                volume
                    .capability(VolumeOperation::Read, &mut Some(&mut denied_io))
                    .is_err()
            );
            assert!(
                volume
                    .capability(VolumeOperation::Write, &mut Some(&mut denied_io))
                    .is_err()
            );
            assert_eq!(denied.receipt().bytes_hashed, 0);
        }
        Ok(())
    }
}
