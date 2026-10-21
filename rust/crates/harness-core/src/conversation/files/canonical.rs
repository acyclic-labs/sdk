//! Closed immutable file identity in canonical field order.
use super::*;
use serde::{Serializer, ser::SerializeMap};

pub(super) struct Canonical<'a>(pub(super) &'a FileRef);
struct Descriptor<'a>(&'a FileDescriptor);

impl Serialize for Canonical<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_map(Some(5))?;
        fields.serialize_entry("descriptor", &Descriptor(&self.0.descriptor))?;
        fields.serialize_entry("display_name", &self.0.display_name)?;
        fields.serialize_entry("path", &self.0.path)?;
        fields.serialize_entry("version", &self.0.version)?;
        fields.serialize_entry(
            "volume",
            &super::super::volumes::canonical::Canonical(&self.0.volume),
        )?;
        fields.end()
    }
}

impl Serialize for Descriptor<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_map(Some(3))?;
        fields.serialize_entry("byte_length", &self.0.byte_length)?;
        fields.serialize_entry("media_type", &self.0.media_type)?;
        fields.serialize_entry("sha256", &self.0.sha256)?;
        fields.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files() -> Result<Vec<FileRef>> {
        let mut files = Vec::new();
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
                for (byte_length, byte) in [(0, 0), (17, 127), (MAX_EXACT_JS_INTEGER, 255)] {
                    files.push(FileRef::new(
                        VolumeRef::new(
                            ProviderRef::new(
                                "acyclic".into(),
                                "filesystem".into(),
                                "2".into(),
                                &mut None,
                            )?,
                            "files".into(),
                            class,
                            owner.clone(),
                            &mut None,
                        )?,
                        "nested/file.txt".into(),
                        "revision-1".into(),
                        FileDescriptor::new(
                            [byte; 32],
                            byte_length,
                            "application/octet-stream".into(),
                        )?,
                        "quoted-\"-\u{03b1}".into(),
                        &mut None,
                    )?);
                }
            }
        }
        Ok(files)
    }

    fn old_digest(
        file: &FileRef,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<[u8; 32]> {
        crate::executor::json::canonical_json_digest(
            file,
            crate::store::default_projection_read_limits().maximum_bytes,
            work,
        )
    }

    #[test]
    fn immutable_file_identity_borrows_canonical_fields_and_preserves_capabilities() -> Result<()> {
        for file in files()? {
            assert_eq!(
                serde_json::to_vec(&Canonical(&file)).unwrap(),
                crate::contract::canonical_json_bytes(&file)?
            );
            let mut previous = None;
            let old =
                acyclic_test_support::measure(|| previous = Some(old_digest(&file, &mut None)));
            let mut current = None;
            let new = acyclic_test_support::measure(|| {
                current = Some(file.identity_digest(u64::MAX, &mut None))
            });
            let previous = previous.unwrap()?;
            assert_eq!(current.unwrap()?, previous);
            assert_eq!(
                file.read_capability(&mut None)?,
                content_label(acyclic_fs::access::FILE_READ_PREFIX, previous, &mut None)?
            );
            let issuer = crate::core::AuthorityIssuer::new(
                "files",
                [9; 32],
                crate::core::Authority {
                    kind: crate::core::AggregateKind::Conversation,
                    id: "files".into(),
                },
            );
            let scope = issuer.issue(
                "read",
                &crate::Capabilities::new([file.read_capability(&mut None)?]),
                None,
                None,
                &mut None,
            )?;
            ContentGrant::verify_read(&issuer.verifier(&mut None)?, &scope, &file)?;
            ContentGrant::verify_file_read(&issuer.verifier(&mut None)?, &scope, &file)?;
            let mut changed = file.clone();
            changed.version.push_str("-changed");
            assert!(
                ContentGrant::verify_read(&issuer.verifier(&mut None)?, &scope, &changed).is_err()
            );
            assert!(
                ContentGrant::verify_file_read(&issuer.verifier(&mut None)?, &scope, &changed)
                    .is_err()
            );
            eprintln!(
                "FILE_IDENTITY_ALLOCS new={} old={}",
                new.count_total, old.count_total
            );
            assert!(
                new.count_total * 2 < old.count_total,
                "file identity rebuilt owned JSON: new={new:?}, old={old:?}"
            );
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn immutable_file_identity_uses_original_hash_work_without_owned_json_copies() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        for file in files()? {
            let expected = file.read_capability(&mut None)?;
            let bytes = crate::contract::canonical_json_bytes(&file)?.len() as u64;
            let cancellation = CancellationToken::new();
            let mut budget = acyclic_fs::performance::work_budget(20_000, 1_048_576);
            budget.bytes_copied = expected.len() as u64;
            budget.bytes_hashed = bytes;
            let original = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&original)?,
            );
            assert_eq!(file.read_capability(&mut Some(&mut io))?, expected);
            assert_eq!(original.receipt().bytes_hashed, bytes);
            assert_eq!(original.receipt().bytes_copied, budget.bytes_copied);
            cancellation.cancel();
            assert!(file.identity_digest(65_536, &mut Some(&mut io)).is_err());
            assert!(file.read_capability(&mut Some(&mut io)).is_err());

            let cancellation = CancellationToken::new();
            budget.bytes_hashed = 0;
            let denied = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&denied)?,
            );
            assert!(file.identity_digest(65_536, &mut Some(&mut io)).is_err());
            assert_eq!(denied.receipt().bytes_hashed, 0);
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn restored_owned_file_hash_is_denied_by_the_same_copy_allowance() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        for file in files()? {
            let cancellation = CancellationToken::new();
            let mut budget = acyclic_fs::performance::work_budget(20_000, 1_048_576);
            budget.bytes_copied = file.read_capability(&mut None)?.len() as u64;
            let original = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&original)?,
            );
            assert!(old_digest(&file, &mut Some(&mut io)).is_err());
            assert_eq!(original.receipt().bytes_hashed, 0);
        }
        Ok(())
    }
}
