use super::*;
use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner};
use crate::resources::ProviderRef;
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

fn absent() -> std::future::Ready<Result<FileRef>> {
    std::future::ready(Err(Error::Unauthorized("no admitted model command".into())))
}

proptest! {
    // Only the host's immutable pointer authorizes a configuration, for this
    // task and this content. No current file or other task's pointer suffices.
    #[test]
    fn configuration_authority_is_bound_to_the_admitted_task_and_manifest(
        body in ".{0,96}", other in any::<[u8;32]>(), task in any::<[u8;16]>(),
    ) {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()),
                StreamPath::new("manifest-binding")?).await?;
            let task = TaskId::from_bytes(task);
            let reference = FileRef::new(
                crate::conversation::VolumeRef::new(
                    ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None)?,
                    "journal".into(), VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([1;16])), &mut None,
                )?, "manifest.json".into(), "1".into(),
                FileDescriptor::from_bytes(body.as_bytes(), "application/json", &mut None)?,
                "manifest.json".into(), &mut None,
            )?;
            let digest = reference.descriptor().sha256();
            prop_assert!(authorize(&volume, task, digest, 4096, absent).await.is_err());
            retain(&volume, task, &reference, 4096).await?;
            retain(&volume, task, &reference, 4096).await?;
            prop_assert!(authorize(&volume, task, digest, 4096, absent).await.is_ok());
            let mut different_task = task.into_bytes();
            different_task[0] ^= 1;
            let recovered = TaskId::from_bytes(different_task);
            prop_assert!(authorize(&volume, recovered, digest, 4096, absent).await.is_err());
            let mut wrong = *digest;
            wrong[0] ^= 1;
            let original = || std::future::ready(Ok(reference.clone()));
            prop_assert!(authorize(&volume, recovered, &wrong, 4096, original).await.is_err());
            prop_assert!(volume.snapshot(&Query::File(key(recovered)?), 1).await?.files.is_empty());
            // Missing derived state is rebuilt only after checking the original
            // admission. Omitting reconstruction fails this same authorization.
            prop_assert!(authorize(&volume, recovered, digest, 4096, original).await.is_ok());
            prop_assert!(authorize(&volume, recovered, digest, 4096, absent).await.is_ok());
            let retained: FileRef = serde_json::from_slice(
                &volume.snapshot(&Query::File(key(recovered)?), 1).await?.files[0].bytes)?;
            prop_assert_eq!(&retained, &reference);
            let expected = &other == digest;
            prop_assert_eq!(authorize(&volume, task, &other, 4096, absent).await.is_ok(), expected);
            // The same equality detector rejects removal of the digest check.
            if !expected {
                let without_digest_check = volume.snapshot(&Query::File(key(task)?), 1).await?.files.len() == 1;
                prop_assert_ne!(without_digest_check, expected);
            }
            let mut changed = reference.clone();
            // A different manifest cannot replace an admitted pointer.
            changed = FileRef::new(changed.volume().clone(), changed.path().into(),
                "2".into(), changed.descriptor().clone(), changed.display_name().into(), &mut None)?;
            prop_assert!(retain(&volume, task, &changed, 4096).await.is_err());
            Ok(())
        })?;
    }
}
