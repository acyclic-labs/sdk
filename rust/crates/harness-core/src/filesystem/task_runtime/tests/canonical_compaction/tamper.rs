//! A descriptor fault on the real canonical publication read, not a standalone checksum test.
use super::*;
use crate::conversation::{
    BoundedContentFuture, ContentResidencyVerifier, FileDescriptor, FileRef,
};
use crate::executor::{ExecutionEvent, PreparationIo};

type Journal = crate::filesystem::FilesystemExecutionJournal<
    MemoryStream,
    MemoryAuthorityBackend,
    MemoryObjectBackend,
>;

pub(super) async fn check(
    mut journal: Journal,
    operation: OperationId,
    summary: &FileRef,
    reader: Arc<dyn ContentResidencyVerifier>,
) -> Result<Journal> {
    let records = journal.replay(operation, 0, 128, None)?.await?;
    let publication = records
        .iter()
        .find_map(|record| {
            matches!(
                &record.event,
                ExecutionEvent::ContextCompacted { step: 0, .. }
            )
            .then_some(&record.event)
        })
        .expect("published canonical compaction");
    let mut digest = *summary.descriptor().sha256();
    digest[0] ^= 1;
    let forged = FileRef::new(
        summary.volume().clone(),
        summary.path().into(),
        summary.version().into(),
        FileDescriptor::new(
            digest,
            summary.descriptor().byte_length(),
            summary.descriptor().media_type().into(),
        )?,
        summary.display_name().into(),
        &mut None,
    )?;
    assert_ne!(summary.descriptor(), forged.descriptor());
    for ignore_descriptor in [false, true] {
        journal = journal.with_input_verifier(Arc::new(DescriptorFault {
            inner: reader.clone(),
            original: summary.clone(),
            forged: forged.clone(),
            ignore_descriptor,
        }));
        // Revalidate the exact durable publication through the production append
        // path. A successful retry must not append a second checkpoint.
        let result = journal
            .append(operation, "context:0:compacted", publication, None)?
            .await;
        if ignore_descriptor {
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                assert_rejected(result);
            }))
            .expect_err("the broken validator must fail the same rejection assertion");
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str));
            assert_eq!(message, Some("tampered summary was accepted"));
        } else {
            assert_rejected(result);
        }
    }
    journal = journal.with_input_verifier(reader);
    assert_eq!(
        journal.replay(operation, 0, 128, None)?.await?.len(),
        records.len()
    );
    Ok(journal)
}

fn assert_rejected(result: Result<()>) {
    assert!(result.is_err(), "tampered summary was accepted");
}

struct DescriptorFault {
    inner: Arc<dyn ContentResidencyVerifier>,
    original: FileRef,
    forged: FileRef,
    ignore_descriptor: bool,
}

impl ContentResidencyVerifier for DescriptorFault {
    fn supports_read(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
        self.inner.supports_read(volume, io)
    }

    fn verify<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, ()>> {
        self.inner.verify(file, maximum, io)
    }

    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        let corrupted = if file == &self.original {
            &self.forged
        } else {
            file
        };
        // Deliberately broken control: load the same immutable bytes while
        // discarding the forged descriptor instead of authenticating it.
        let checked = if self.ignore_descriptor && corrupted == &self.forged {
            &self.original
        } else {
            corrupted
        };
        self.inner.read(checked, maximum, ceiling, io)
    }
}
