use super::*;
use crate::conversation::{BoundedContentFuture, ContentHold, ContentIo};
use crate::resources::GenerationRef;

struct Updating<'a>(&'a LocalHarness);

impl ContentResidencyVerifier for Updating<'_> {
    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
        maximum: u64,
        io: Option<&'io ContentIo>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            (
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        assert!(
            generation.is_some(),
            "instruction capture performed an unpinned read"
        );
        BoundedContentFuture::new(io, || async move {
            let reader = self.0.context_reader()?;
            let result = reader
                .read_private_path(volume, prefix, path, generation, maximum, io)?
                .await?;
            if path == "AGENTS.md" {
                instruction(self.0, "AGENTS.md", "new agents").await?;
                instruction(self.0, "CLAUDE.md", "new claude").await?;
            }
            Ok(result)
        })
    }
}

#[tokio::test]
async fn volume_generation_is_pinned_across_an_update_between_instruction_reads() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let harness = LocalHarness::open(volume_config(directory.path(), project, text())?).await?;
    instruction(&harness, "AGENTS.md", "old agents").await?;
    instruction(&harness, "CLAUDE.md", "old claude").await?;
    let names = vec!["AGENTS.md".into(), "CLAUDE.md".into()];
    let captured = harness
        .volume_instructions(&names, 1024, &Updating(&harness))
        .await?;
    assert_eq!(captured.instructions.len(), 2);
    assert_eq!(
        captured.instructions[0].version(),
        captured.instructions[1].version()
    );
    let reader = harness.context_reader()?;
    for (file, expected) in captured
        .instructions
        .iter()
        .zip([b"old agents", b"old claude"])
    {
        assert_eq!(reader.read(file, 1024, None)?.await?.as_ref(), expected);
    }
    let operation = OperationId::new();
    harness.stage_context_manifest(operation).await?;
    let (_, current) = manifest(&harness, operation).await?;
    assert!(
        current
            .discovery
            .instructions
            .iter()
            .all(|file| file.volume() == harness.project())
    );
    for (file, expected) in current
        .discovery
        .instructions
        .iter()
        .zip([b"new agents", b"new claude"])
    {
        assert_eq!(reader.read(file, 1024, None)?.await?.as_ref(), expected);
    }
    Ok(())
}
