//! Settings reuse the original effective-source cut retained by the journal.
use super::*;

impl<'a> Tree<'a> {
    /// Retain one complete original source cut before selecting a model step.
    pub(in crate::local) async fn for_step(
        volumes: &'a Volumes<'a>,
        journal: &dyn crate::executor::ExecutionJournal,
        operation: crate::OperationId,
        step: u32,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let attempt = io.map(crate::executor::PreparationIo::scoped).transpose()?;
        let temporary = attempt.as_ref();
        let key = crate::executor::with_history_io(temporary, |work| {
            crate::executor::preparation_format(format_args!("settings:{step}:sources"), work)
        })?;
        let reader = crate::filesystem::FilesystemContentVerifier::new(
            volumes.files.clone(),
            volumes.verifier,
            volumes.signed,
            volumes.maximum_bytes,
            temporary,
        )?;
        let retained = crate::layers::ContentSources {
            reader: reader.as_ref(),
            seed: Some(volumes.seed),
            home: volumes.home,
            configuration: volumes.configuration,
            maximum_bytes: volumes.maximum_bytes,
            io,
        }
        .retain(journal, operation, &key, config::AGENT)
        .await?;
        let mut tree = Self::pinned(volumes, retained.selection.home.1, io)?;
        tree.configuration = std::borrow::Cow::Owned(retained.selection.configuration);
        tree._cut_hold = retained.custody;
        Ok(tree)
    }
}
