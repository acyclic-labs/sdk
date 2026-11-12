//! Original committed inputs retain their host reader, without delegating it.
use super::*;

pub(in crate::filesystem) struct SelectedInputs<A: 'static, O: 'static> {
    owner: Arc<FilesystemContentVerifier<A, O>>,
    files: Vec<FileRef>,
}

fn identity(file: &FileRef) -> (&VolumeRef, &str, &str, &[u8; 32], u64, &str, &str) {
    (
        file.volume(),
        file.path(),
        file.version(),
        file.descriptor().sha256(),
        file.descriptor().byte_length(),
        file.descriptor().media_type(),
        file.display_name(),
    )
}

impl<A, O> SelectedInputs<A, O> {
    pub(in crate::filesystem) fn new(
        owner: &Arc<FilesystemContentVerifier<A, O>>,
        mut files: Vec<FileRef>,
    ) -> Option<Arc<Self>> {
        // Move the existing committed selection. Ordinary files keep their
        // original reader, policy and caller ceiling; no directory is delegated.
        files.retain(|file| file.volume().is_agent_journal());
        if files.is_empty() {
            return None;
        }
        files.sort_unstable_by(|a, b| identity(a).cmp(&identity(b)));
        files.dedup();
        Some(Arc::new(Self {
            owner: Arc::clone(owner),
            files,
        }))
    }
}

impl<A, O> FilesystemContentVerifier<A, O> {
    pub(in crate::filesystem) fn with_selected_inputs(
        mut self: Arc<Self>,
        inputs: Option<Arc<SelectedInputs<A, O>>>,
    ) -> Result<Arc<Self>> {
        Arc::get_mut(&mut self)
            .ok_or_else(|| Error::Conflict("turn input reader is already shared".into()))?
            .selected_inputs = inputs;
        Ok(self)
    }

    pub(super) fn selected_input(
        &self,
        reference: &FileRef,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<&FilesystemContentVerifier<A, O>>> {
        let Some(inputs) = &self.selected_inputs else {
            return Ok(None);
        };
        let mut refused = None;
        let selected = inputs.files.binary_search_by(|file| {
            if let Some(io) = io
                && let Err(error) = io.charged(WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })
            {
                refused = Some(error);
                return std::cmp::Ordering::Equal;
            }
            identity(file).cmp(&identity(reference))
        });
        if let Some(error) = refused {
            return Err(error);
        }
        if selected.is_err() {
            return Ok(None);
        }
        crate::executor::with_history_io(io, |work| {
            for grants in std::iter::once(self.authority.scope().capabilities()).chain(ceiling) {
                if !crate::runtime::read_granted_with_work(grants, reference, work)? {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "selected input is outside the caller's original read grant",
                    ));
                }
            }
            Ok(())
        })?;
        Ok(Some(inputs.owner.as_ref()))
    }
}
