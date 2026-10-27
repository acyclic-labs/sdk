use super::*;

/// Capability-scoped context for one agent turn's tools and model content.
#[derive(Clone)]
pub struct TaskContext {
    pub(crate) harness: Arc<AgentHarness>,
    pub(super) scope: RuntimeScope,
    pub(super) output_content: ContentBindings,
}

impl TaskContext {
    /// Returns the exact effective authority.
    #[must_use]
    pub const fn scope(&self) -> &RuntimeScope {
        &self.scope
    }

    /// Narrows this context's grants and limits for one bound execution.
    pub(crate) fn scoped(&self, grants: Capabilities, limits: Limits) -> Result<Self> {
        Ok(Self {
            scope: self.scope.narrow(grants, limits)?,
            ..self.clone()
        })
    }
}

#[doc(hidden)]
pub fn read_granted(grants: &Capabilities, file: &FileRef) -> Result<bool> {
    read_granted_with_work(grants, file, &mut None)
}

pub(crate) fn read_granted_with_work(
    grants: &Capabilities,
    file: &FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    file.validate(work)?;
    if contains_content_grant_with_work(grants, file, acyclic_fs::access::FILE_READ_PREFIX, work)? {
        return Ok(true);
    }
    crate::core::directory_read_granted_with_work(
        grants,
        file.volume(),
        file.path()
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent),
        work,
    )
}
