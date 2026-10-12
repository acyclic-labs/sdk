use super::*;
use crate::kernel::{KernelAccounting, Owned};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    pub(super) fn canonical_path(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<String>, LazyWorkspaceError> {
        accounting.check()?;
        PortablePath::validate(path, self.workspace.limits())
            .map_err(crate::kernel::NamespacePathError::Path)?;
        let copied = copy_bytes(path.as_bytes(), accounting)?;
        Ok(Owned::new(
            String::from_utf8(copied.value).expect("validated UTF-8 path"),
            copied.allocation,
            copied.bytes,
        ))
    }

    pub(crate) fn namespace_path(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<NamespacePath>, LazyWorkspaceError> {
        let (value, allocation) = NamespacePath::parse_in_profile_with_accounting(
            path,
            self.workspace.profile(),
            self.workspace.limits(),
            accounting,
            cancellation(accounting),
        )?;
        let bytes = value.retained_bytes();
        Ok(Owned::new(value, allocation, bytes))
    }
    pub(super) fn child_path(
        &self,
        directory: &NamespacePath,
        name: &LogicalName,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Owned<String>>, LazyWorkspaceError> {
        if name.unicode_utf8_bytes().is_none() {
            return Ok(None);
        }
        let cancellation = cancellation(accounting);
        let (value, allocation) = directory.child_with_accounting(
            name,
            self.workspace.limits(),
            accounting,
            cancellation,
        )?;
        let bytes = value.retained_bytes();
        let child = Owned::new(value, allocation, bytes);
        let result = child
            .value
            .unicode_text_with_accounting(accounting, cancellation);
        child.retire(accounting)?;
        result
            .map(|(value, allocation)| {
                let bytes = value.capacity() as u64;
                Some(Owned::new(value, allocation, bytes))
            })
            .map_err(Into::into)
    }
}
