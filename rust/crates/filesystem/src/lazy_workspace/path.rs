use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    pub(super) fn canonical_path(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<String>, LazyWorkspaceError> {
        PortablePath::validate(path, self.workspace.limits())
            .map_err(crate::kernel::NamespacePathError::Path)?;
        let bytes = copy_bytes(path.as_bytes(), ctx)?;
        Ok(Owned::new(
            String::from_utf8(bytes.value).expect("validated UTF-8"),
            bytes.allocation,
            bytes.bytes,
        ))
    }
    pub(crate) fn namespace_path(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<NamespacePath>, LazyWorkspaceError> {
        let (value, guard) = NamespacePath::parse_in_profile_with_accounting(
            path,
            self.workspace.profile(),
            self.workspace.limits(),
            ctx,
            cancellation(ctx),
        )?;
        let bytes = value.retained_bytes();
        Ok(Owned::new(value, guard, bytes))
    }
    pub(super) fn child_path(
        &self,
        directory: &NamespacePath,
        name: &LogicalName,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<Owned<String>>, LazyWorkspaceError> {
        if name.unicode_utf8_bytes().is_none() {
            return Ok(None);
        }
        let (value, guard) = directory.child_with_accounting(
            name,
            self.workspace.limits(),
            ctx,
            cancellation(ctx),
        )?;
        let bytes = value.retained_bytes();
        let child = Owned::new(value, guard, bytes);
        let result = child
            .value
            .unicode_text_with_accounting(ctx, cancellation(ctx));
        child.retire(ctx)?;
        result
            .map(|(value, guard)| {
                let bytes = value.capacity() as u64;
                Some(Owned::new(value, guard, bytes))
            })
            .map_err(Into::into)
    }
}
