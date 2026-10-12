use super::*;

impl NativeProcessRequest {
    pub(super) fn validate(&self, view: &NativeViewManifest, io: &super::super::PreparationIo) -> Result<()> {
        use super::super::native_contract::validation_error;
        if self.standard_input.is_some() && self.mcp_stdio.is_some() {
            return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid, "literal stdin and MCP exchange are mutually exclusive"));
        }
        if let Some(exchange) = &self.mcp_stdio {
            let work = io.original_native_work().ok_or(boundary(crate::wire::ErrorCode::Storage, "native invocation requires original work"))?;
            drop(exchange.exchange(Some(work))?);
        }
        let items = self.environment.len().checked_add(self.argv.len())
            .and_then(|items| items.checked_add(self.cwd.components().count()))
            .ok_or_else(|| validation_error(Some(io), crate::wire::ErrorCode::Invalid, "native invocation item count overflow"))?;
        io.charge(WorkCounters { items_examined: items as u64, ..Default::default() })?;
        if !self.executable.is_absolute() || !self.cwd.is_absolute() || !self.view.options.root.is_absolute()
            || !self.cwd.strip_prefix(&self.view.options.root).is_ok_and(|path|
                path.components().all(|part| matches!(part, std::path::Component::Normal(_))))
            || !self.has_valid_capture_allowances()
            || self.environment.iter().any(|(key, value)| key.is_empty() || key.contains(['=', '\0']) || value.contains('\0'))
            || self.argv.iter().any(|arg| arg.contains('\0')) {
            return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid, "invalid native process invocation"));
        }
        #[cfg(windows)]
        for (index, key) in self.environment.keys().enumerate() {
            for prior in self.environment.keys().take(index) {
                io.charge(WorkCounters { items_examined: 1, ..Default::default() })?;
                if key.chars().flat_map(char::to_uppercase).eq(prior.chars().flat_map(char::to_uppercase)) {
                    return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid, "duplicate native Windows environment key"));
                }
            }
        }
        if &self.view != view { return Err(validation_error(Some(io), crate::wire::ErrorCode::Conflict, "native command view changed")); }
        Ok(())
    }
}
