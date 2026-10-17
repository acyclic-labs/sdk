use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn authorize_content_write(
        &self,
        reference: &WorkspaceRef,
        grant: &ContentGrant,
        path: &str,
        replay: bool,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&PreparationIo>,
    ) -> Result<GenerationRef> {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        let workspace = self.open(reference, Some(io)).await?;
        let scope = grant.policy_scope();
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        let view = crate::filesystem::access::access_view(
            &workspace,
            scope,
            None,
            capability_payload(label, VolumeOperation::Write, io)?,
            ceiling,
            io,
        )
        .await?;
        if replay {
            if !view.0.decide(Action::Create, path, access_now()).allowed {
                require(&view.0, Action::Write, path, io)?;
            }
            return self.generation_ref_id(view.2, Some(io));
        }
        // Refuse before probing existence when neither mutation can succeed.
        if !view.0.decide(Action::Create, path, access_now()).allowed {
            require(&view.0, Action::Write, path, io)?;
        }
        let generation = io.sdk(workspace.generation(view.2, io, io.cancellation()).await)?;
        let absolute = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let hold = io.guard_owned(absolute.capacity() as u64)?;
        let absolute = (absolute, hold);
        // Check the target and every missing parent. Existing parents need no
        // disclosure grant: create-only inboxes must remain usable without List.
        let mut candidate = path;
        loop {
            let prefix = &absolute.0[..candidate.len() + 1];
            let present = match io.sdk(generation.stat(prefix, io, io.cancellation()).await) {
                Ok(_) => true,
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => false,
                Err(error) => return Err(error),
            };
            if candidate == path || !present {
                let action = if present {
                    Action::Write
                } else {
                    Action::Create
                };
                require(&view.0, action, candidate, io)?;
            }
            let Some((parent, _)) = candidate.rsplit_once('/') else {
                break;
            };
            candidate = parent;
        }
        self.generation_ref_id(view.2, Some(io))
    }
}
