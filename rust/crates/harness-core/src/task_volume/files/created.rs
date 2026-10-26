use super::*;

impl<P: StreamProvider + ?Sized> TaskFiles<P> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn for_created_task_with_native_work(
        &self,
        task: &crate::runtime::TaskContext,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<Option<Arc<Self>>> {
        task.require_original_native_work(work)?;
        let scope = task.scope();
        let Some(access) = scope.created_access.as_ref() else {
            return Ok(None);
        };
        let denied = || crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "approval files require the original child owner",
        };
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let owner = task.owned_mailbox.as_ref().ok_or_else(denied)?;
        owner.validate_scope(scope, Some(&io))?;
        if task.durable_task_id() != Some(owner.task_id())
            || access.operation.into_bytes() != owner.task_id().into_bytes()
        {
            return Err(denied());
        }
        access
            .publication_lease
            .as_ref()
            .ok_or_else(denied)?
            .verify(Some(&io))?
            .await?;
        owner.verify_owner(Some(&io))?.await?;
        self.for_authenticated_scope_with_native_work(owner.task_id(), scope, work)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn for_authenticated_scope_with_native_work(
        &self,
        task: crate::TaskId,
        scope: &crate::runtime::RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<Option<Arc<Self>>> {
        let Some(access) = scope.created_access.as_ref() else {
            return Ok(None);
        };
        let denied = || crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "approval files differ from admitted child resources",
        };
        if access.operation.into_bytes() != task.into_bytes() {
            return Err(denied());
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut bindings = access.content.iter().filter(|binding| {
            binding.default_writer
                && binding.scratch_creation.is_some()
                && binding.volume.class() == VolumeClass::AgentPrivate
        });
        let binding = bindings.next().ok_or_else(denied)?;
        if bindings.next().is_some() || !matches!(binding.volume.owner(), VolumeOwner::Agent(_)) {
            return Err(denied());
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let grant = binding.volume.capability(operation, &mut Some(&mut &io))?;
            if !scope.grants().contains(&grant) || !access.grants.contains(&grant) {
                return Err(denied());
            }
        }
        let maximum_bytes = scope.limits().file_bytes;
        if maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(denied());
        }
        Self::open_authorized_native(
            self.inner.provider.clone(),
            &binding.volume,
            maximum_bytes,
            work,
        )
        .await
        .map(Some)
    }

    #[doc(hidden)]
    pub async fn for_created_task(
        &self,
        task: &crate::runtime::TaskContext,
        effective: &crate::runtime::RuntimeScope,
    ) -> crate::Result<Option<Self>> {
        let scope = task.scope();
        let Some(access) = scope.created_access.as_ref() else {
            return Ok(None);
        };
        let denied =
            || crate::Error::Unauthorized("task files require the original child owner".into());
        let owner = task.owned_mailbox.as_ref().ok_or_else(denied)?;
        owner.validate_scope(scope, None)?;
        if task.durable_task_id() != Some(owner.task_id())
            || access.operation.into_bytes() != owner.task_id().into_bytes()
        {
            return Err(denied());
        }
        access
            .publication_lease
            .as_ref()
            .ok_or_else(denied)?
            .verify(None)?
            .await?;
        owner.verify_owner(None)?.await?;
        self.for_authenticated_scope(owner.task_id(), scope, effective)
            .await
    }

    // Only runtime code may supply a scope authenticated against the retained
    // admission and creation receipts. This route does not acquire a lease.
    pub(crate) async fn for_authenticated_scope(
        &self,
        task: crate::TaskId,
        scope: &crate::runtime::RuntimeScope,
        effective: &crate::runtime::RuntimeScope,
    ) -> crate::Result<Option<Self>> {
        let Some(access) = scope.created_access.as_ref() else {
            return Ok(None);
        };
        let denied =
            || crate::Error::Unauthorized("task files differ from admitted child resources".into());
        if access.operation.into_bytes() != task.into_bytes() {
            return Err(denied());
        }
        let mut bindings = access.content.iter().filter(|binding| {
            binding.default_writer
                && binding.scratch_creation.is_some()
                && binding.volume.class() == VolumeClass::AgentPrivate
        });
        let binding = bindings.next().ok_or_else(denied)?;
        if bindings.next().is_some() || !matches!(binding.volume.owner(), VolumeOwner::Agent(_)) {
            return Err(denied());
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let grant = binding.volume.capability(operation, &mut None)?;
            if !scope.grants().contains(&grant)
                || !effective.grants().contains(&grant)
                || !access.grants.contains(&grant)
            {
                return Err(denied());
            }
        }
        let maximum_bytes = scope.limits().file_bytes.min(effective.limits().file_bytes);
        if maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(denied());
        }
        let path = StreamPath::new(format!(
            "harness/task-volumes/{}",
            binding.volume.storage_name(&mut None)?
        ))
        .map_err(storage)?;
        Ok(Some(Self {
            inner: TaskVolume::open(self.inner.provider.clone(), path)
                .await
                .map_err(storage)?,
            volume: binding.volume.clone(),
            maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody: None,
        }))
    }
}
