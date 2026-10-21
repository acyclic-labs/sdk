use super::*;

impl<P: StreamProvider + ?Sized> TaskFiles<P> {
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
        }))
    }
}
