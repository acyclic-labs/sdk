use super::*;

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    /// Resolve and publish at the observed cut; stale resolution fails closed.
    pub async fn submit(
        &mut self,
        input: &str,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<OperationId> {
        self.submit_with_retry::<false>(input, reader).await
    }

    #[cfg(feature = "tui")]
    pub(crate) async fn submit_input(
        &mut self,
        input: &str,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<OperationId> {
        self.submit_with_retry::<true>(input, reader).await
    }

    pub(crate) async fn publish_input_resolution(
        &mut self,
        resolution: CommandResolution,
    ) -> Result<OperationId> {
        self.publish_input_resolution_at::<true>(resolution, OperationId::new())
            .await
    }

    pub(super) async fn publish_input_resolution_at<const RETRY: bool>(
        &mut self,
        resolution: CommandResolution,
        operation: OperationId,
    ) -> Result<OperationId> {
        for _ in 0..super::publication::PUBLICATION_ATTEMPTS {
            let before = self.generation;
            match self
                .publish_resolution_with_retry::<RETRY>(resolution.clone(), operation)
                .await
            {
                // Quit modifies only exit_requested. Recompute it over an owner
                // layout update rather than clobbering selection/history fields.
                Err(Error::Conflict(_))
                    if RETRY
                        && self.generation != before
                        && matches!(
                            resolution,
                            CommandResolution::Submit {
                                request: CommandRequest::Quit
                            }
                        ) => {}
                result => return result,
            }
        }
        Err(Error::Conflict(
            "command layout remained busy; retry the input".into(),
        ))
    }

    async fn submit_with_retry<const RETRY: bool>(
        &mut self,
        input: &str,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<OperationId> {
        // Allocate once, before resolution or any publication attempt.
        let operation = OperationId::new();
        for _ in 0..super::publication::PUBLICATION_ATTEMPTS {
            if let Some(confirmation) = self
                .state(CommandFileRoot::Session, "layout.json")?
                .and_then(|layout| layout.get("confirmation").cloned())
                .filter(|value| !value.is_null())
            {
                match input.trim().to_ascii_lowercase().as_str() {
                    "yes" | "y" => return self.confirm_request(confirmation, true).await,
                    "no" | "n" => return self.confirm_request(confirmation, false).await,
                    _ => {}
                }
            }
            if let Some(operation) = self.view_input(input).await? {
                return Ok(operation);
            }
            let cut = self.generation;
            let resolution = match &self.composer {
                Some(composer) => composer.resolve(operation, input).await?,
                None => {
                    self.catalog
                        .resolve(input, reader, self.maximum_bytes)
                        .await?
                }
            };
            self.load_task_settings().await?;
            if self.generation != cut {
                if RETRY && self.composer.is_none() {
                    continue;
                }
                return Err(Error::Conflict(
                    "command query advanced during resolution; retry from the new cut".into(),
                ));
            }
            return if RETRY {
                self.publish_input_resolution_at::<true>(resolution, operation)
                    .await
            } else {
                self.publish_resolution_with_retry::<false>(resolution, operation)
                    .await
            };
        }
        Err(Error::Conflict(
            "command resolution remained busy; retry the input".into(),
        ))
    }
}
