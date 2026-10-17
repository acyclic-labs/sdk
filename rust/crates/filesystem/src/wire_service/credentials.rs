impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemWireService<A, O> {
    async fn issue_credential(
        &self,
        request: Request<wire::CredentialRequest>,
        kind: CredentialKind,
    ) -> Result<Response<wire::CredentialResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.expires_after_seconds == 0
            || request.expires_after_seconds > self.limits.maximum_credential_seconds
        {
            return Err(Status::invalid_argument(
                "credential lifetime exceeds the configured bound",
            ));
        }
        let workspace = self.workspace(request.workspace).await?;
        let generation = match request.generation {
            Some(reference) => {
                let generation = self.generation(Some(reference)).await?;
                if generation.workspace.id() != workspace.id() {
                    return Err(Status::failed_precondition(
                        "credential generation belongs to another workspace",
                    ));
                }
                generation
            }
            None => {
                workspace
                    .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| status(&failure.error))?
                    .value
            }
        };
        let issuer = self.credential_issuer.as_ref().ok_or_else(|| {
            Status::unimplemented("credential issuance is unavailable in this deployment")
        })?;
        let enabled = match kind {
            CredentialKind::Mount => issuer.mount_enabled(),
            CredentialKind::S3 => issuer.s3_enabled(),
        };
        if !enabled {
            return Err(Status::unimplemented(
                "requested credential surface is unavailable",
            ));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::permission_denied("credential clock is unavailable"))?
            .as_secs();
        let (view, _) = workspace
            .access
            .as_ref()
            .ok_or_else(|| Status::permission_denied("credential policy binding is missing"))?;
        let requested_end = now
            .checked_add(request.expires_after_seconds)
            .ok_or_else(|| Status::invalid_argument("credential deadline overflows"))?;
        let deadline = view
            .valid_until()
            .map_or(requested_end, |end| end.min(requested_end));
        if deadline <= now || !view.allows_every_path(&crate::access::Action::ALL, now) {
            return Err(Status::permission_denied("credential authority expired"));
        }
        let grant = issuer
            .issue(CredentialGrantRequest {
                kind,
                workspace_id: workspace.id(),
                workspace_name: workspace.name().clone(),
                generation_id: generation.id(),
                writable: request.writable,
                expires_after_seconds: deadline - now,
                expires_at_unix_seconds: i64::try_from(deadline).map_err(|_| {
                    Status::invalid_argument("credential deadline is unrepresentable")
                })?,
                idempotency_key: operation(request.operation)?,
            })
            .await?;
        let expiry = match &grant {
            CredentialGrant::Bearer {
                expires_at_unix_seconds,
                ..
            }
            | CredentialGrant::S3 {
                expires_at_unix_seconds,
                ..
            } => *expires_at_unix_seconds,
        };
        let expiry = u64::try_from(expiry)
            .map_err(|_| Status::permission_denied("issued credential has invalid expiry"))?;
        if expiry <= now || expiry > deadline {
            return Err(Status::permission_denied(
                "issued credential exceeds its authority deadline",
            ));
        }
        let response = match grant {
            CredentialGrant::Bearer {
                endpoint,
                token,
                expires_at_unix_seconds,
            } if kind == CredentialKind::Mount
                && !endpoint.is_empty()
                && !token.is_empty()
                && expires_at_unix_seconds > 0 =>
            {
                wire::CredentialResponse {
                    endpoint,
                    expires_at_unix_seconds,
                    credential: Some(wire::credential_response::Credential::BearerToken(token)),
                }
            }
            CredentialGrant::S3 {
                endpoint,
                bucket,
                region,
                access_key_id,
                secret_access_key,
                session_token,
                expires_at_unix_seconds,
            } if kind == CredentialKind::S3
                && !endpoint.is_empty()
                && !bucket.is_empty()
                && !region.is_empty()
                && !access_key_id.is_empty()
                && !secret_access_key.is_empty()
                && expires_at_unix_seconds > 0 =>
            {
                wire::CredentialResponse {
                    endpoint,
                    expires_at_unix_seconds,
                    credential: Some(wire::credential_response::Credential::S3(
                        wire::S3Credential {
                            bucket,
                            region,
                            access_key_id,
                            secret_access_key,
                            session_token,
                        },
                    )),
                }
            }
            _ => {
                return Err(Status::internal(
                    "credential issuer returned an invalid grant",
                ));
            }
        };
        Ok(Response::new(response))
    }

    async fn retain(
        &self,
        request: Request<wire::RetainGenerationRequest>,
        checkpoint: bool,
    ) -> Result<Response<wire::RetainGenerationResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let _ = operation(request.operation)?;
        let generation = self.generation(request.generation).await?;
        if checkpoint {
            let workspace = self
                .workspace(generation_ref(&generation).workspace)
                .await?;
            let head = workspace
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| status(&failure.error))?
                .value;
            if head.id() != generation.id() {
                return Err(Status::failed_precondition(
                    "checkpoint requires the current head",
                ));
            }
            workspace
                .checkpoint(&request.identity)
                .await
                .map_err(|error| status(&error))?;
        } else {
            generation
                .pin(
                    &request.identity,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| status(&failure.error))?;
        }
        Ok(Response::new(wire::RetainGenerationResponse {
            generation: Some(generation_ref(&generation)),
            identity: request.identity,
            status: wire::MutationStatus::Committed as i32,
        }))
    }
}
