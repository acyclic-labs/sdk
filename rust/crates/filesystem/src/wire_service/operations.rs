#[tonic::async_trait]
impl<A, O> wire::filesystem_service_server::FilesystemService for FilesystemWireService<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn handshake(
        &self,
        request: Request<wire::HandshakeRequest>,
    ) -> Result<Response<wire::HandshakeResponse>, Status> {
        self.admit(&request)?;
        if let Some(requested) = request
            .into_inner()
            .protocol
            .and_then(|value| value.protocol)
        {
            if !requested.version.is_empty() && requested.version != FILESYSTEM_PROTOCOL_VERSION {
                return Err(Status::failed_precondition(
                    "unsupported filesystem contract version",
                ));
            }
            let digest = crate::descriptor_digest();
            if !requested.descriptor_digest.is_empty() && requested.descriptor_digest != digest {
                return Err(Status::failed_precondition(
                    "filesystem descriptor digest mismatch",
                ));
            }
        }
        let protocol = protocol::ProtocolIdentity {
            version: FILESYSTEM_PROTOCOL_VERSION.to_owned(),
            descriptor_digest: crate::descriptor_digest(),
        };
        Ok(Response::new(wire::HandshakeResponse {
            protocol: Some(protocol::HandshakeResponse {
                protocol: Some(protocol),
                supported: Some(protocol::CapabilitySet {
                    capabilities: vec![protocol::Capability {
                        name: "filesystem".to_owned(),
                        version: FILESYSTEM_PROTOCOL_VERSION.to_owned(),
                    }],
                }),
            }),
            capabilities: Some(wire::Capabilities {
                contract_version: FILESYSTEM_PROTOCOL_VERSION.to_owned(),
                profiles: hosted_contract::supported_profiles(),
                maximum_request_bytes: self.limits.maximum_request_bytes,
                maximum_response_bytes: self.limits.maximum_response_bytes,
                maximum_transaction_mutations: self.limits.maximum_transaction_mutations,
                maximum_page_items: self.limits.maximum_page_items,
                native_mount_credentials: self
                    .credential_issuer
                    .as_ref()
                    .is_some_and(|issuer| issuer.mount_enabled()),
                s3_credentials: self
                    .credential_issuer
                    .as_ref()
                    .is_some_and(|issuer| issuer.s3_enabled()),
                source_reconciliation: self.source_provider.is_some(),
            }),
        }))
    }

    async fn create_workspace(
        &self,
        request: Request<wire::CreateWorkspaceRequest>,
    ) -> Result<Response<wire::WorkspaceResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let operation_id = operation(request.operation)?.operation_id();
        let profile = profile(request.profile)?;
        let lifecycle = if self.filesystem.capabilities().durable {
            Lifecycle::Durable
        } else {
            Lifecycle::Ephemeral
        };
        let mut config = VolumeConfig::portable(lifecycle);
        config.profile = profile;
        let workspace = self
            .filesystem
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                request.name,
                crate::VolumeInitialization {
                    config,
                    access_policy: Some(crate::access::INITIAL_POLICY),
                },
                operation_id,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                None,
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value
            .workspace();
        Ok(Response::new(wire::WorkspaceResponse {
            workspace: Some(workspace_message(&workspace).await?),
            status: wire::MutationStatus::Committed as i32,
        }))
    }

    async fn open_workspace(
        &self,
        request: Request<wire::OpenWorkspaceRequest>,
    ) -> Result<Response<wire::WorkspaceResponse>, Status> {
        self.admit(&request)?;
        let workspace = match required(request.into_inner().selector, "workspace selector")? {
            wire::open_workspace_request::Selector::Workspace(reference) => {
                self.workspace(Some(reference)).await?
            }
            wire::open_workspace_request::Selector::Name(name) => self
                .filesystem
                .open_workspace(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| status(&failure.error))?
                .value
                .bind_coarse_access(&crate::access::Action::ALL, &CancellationToken::new())
                .await
                .map_err(|error| status(&error))?,
        };
        Ok(Response::new(wire::WorkspaceResponse {
            workspace: Some(workspace_message(&workspace).await?),
            status: wire::MutationStatus::Committed as i32,
        }))
    }

    async fn delete_workspace(
        &self,
        request: Request<wire::DeleteWorkspaceRequest>,
    ) -> Result<Response<wire::MutationResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let workspace = self.workspace(request.workspace).await?;
        let outcome = workspace
            .delete(operation(request.operation)?)
            .await
            .map_err(|error| status(&error))?;
        let status = match outcome {
            WorkspaceDelete::Deleted => wire::MutationStatus::Committed,
            WorkspaceDelete::AlreadyDeleted => wire::MutationStatus::AlreadyCommitted,
            WorkspaceDelete::Conflict => wire::MutationStatus::Conflict,
            WorkspaceDelete::IdempotencyConflict => wire::MutationStatus::IdempotencyConflict,
        };
        Ok(Response::new(mutation(status, None, None)))
    }

    async fn get_head(
        &self,
        request: Request<wire::GetHeadRequest>,
    ) -> Result<Response<wire::GenerationResponse>, Status> {
        self.admit(&request)?;
        let workspace = self.workspace(request.into_inner().workspace).await?;
        let generation = workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        Ok(Response::new(generation_message(&generation).await?))
    }

    async fn get_generation(
        &self,
        request: Request<wire::GetGenerationRequest>,
    ) -> Result<Response<wire::GenerationResponse>, Status> {
        self.admit(&request)?;
        let generation = self.generation(request.into_inner().generation).await?;
        Ok(Response::new(generation_message(&generation).await?))
    }

    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<wire::ReadResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_bytes == 0 || request.maximum_bytes > self.limits.maximum_response_bytes
        {
            return Err(Status::invalid_argument(
                "maximum_bytes is outside service bounds",
            ));
        }
        let generation = self.generation(request.generation).await?;
        let cancellation = CancellationToken::new();
        let bytes = match request.range {
            Some(range) => {
                if range.length > request.maximum_bytes {
                    return Err(Status::invalid_argument("range exceeds maximum_bytes"));
                }
                generation
                    .read_range(
                        &request.path,
                        range.offset,
                        range.length,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(|failure| status(&failure.error))?
                    .value
                    .bytes
            }
            None => {
                generation
                    .read(
                        &request.path,
                        request.maximum_bytes,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(|failure| status(&failure.error))?
                    .value
            }
        };
        Ok(Response::new(wire::ReadResponse {
            contents: bytes.to_vec(),
        }))
    }

    async fn stat(
        &self,
        request: Request<wire::StatRequest>,
    ) -> Result<Response<wire::StatResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let value = self
            .generation(request.generation)
            .await?
            .stat(
                &request.path,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        Ok(Response::new(wire::StatResponse {
            stat: Some(stat_message(&value)),
        }))
    }

    async fn list_directory(
        &self,
        request: Request<wire::ListDirectoryRequest>,
    ) -> Result<Response<wire::ListDirectoryResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let page = required(request.page, "page")?;
        if page.maximum_items == 0 || page.maximum_items > self.limits.maximum_page_items {
            return Err(Status::invalid_argument("directory page bound is invalid"));
        }
        let generation = self.generation(request.generation).await?;
        let after = page.after.map(input_logical_name).transpose()?;
        let listed = generation
            .list_directory(
                &request.path,
                after.as_ref(),
                page.maximum_items,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value
            .0;
        let next = listed
            .has_more
            .then(|| listed.entries.last().map(|entry| logical_name(&entry.name)))
            .flatten();
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(listed.entries.len())
            .map_err(|_| Status::resource_exhausted("directory page allocation failed"))?;
        for entry in listed.entries {
            let name = std::str::from_utf8(entry.name.as_bytes())
                .map_err(|_| Status::data_loss("portable directory name is not UTF-8"))?;
            let child = if request.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", request.path.trim_end_matches('/'))
            };
            let stat = generation
                .stat(&child, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| status(&failure.error))?
                .value;
            if stat.file_id != entry.file_id || stat.kind != entry.kind {
                return Err(Status::data_loss(
                    "directory binding changed within immutable generation",
                ));
            }
            entries.push(wire::DirectoryEntry {
                name: Some(logical_name(&entry.name)),
                stat: Some(stat_message(&stat)),
            });
        }
        Ok(Response::new(wire::ListDirectoryResponse {
            page: Some(wire::DirectoryPage { entries, next }),
        }))
    }

    async fn read_link(
        &self,
        request: Request<wire::ReadLinkRequest>,
    ) -> Result<Response<wire::ReadResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let contents = self
            .generation(request.generation)
            .await?
            .read_symbolic_link(
                &request.path,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value
            .bytes;
        if u64::try_from(contents.len()).unwrap_or(u64::MAX) > request.maximum_bytes
            || request.maximum_bytes > self.limits.maximum_response_bytes
        {
            return Err(Status::resource_exhausted(
                "symbolic link exceeds response bound",
            ));
        }
        Ok(Response::new(wire::ReadResponse {
            contents: contents.to_vec(),
        }))
    }

    async fn plan_extents(
        &self,
        request: Request<wire::PlanExtentsRequest>,
    ) -> Result<Response<wire::PlanExtentsResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_extents == 0 || request.maximum_extents > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument("extent bound is invalid"));
        }
        let range = required(request.range, "range")?;
        let plan = self
            .generation(request.generation)
            .await?
            .plan_extents(
                &request.path,
                range.offset,
                range.length,
                request.maximum_extents,
            )
            .await
            .map_err(|error| status(&error))?;
        let truncated =
            u32::try_from(plan.spans.len()).unwrap_or(u32::MAX) == request.maximum_extents;
        Ok(Response::new(wire::PlanExtentsResponse {
            extents: plan
                .spans
                .into_iter()
                .map(|span| wire::Extent {
                    range: Some(wire::ByteRange {
                        offset: span.offset,
                        length: span.length,
                    }),
                    kind: hosted_contract::extent_kind(span.kind) as i32,
                })
                .collect(),
            truncated,
        }))
    }

    async fn apply_transaction(
        &self,
        request: Request<wire::ApplyTransactionRequest>,
    ) -> Result<Response<wire::MutationResponse>, Status> {
        self.rpc_apply_transaction(request).await
    }

    async fn rebase_transaction(
        &self,
        request: Request<wire::RebaseTransactionRequest>,
    ) -> Result<Response<wire::RebaseTransactionResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.mutations.is_empty()
            || u32::try_from(request.mutations.len()).unwrap_or(u32::MAX)
                > self.limits.maximum_transaction_mutations
            || request.maximum_conflicts == 0
            || request.maximum_conflicts > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument(
                "transaction rebase bounds are invalid",
            ));
        }
        let base = self.generation(request.base).await?;
        let workspace = self.workspace(generation_ref(&base).workspace).await?;
        let mut transaction = workspace
            .begin_transaction_at(
                &base,
                operation(request.operation)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        for mutation in request.mutations {
            apply_mutation(&mut transaction, mutation).await?;
        }
        match transaction
            .rebase(
                request.maximum_conflicts,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value
        {
            TransactionRebase::Rebased(generation) => {
                Ok(Response::new(wire::RebaseTransactionResponse {
                    base: Some(generation_ref(&generation)),
                    conflicts: Vec::new(),
                    truncated: false,
                }))
            }
            TransactionRebase::Conflicted {
                conflicts,
                truncated,
            } => Ok(Response::new(wire::RebaseTransactionResponse {
                base: Some(generation_ref(&base)),
                conflicts: conflicts.into_iter().map(transaction_conflict).collect(),
                truncated,
            })),
        }
    }

    async fn fork_workspace(
        &self,
        request: Request<wire::ForkWorkspaceRequest>,
    ) -> Result<Response<wire::WorkspaceResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let idempotency_key = operation(request.operation)?;
        let source_reference = request.source;
        let source = self.generation(source_reference.clone()).await?;
        let source_workspace = source.workspace.clone();
        let destination = source_workspace
            .with_stable_policy(|| async {
                // Rebind after lease admission: the first lookup may have raced a
                // policy edit. The lease freezes policy, but cannot freeze time.
                let source = self.generation(source_reference).await?;
                if source
                    .workspace
                    .access
                    .as_ref()
                    .is_none_or(|(view, _)| view.valid_until().is_some())
                {
                    return Err(Status::permission_denied(
                        "fork requires nonexpiring source authority",
                    ));
                }
                let options = ForkOptions::from_generation(source, idempotency_key);
                let options = match request.selection {
                    Some(selection) => options.inherit_paths(selection.paths),
                    None => options,
                };
                #[cfg(test)]
                tests::fork_policy_cut(&source_workspace).await;
                source_workspace
                    .fork::<_, _, WorkspaceError>(
                        request.destination_name,
                        options,
                        crate::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                        &mut |_| {
                            std::future::ready(Ok(crate::OperationReceipt {
                                value: (),
                                work: crate::WorkCounters::UNCHARGED,
                            }))
                        },
                    )
                    .await
                    .map_err(|failure| status(&failure.error))
                    .map(|receipt| receipt.value)
            })
            .await
            .map_err(|error| status(&error))??;
        Ok(Response::new(wire::WorkspaceResponse {
            workspace: Some(workspace_message(&destination).await?),
            status: wire::MutationStatus::Committed as i32,
        }))
    }

    async fn diff(
        &self,
        request: Request<wire::DiffRequest>,
    ) -> Result<Response<wire::DiffResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_changes == 0 || request.maximum_changes > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument("diff bound is invalid"));
        }
        let from = self.generation(request.from).await?;
        let to = self.generation(request.to).await?;
        let workspace = from.workspace.clone();
        let changes = workspace
            .diff(
                &from,
                &to,
                request.maximum_changes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        Ok(Response::new(wire::DiffResponse {
            from: Some(generation_ref(changes.from())),
            to: Some(generation_ref(changes.to())),
            files: changes
                .changes()
                .files
                .iter()
                .map(file_record_change)
                .collect(),
            bindings: changes
                .changes()
                .bindings
                .iter()
                .map(directory_binding_change)
                .collect(),
            truncated: changes.changes().truncated,
            work: Some(work_counters(changes.work())),
        }))
    }

    async fn rebase(
        &self,
        request: Request<wire::RebaseRequest>,
    ) -> Result<Response<wire::RebaseResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_generations == 0
            || request.maximum_generations > self.limits.maximum_page_items
            || request.maximum_changes == 0
            || request.maximum_changes > self.limits.maximum_page_items
            || request.maximum_conflicts == 0
            || request.maximum_conflicts > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument("rebase bounds are invalid"));
        }
        let workspace = self.workspace(request.workspace).await?;
        let outcome = workspace
            .live_rebase(
                operation(request.operation)?,
                request.maximum_generations,
                request.maximum_changes,
                request.maximum_conflicts,
            )
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(rebase_message(outcome)))
    }

    async fn plan_join(
        &self,
        request: Request<wire::PlanJoinRequest>,
    ) -> Result<Response<wire::JoinPlan>, Status> {
        self.rpc_plan_join(request).await
    }

    async fn apply_join(
        &self,
        request: Request<wire::ApplyJoinRequest>,
    ) -> Result<Response<wire::JoinResponse>, Status> {
        self.rpc_apply_join(request).await
    }

    async fn checkpoint(
        &self,
        request: Request<wire::RetainGenerationRequest>,
    ) -> Result<Response<wire::RetainGenerationResponse>, Status> {
        self.retain(request, true).await
    }

    async fn pin(
        &self,
        request: Request<wire::RetainGenerationRequest>,
    ) -> Result<Response<wire::RetainGenerationResponse>, Status> {
        self.retain(request, false).await
    }

    type ExportStream = WireStream<wire::ExportChunk>;

    async fn export(
        &self,
        request: Request<wire::ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        self.rpc_export(request).await
    }

    async fn import(
        &self,
        request: Request<tonic::Streaming<wire::ImportChunk>>,
    ) -> Result<Response<wire::ImportResponse>, Status> {
        self.rpc_import(request).await
    }

    async fn issue_mount_credential(
        &self,
        request: Request<wire::CredentialRequest>,
    ) -> Result<Response<wire::CredentialResponse>, Status> {
        self.issue_credential(request, CredentialKind::Mount).await
    }

    async fn issue_s3_credential(
        &self,
        request: Request<wire::CredentialRequest>,
    ) -> Result<Response<wire::CredentialResponse>, Status> {
        self.issue_credential(request, CredentialKind::S3).await
    }

    async fn get_source_state(
        &self,
        request: Request<wire::SourceStateRequest>,
    ) -> Result<Response<wire::SourceResponse>, Status> {
        self.admit(&request)?;
        let (workspace, scope) = self.source_scope(request.into_inner().workspace).await?;
        let result = self.source_provider()?.state(scope).await?;
        Ok(Response::new(
            self.source_response(&workspace, result).await?,
        ))
    }

    async fn reconcile_source(
        &self,
        request: Request<wire::SourceOperationRequest>,
    ) -> Result<Response<wire::SourceResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let idempotency_key = operation(request.operation)?;
        let (workspace, scope) = self.source_scope(request.workspace).await?;
        let result = self
            .source_provider()?
            .reconcile(HostedSourceOperation {
                scope,
                idempotency_key,
            })
            .await?;
        Ok(Response::new(
            self.source_response(&workspace, result).await?,
        ))
    }

    async fn rescan_source(
        &self,
        request: Request<wire::SourceOperationRequest>,
    ) -> Result<Response<wire::SourceResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let idempotency_key = operation(request.operation)?;
        let (workspace, scope) = self.source_scope(request.workspace).await?;
        let result = self
            .source_provider()?
            .rescan(HostedSourceOperation {
                scope,
                idempotency_key,
            })
            .await?;
        Ok(Response::new(
            self.source_response(&workspace, result).await?,
        ))
    }

    async fn seal_source(
        &self,
        request: Request<wire::SourceOperationRequest>,
    ) -> Result<Response<wire::SourceResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let idempotency_key = operation(request.operation)?;
        let (workspace, scope) = self.source_scope(request.workspace).await?;
        let result = self
            .source_provider()?
            .seal(HostedSourceOperation {
                scope,
                idempotency_key,
            })
            .await?;
        Ok(Response::new(
            self.source_response(&workspace, result).await?,
        ))
    }

    async fn observe(
        &self,
        request: Request<wire::ObserveRequest>,
    ) -> Result<Response<wire::ObserveResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let workspace = self.workspace(request.workspace).await?;
        let operation_id = raw_operation(&request.operation_id)?;
        let observed = self
            .filesystem
            .observe_volume_operation(
                workspace.id().volume_id(),
                operation_id,
                crate::WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| Status::unavailable(failure.error.to_string()))?;
        let response = match observed.value {
            Some(commit) => wire::ObserveResponse {
                state: "completed".to_owned(),
                outcome: Some(observed_mutation(&workspace, &commit)?),
            },
            None => wire::ObserveResponse {
                state: "unknown".to_owned(),
                outcome: None,
            },
        };
        Ok(Response::new(response))
    }

    async fn cancel(
        &self,
        request: Request<wire::CancelRequest>,
    ) -> Result<Response<wire::CancelResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let observed = self
            .observe(Request::new(wire::ObserveRequest {
                workspace: request.workspace,
                operation_id: request.operation_id,
            }))
            .await?
            .into_inner();
        Ok(Response::new(wire::CancelResponse {
            operation: Some(observed),
        }))
    }
}
