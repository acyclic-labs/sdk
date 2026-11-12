use super::workspaces::FilePublication;
use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// The kernel owns journal policy. Re-admission changes only that policy,
    /// through the same authorized publication as ordinary workspace writes.
    pub(in crate::filesystem) fn enforce_journal_policy<'a, F, Fut>(
        &'a self,
        volume: &'a VolumeRef,
        verifier: &'a AuthorityVerifier,
        host_scope: &'a Scope,
        authorize: F,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>
    where
        F: FnOnce() -> Fut + acyclic_stream::ProviderTask + 'a,
        Fut: Future<Output = Result<ContentGrant>> + acyclic_stream::ProviderTask + 'a,
    {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let owned;
            let io = match io {
                Some(io) => io,
                None => {
                    owned = PreparationIo::for_unmeasured(&CancellationToken::new())?;
                    &owned
                }
            };
            verifier.verify(host_scope, &mut Some(&mut io.history_io()))?;
            if volume.provider() != &self.provider
                || !volume.is_agent_journal()
                || !host_scope
                    .capabilities()
                    .contains(crate::contract::capability::HARNESS_HOST)
            {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "journal policy requires its host binding",
                ));
            }
            let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
            let _reference = io.guard_owned(reference.logical_owned_bytes()?)?;
            let workspace = self.open(&reference, Some(io)).await?;
            let head = self.current_generation(&workspace, io).await?;
            let bytes = crate::executor::with_history_io(Some(io), |work| {
                crate::core::copy_copy_vector(acyclic_fs::access::POLICY_METADATA_KEY, work)
            })?;
            let _key = io.guard_owned(bytes.capacity() as u64)?;
            let name = AttributeName::new(AttributeClass::PosixXattr, bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let recorded = read_file_annotation(&head, "", &name, None, Some(io)).await?;
            let _recorded = recorded
                .as_ref()
                .map(|bytes| io.guard_bytes(bytes))
                .transpose()?;
            let expected = include_bytes!("journal-policy.json");
            if recorded.as_deref() == Some(expected.as_slice()) {
                return Ok(());
            }
            let grant = authorize().await?;
            if !grant
                .policy_scope()
                .capabilities()
                .contains(crate::contract::capability::HARNESS_HOST)
            {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "journal policy publication requires host custody",
                ));
            }
            grant.require(volume, VolumeOperation::Write, Some(io))?;
            let scope = grant.policy_scope();
            let label = crate::core::filesystem_scope_label(
                scope.capabilities(),
                &mut Some(&mut io.history_io()),
            )?;
            let payload = match label {
                Some(label) => access::capability_payload(Some(label), VolumeOperation::Write, io)?,
                None => access::whole_volume_payload(acyclic_fs::access::ActionSet::Owner)?,
            };
            let view = access::access_view(&workspace, scope, None, payload, None, io).await?;
            access::require(&view.0, acyclic_fs::access::Action::Admin, "", io)?;
            let fence = view
                .0
                .publication_fence()
                .ok_or_else(|| io.invalid("journal policy has no publication fence"))?;
            let work = WorkCounters {
                bytes_hashed: (volume.id().len() + head.id().to_bytes().len() + expected.len())
                    as u64,
                ..WorkCounters::UNCHARGED
            };
            io.preflight(work)?;
            let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.journal-policy.v1");
            hash.update(volume.id().as_bytes());
            hash.update(head.id().to_bytes().as_slice());
            hash.update(expected);
            let mut identity = [0; 16];
            identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
            if identity == [0; 16] {
                identity[0] = 1;
            }
            io.charge(work)?;
            let mut transaction = io.sdk(
                workspace
                    .begin_transaction_at(
                        &head,
                        FilesystemKey::from_bytes(identity),
                        io,
                        io.cancellation(),
                    )
                    .await,
            )?;
            io.sdk(
                transaction
                    .observe_policy_fence(fence, io, io.cancellation())
                    .await,
            )?;
            io.sdk(
                transaction
                    .change_named_attribute(
                        "/",
                        &name,
                        Some((expected, acyclic_fs::NamedAttributeWriteMode::Upsert)),
                        io,
                        io.cancellation(),
                    )
                    .await,
            )?;
            Self::commit_contextual_publication(
                transaction,
                io,
                FilePublication::Authorized(fence, &grant, None),
                None,
            )?
            .await?;
            Ok(())
        })
    }
}
