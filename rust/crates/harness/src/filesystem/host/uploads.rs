use super::*;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod properties;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one implementation pins the complete staged file contract"
    )]
    pub(in crate::filesystem) async fn put_content_impl(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        internal_class: Option<InternalContentClass>,
        expected_generation: Option<&GenerationRef>,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<FileRef>
    where
        A: 'static,
        O: 'static,
    {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            grant.require(volume, VolumeOperation::Write, io)?;
            if volume.provider() != &self.provider || bytes.len() as u64 > maximum_bytes {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "content provider or size limit mismatch",
                ));
            }
            crate::executor::with_history_io(io, |work| {
                crate::conversation::validate_content_path(path, work)
            })?;
            let descriptor = crate::executor::with_history_io(io, |work| {
                FileDescriptor::from_bytes(bytes, media_type, work)
            })?;
            let operation = fork_preparer_io::operation_key(idempotency_key, io)?;
            let provisional = crate::executor::with_history_io(io, |work| {
                FileRef::new(
                    volume.copy(work)?,
                    stock_string_copy(path, io)?,
                    fork_preparer_io::hex_key(&operation.into_bytes(), io)?,
                    FileDescriptor::new(
                        *descriptor.sha256(),
                        descriptor.byte_length(),
                        stock_string_copy(descriptor.media_type(), io)?,
                    )?,
                    stock_string_copy(display_name, io)?,
                    work,
                )
            })?;
            match internal_class {
                Some(class)
                    if volume.class() == VolumeClass::AgentPrivate
                        && provisional.path().starts_with(class.prefix()) => {}
                Some(_) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    if io.and_then(PreparationIo::original_native_work).is_some() {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unauthorized,
                            reason: "journal path or volume is not assigned to this host",
                        });
                    }
                    return Err(Error::Unauthorized(
                        "journal path or volume is not assigned to this host".into(),
                    ));
                }
                None if provisional.path() == ".system"
                    || provisional.path().starts_with(".system/")
                    || provisional.path() == "turns/.canonical"
                    || provisional.path().starts_with("turns/.canonical/") =>
                {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "internal storage paths are reserved",
                    ));
                }
                None => {}
            }
            let receipt_identity = if internal_class.is_some() {
                provisional.path()
            } else {
                idempotency_key.as_str()
            };
            let receipt_path =
                content_identity_path_io(receipt_identity, false, ".system/harness-uploads/", io)?;
            let metadata_path = content_metadata_path_io(provisional.path(), false, io)?;
            let metadata_bytes = match io {
                Some(io) => io.encode(&provisional, io.remaining()?.bytes_encoded, false)?,
                None => acyclic_stream::OwnedBytes::from(
                    serde_json::to_vec(&provisional)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                ),
            };
            let receipt_bytes = match io {
                Some(io) => io.encode(
                    &(&provisional, expected_generation),
                    io.remaining()?.bytes_encoded,
                    true,
                )?,
                None => acyclic_stream::OwnedBytes::from(crate::contract::canonical_json_bytes(
                    &(&provisional, expected_generation),
                )?),
            };
            let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
            let prior = crate::conversation::BoundedContentFuture::new(io, || async {
                let opened = match io {
                    Some(io) => (self).open(&workspace, Some(io)).await?,
                    None => self.open(&workspace, None).await?,
                };
                Ok(match io {
                    Some(io) => {
                        let hash = acyclic_fs::WorkCounters {
                            bytes_hashed: idempotency_key.as_str().len() as u64,
                            ..Default::default()
                        };
                        io.preflight(hash)?;
                        let key = filesystem_key(idempotency_key);
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(original) = io.original_native_work() {
                            original.spent(hash).map_err(|cause| Error::NativeWork {
                                cause,
                                custody: None,
                            })?;
                            opened
                                .operation_generation(key, original, io.cancellation())
                                .await
                                .map_err(|source| Error::NativeWorkspace {
                                    source: Box::new(source),
                                    custody: None,
                                })?
                        } else {
                            io.charge(hash)?;
                            io.receive(
                                opened
                                    .operation_generation(key, io.remaining()?, io.cancellation())
                                    .await,
                            )?
                        }
                        #[cfg(target_arch = "wasm32")]
                        {
                            io.charge(hash)?;
                            io.receive(
                                opened
                                    .operation_generation(key, io.remaining()?, io.cancellation())
                                    .await,
                            )?
                        }
                    }
                    None => opened
                        .operation_generation(
                            filesystem_key(idempotency_key),
                            acyclic_fs::WorkBudget::UNBOUNDED,
                            &acyclic_fs::CancellationToken::new(),
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| map_error(failure.error))?,
                })
            })?
            .await?;
            if let Some(prior) = prior {
                let generation = (self).generation_ref_id(prior.id(), io)?;
                match io {
                    Some(io) => {
                        crate::conversation::BoundedContentFuture::new(Some(io), || {
                            fork_preparer_io::retain(&prior, Some(io))
                                .map(|result| result.map_err(Error::from))
                        })?
                        .await?
                    }
                    None => {
                        crate::conversation::BoundedContentFuture::new(None, || {
                            self.retain_generation(&workspace, &generation)
                        })?
                        .await?
                    }
                }
                let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
                let receipt_path = crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(format_args!("/{receipt_path}"), work)
                })?;
                const RECORD_BYTES: u64 = 6
                    * (crate::conversation::MAX_PATH_BYTES as u64
                        + 32 * crate::conversation::MAX_LABEL_BYTES as u64
                        + 1024);
                let original_receipt = crate::conversation::BoundedContentFuture::new(io, || {
                    read_generation(&prior, &receipt_path, RECORD_BYTES, io)
                })?
                .await?;
                if original_receipt.as_ref() != receipt_bytes.as_ref() {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "upload identity belongs to a different request",
                    ));
                }
                drop(original_receipt);
                let indexed = match io {
                    Some(io) => {
                        let indexed_path = crate::executor::with_history_io(Some(io), |work| {
                            crate::executor::preparation_format(
                                format_args!("/{metadata_path}"),
                                work,
                            )
                        })?;
                        crate::conversation::BoundedContentFuture::new(Some(io), || {
                            read_generation(
                                &prior,
                                &indexed_path,
                                metadata_bytes.len() as u64,
                                Some(io),
                            )
                        })?
                        .await?
                    }
                    None => {
                        let indexed_path = format!("/{metadata_path}");
                        crate::conversation::BoundedContentFuture::new(None, || {
                            self.read(
                                &workspace,
                                Some(&generation),
                                &indexed_path,
                                metadata_bytes.len() as u64,
                                None,
                            )
                        })?
                        .await?
                    }
                };
                if indexed.as_ref() != metadata_bytes.as_slice() {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "upload metadata index does not match",
                    ));
                }
                let reference = {
                    let descriptor = &descriptor;
                    crate::executor::with_history_io(io, |work| {
                        crate::conversation::FileRef::new(
                            (volume).copy(work)?,
                            stock_string_copy(path, io)?,
                            stock_string_copy(&version, io)?,
                            crate::conversation::FileDescriptor::new(
                                *descriptor.sha256(),
                                descriptor.byte_length(),
                                stock_string_copy(descriptor.media_type(), io)?,
                            )?,
                            stock_string_copy(display_name, io)?,
                            work,
                        )
                    })
                }?;
                let committed = crate::conversation::BoundedContentFuture::new(io, || {
                    self.read_pinned_io(&reference, maximum_bytes, io)
                })?
                .await?;
                if committed.as_ref() != bytes {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "upload identity belongs to different content",
                    ));
                }
                return Ok(reference);
            }
            let absolute = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("/{}", provisional.path()), work)
            })?;
            let mut mutations = crate::model::vec_with_capacity(6, io)?;
            if let Some((parent, _)) = absolute.rsplit_once('/')
                && !parent.is_empty()
            {
                mutations.push(WorkspaceMutation::CreateDirectory {
                    path: stock_string_copy(parent, io)?,
                });
            }
            let owned_bytes = fork_preparer_io::clone_bytes(bytes, io)?;
            mutations.push(WorkspaceMutation::PutFile {
                path: absolute,
                bytes: owned_bytes,
            });
            mutations.push(WorkspaceMutation::CreateDirectory {
                path: stock_string_copy("/.system/harness-uploads", io)?,
            });
            mutations.push(WorkspaceMutation::CreateDirectory {
                path: stock_string_copy("/.system/harness-file-metadata", io)?,
            });
            mutations.push(WorkspaceMutation::PutFile {
                path: crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(format_args!("/{metadata_path}"), work)
                })?,
                bytes: metadata_bytes,
            });
            mutations.push(WorkspaceMutation::PutFile {
                path: crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(format_args!("/{receipt_path}"), work)
                })?,
                bytes: receipt_bytes,
            });
            let generation = self
                .apply_generation(
                    &workspace,
                    expected_generation,
                    &mutations,
                    IdempotencyKey(stock_string_copy(idempotency_key.as_str(), io)?),
                    io,
                    None,
                )?
                .await?;
            let generation_reference = (self).generation_ref_id(generation.id(), io)?;
            let version = fork_preparer_io::hex_key(generation_reference.as_resource().key(), io)?;
            {
                match io {
                    Some(io) => {
                        crate::conversation::BoundedContentFuture::new(Some(io), || {
                            fork_preparer_io::retain(&generation, Some(io))
                                .map(|result| result.map_err(Error::from))
                        })?
                        .await?
                    }
                    None => {
                        let pin = format!("harness-generation-{version}");
                        let cancellation = acyclic_fs::CancellationToken::new();
                        crate::conversation::BoundedContentFuture::new(None, || {
                            generation
                                .pin(pin, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                                .map(|result| {
                                    result
                                        .map(|receipt| receipt.value)
                                        .map_err(|failure| map_error(failure.error))
                                })
                        })?
                        .await?;
                    }
                }
            }
            let reference = {
                let descriptor = &descriptor;
                crate::executor::with_history_io(io, |work| {
                    crate::conversation::FileRef::new(
                        (volume).copy(work)?,
                        stock_string_copy(path, io)?,
                        stock_string_copy(&version, io)?,
                        crate::conversation::FileDescriptor::new(
                            *descriptor.sha256(),
                            descriptor.byte_length(),
                            stock_string_copy(descriptor.media_type(), io)?,
                        )?,
                        stock_string_copy(display_name, io)?,
                        work,
                    )
                })
            }?;
            crate::conversation::BoundedContentFuture::new(io, || {
                self.read_pinned_io(&reference, maximum_bytes, io)
            })?
            .await?;
            Ok(reference)
        })?
        .await
    }
}
