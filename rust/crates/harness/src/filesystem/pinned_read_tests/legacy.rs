use super::*;

pub(super) async fn read_pinned(
    host: &Host,
    reference: &FileRef,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    crate::conversation::BoundedContentFuture::new(io, || {
        original_future(host, reference, maximum_bytes, io)
    })?
    .await
}

fn original_future<'a>(
    host: &'a Host,
    reference: &'a FileRef,
    maximum_bytes: u64,
    io: Option<&'a PreparationIo>,
) -> impl std::future::Future<Output = Result<acyclic_stream::OwnedBytes>>
+ acyclic_stream::ProviderTask
+ 'a {
    async move {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = io.and_then(PreparationIo::original_native_work) {
            let scratch = PreparationIo::from_native_work(original)?;
            let io = &scratch;
            reference.validate(&mut Some(&mut io.history_io()))?;
            if reference.volume().provider() != &host.provider
                || reference.descriptor().byte_length() > maximum_bytes
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "content provider or size limit mismatch",
                });
            }
            let generation = host.file_generation(reference, Some(io))?;
            let workspace =
                fork_preparer_io::content_workspace(&host.provider, reference.volume(), Some(io))?;
            let rendered = original
                .display(&format_args!("/{}", reference.path()))
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            let path_custody = rendered.1;
            let path = rendered.0;
            let opened = (host).open(&workspace, Some(io)).await?;
            let pinned = (host).generation(&opened, &generation, Some(io)).await?;
            acyclic_test_support::differential::record(("read", maximum_bytes));
            let bytes = pinned
                .read(&path, maximum_bytes, original, io.cancellation())
                .await
                .map_err(|source| Error::NativeWorkspace {
                    source: Box::new(source),
                    custody: None,
                })?;
            fork_preparer_io::retain(&pinned, Some(io)).await?;
            crate::executor::with_history_io(Some(io), |work| {
                reference.descriptor().verify(&bytes, work)
            })?;
            drop(path);
            drop(path_custody);
            return Ok(bytes);
        }
        if reference.volume().provider() != &host.provider
            || reference.descriptor().byte_length() > maximum_bytes
        {
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        if let Some(io) = io {
            io.retain_owned((reference.path().len() + 1) as u64)?;
        }
        let _path_hold = io
            .map(|io| io.guard_owned((reference.path().len() + 1) as u64))
            .transpose()?;
        let generation = host.file_generation(reference, io)?;
        let generation_hold = io
            .map(|io| io.guard_owned(generation.logical_owned_bytes()?))
            .transpose()?;
        let workspace =
            fork_preparer_io::content_workspace(&host.provider, reference.volume(), io)?;
        let _workspace_hold = io
            .map(|io| io.guard_owned(workspace.logical_owned_bytes()?))
            .transpose()?;
        let path = format!("/{}", reference.path());
        let bytes = match io {
            Some(io) => {
                let opened = (host).open(&workspace, Some(io)).await?;
                let pinned = (host).generation(&opened, &generation, Some(io)).await?;
                acyclic_test_support::differential::record(("read", maximum_bytes));
                let bytes = io.receive(
                    pinned
                        .read(&path, maximum_bytes, io.remaining()?, io.cancellation())
                        .await,
                )?;
                let capacity = bytes.retained_capacity().ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Unsupported,
                        "content producer does not expose its owned capacity",
                    )
                })?;
                io.retain_owned(capacity as u64)?;
                fork_preparer_io::retain(&pinned, Some(io)).await?;
                bytes
            }
            None => {
                host.read(&workspace, Some(&generation), &path, maximum_bytes, None)
                    .await?
            }
        };
        crate::executor::with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
        drop(generation);
        drop(generation_hold);
        Ok(bytes)
    }
}

// Independent reference for the representation selected by an unmetered read.
// Keep the original metered/native state machine above as the baseline.
fn unmetered_future<'a>(
    host: &'a Host,
    reference: &'a FileRef,
    maximum_bytes: u64,
) -> impl std::future::Future<Output = Result<acyclic_stream::OwnedBytes>>
+ acyclic_stream::ProviderTask
+ 'a {
    async move {
        if reference.volume().provider() != &host.provider
            || reference.descriptor().byte_length() > maximum_bytes
        {
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        let generation = host.file_generation(reference, None)?;
        let workspace =
            fork_preparer_io::content_workspace(&host.provider, reference.volume(), None)?;
        let path = format!("/{}", reference.path());
        let bytes = host
            .read(&workspace, Some(&generation), &path, maximum_bytes, None)
            .await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        Ok(bytes)
    }
}

pub(super) fn saved_frame_bytes(host: &Host, reference: &FileRef, maximum_bytes: u64) -> u64 {
    (std::mem::size_of_val(&original_future(host, reference, maximum_bytes, None))
        - std::mem::size_of_val(&unmetered_future(host, reference, maximum_bytes))) as u64
}
