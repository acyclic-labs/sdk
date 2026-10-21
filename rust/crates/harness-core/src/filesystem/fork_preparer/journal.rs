//! Fork journal serialization and immutable record checks.
use super::*;
pub(super) fn preparation_journal(
    provider: &ProviderRef,
    operation: OperationId,
    io: Option<&PreparationIo>,
) -> Result<WorkspaceRef> {
    let name = crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(
            format_args!("harness-fork-preparation-{operation}"),
            work,
        )
    })?;
    let provider = match io {
        Some(io) => provider.clone_with_io(io)?,
        None => provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}

pub(super) fn allocation_ref(
    provider: &ProviderRef,
    volume: &VolumeRef,
    io: Option<&PreparationIo>,
) -> Result<WorkspaceRef> {
    let volume_name = crate::executor::with_history_io(io, |work| volume.storage_name(work))?;
    let hold = io
        .map(|io| io.guard_owned(volume_name.capacity() as u64))
        .transpose()?;
    let name = crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(format_args!("fork-allocation-{volume_name}"), work)
    })?;
    drop(volume_name);
    drop(hold);
    let provider = match io {
        Some(io) => provider.clone_with_io(io)?,
        None => provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}

pub(crate) fn seed_digest(
    seed: &ForkSeed,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    let mut history = io;
    seed.validate(
        &mut history
            .as_mut()
            .map(|history| history as &mut dyn crate::store::history::HistoryIo),
    )?;
    let digest = record_digest(seed, maximum_bytes, io)?;
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    acyclic_native_runtime::differential::record(("fork seed digest", 1u64));
    Ok(digest)
}

pub(super) fn record_digest<T: Serialize>(
    value: &T,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    let encoded_hold;
    let encoded = encode(value, maximum_bytes, io)?;
    encoded_hold = io.map(|io| io.guard_bytes(&encoded)).transpose()?.flatten();
    let digest = crate::model::hash_bytes(&encoded, io)?;
    drop(encoded);
    drop(encoded_hold);
    Ok(digest)
}

pub(super) async fn write_record<A: AsyncAuthorityStore, O: AsyncObjectStore, T: Serialize>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    value: &T,
    maximum_bytes: u64,
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
    preparation: Option<&Arc<TaskResourcePreparation>>,
) -> Result<()> {
    let bytes = encode(value, maximum_bytes, io)?;
    let hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
    let result = (host)
        .create_records(
            journal,
            &[WorkspaceMutation::PutFile {
                path: stock_string_copy(path, io)?,
                bytes,
            }],
            IdempotencyKey(stock_string_copy(key.as_str(), io)?),
            io,
            preparation,
        )?
        .await;
    drop(hold);
    match result {
        Ok(_) => Ok(()),
        Err(error) if error.code() == crate::wire::ErrorCode::Conflict => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) async fn write_once<A, O, T>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    value: &T,
    maximum: u64,
    key: &IdempotencyKey,
    operation: OperationId,
    io: Option<&PreparationIo>,
    preparation: Option<&Arc<TaskResourcePreparation>>,
) -> Result<T>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    T: Serialize + bounded_io::PreparationRecord,
{
    if let Some(prior) = read_record(host, journal, path, maximum, io).await? {
        return Ok(prior);
    }
    write_record(host, journal, path, value, maximum, key, io, preparation).await?;
    read_record(host, journal, path, maximum, io)
        .await?
        .ok_or(Error::Indeterminate(operation))
}

pub(super) async fn read_claim<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    request: &ForkRequest,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<bool> {
    match read_record::<A, O, ForkRequest>(host, journal, "/request.json", maximum_bytes, io)
        .await?
    {
        Some(claim) if claim == *request => Ok(true),
        Some(_) => Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "fork operation is claimed by another request",
        )),
        None => Ok(false),
    }
}

pub(super) async fn read_report<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    request: &ForkRequest,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<Option<ForkReport>> {
    let report =
        read_record::<A, O, ForkReport>(host, journal, "/report.json", maximum_bytes, io).await?;
    if let Some(report) = &report {
        crate::executor::with_history_io(io, |work| report.validate(work))?;
        if report.request != *request {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "fork report does not match its claimed request",
            ));
        }
    }
    Ok(report)
}

pub(super) fn validate_capture(selection: &ForkSelection, capture: &Capture) -> Result<()> {
    match capture {
        Capture::Captured(resource) if resource.source == selection.revision => {
            resource.validate(&mut None)
        }
        Capture::Captured(_) => Err(Error::Invalid(
            "capture provider substituted another source revision".into(),
        )),
        Capture::Unsupported(reason) if !reason.is_empty() => Ok(()),
        Capture::Unsupported(_) => Err(Error::Invalid("fork capture reason is invalid".into())),
        Capture::InFlight(operation) | Capture::Indeterminate(operation) => {
            Err(Error::Indeterminate(*operation))
        }
    }
}

pub(super) fn encode<T: Serialize>(
    value: &T,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    if let Some(io) = io {
        return io.encode(value, maximum_bytes, false);
    }
    crate::executor::json::json_bytes_bounded(
        value,
        crate::executor::json::JsonBounds {
            maximum_bytes,
            maximum_items: u64::MAX,
            maximum_depth: 256,
        },
        None,
        &mut |_| Ok(()),
    )
    .map(Into::into)
}

pub(super) async fn read_allocation_claim<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<Option<AllocationClaim>> {
    read_record_decoded_bounded(
        host,
        journal,
        "/claim.json",
        maximum_bytes,
        io,
        |bytes, io| match io {
            Some(io) => io.decode_json(bytes),
            None => crate::contract::json_from_slice(bytes)
                .map_err(|error| Error::Invalid(format!("fork allocation is invalid: {error}"))),
        },
    )
    .await
}

pub(super) async fn read_record<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    T: bounded_io::PreparationRecord,
>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<Option<T>> {
    read_record_decoded_bounded(
        host,
        journal,
        path,
        maximum_bytes,
        io,
        |bytes, io| match io {
            Some(io) => Ok(bounded_io::retain_decoded(io.decode(
                std::marker::PhantomData::<T>,
                bytes,
                crate::wire::ErrorCode::Storage,
            )?)?),
            None => crate::contract::json_from_slice(bytes).map_err(|error| {
                Error::Invalid(format!("fork journal record is invalid: {error}"))
            }),
        },
    )
    .await
}

pub(super) async fn read_record_decoded_bounded<A: AsyncAuthorityStore, O: AsyncObjectStore, T>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    maximum_bytes: u64,
    io: Option<&PreparationIo>,
    decode: impl FnOnce(&acyclic_stream::OwnedBytes, Option<&PreparationIo>) -> Result<T>,
) -> Result<Option<T>> {
    let raw;
    match (host).read(journal, None, path, maximum_bytes, io).await {
        Ok(bytes) => {
            raw = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            let value = decode(&bytes, io);
            drop(bytes);
            drop(raw);
            value.map(Some)
        }
        Err(error) if journal_absent(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(in crate::filesystem) fn journal_absent(error: &Error) -> bool {
    match error {
        Error::NotFound(_) => true,
        #[cfg(not(target_arch = "wasm32"))]
        Error::NativeWorkspace { source, .. } => {
            matches!(source.as_ref(), acyclic_fs::WorkspaceError::NotFound)
        }
        _ => false,
    }
}

use crate::executor::{stock_boundary, stock_string_copy};

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;

    async fn first_publication_wins(metered: bool) -> Result<()> {
        let provider = ProviderRef::new(
            "immutable-journal".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
        let reference = crate::filesystem::workspace_ref(provider, "first-winner")?;
        bounded_io::create(
            &host,
            "first-winner",
            &IdempotencyKey::new("create")?,
            None,
            None,
        )
        .await?;
        let ledger = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(1_000_000, 32_000_000),
            &acyclic_fs::CancellationToken::new(),
        )?;
        let io = metered.then_some(&ledger);
        let path = "/capture-start.json";
        // Both writers observed absence. The second writer resumes only after
        // the first publishes, an exact schedule across the read/write awaits.
        assert!(
            read_record::<_, _, Uuid>(&host, &reference, path, 4096, io)
                .await?
                .is_none()
        );
        assert!(
            read_record::<_, _, Uuid>(&host, &reference, path, 4096, io)
                .await?
                .is_none()
        );
        let first = Uuid::from_bytes([1; 16]);
        let second = Uuid::from_bytes([2; 16]);
        write_record(
            &host,
            &reference,
            path,
            &first,
            4096,
            &IdempotencyKey::new("writer-one")?,
            io,
            None,
        )
        .await?;
        write_record(
            &host,
            &reference,
            path,
            &second,
            4096,
            &IdempotencyKey::new("writer-two")?,
            io,
            None,
        )
        .await?;
        assert_eq!(
            read_record::<_, _, Uuid>(&host, &reference, path, 4096, io).await?,
            Some(first)
        );
        Ok(())
    }

    #[tokio::test]
    async fn resumed_unmetered_writer_cannot_replace_the_first_capture_attempt() -> Result<()> {
        first_publication_wins(false).await
    }

    #[tokio::test]
    async fn resumed_metered_writer_cannot_replace_the_first_capture_attempt() -> Result<()> {
        first_publication_wins(true).await
    }
}
