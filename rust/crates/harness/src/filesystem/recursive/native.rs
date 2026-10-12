use super::*;
/// Rebinds the immutable request to the original native authenticated cut.
#[cfg(not(target_arch = "wasm32"))]
#[allow(
    clippy::too_many_arguments,
    reason = "original providers and transferred custody stay explicit"
)]
pub async fn restore_recursive_fork_source_with_native_work<P, A, O>(
    filesystem: Arc<crate::filesystem::FilesystemHost<A, O>>,
    stream: &Arc<P>,
    parent: crate::core::Reducer,
    verifier: &crate::core::AuthorityVerifier,
    signed: &crate::core::Scope,
    caller_project: &VolumeRef,
    stream_provider: &crate::resources::ProviderRef,
    request: (ForkRequest, acyclic_fs::NativeAllocation),
    history_limits: crate::store::HistoryReadLimits,
    source_read: Option<&crate::source_authorization::ProjectSourceReadHandle>,
    resolver: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    cancellation: &acyclic_fs::CancellationToken,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    cancellation
        .check()
        .map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let mut history = io.history_io();
    let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
    request.0.validate(&mut observer)?;
    if &request.0.parent != parent.authority() || request.0.parent_revision != parent.revision() {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: "restored source differs from original admitted parent cut",
        });
    }
    verifier.verify_scope_for_with_work(parent.authority(), signed, &mut observer)?;
    if request.0.authorized_source.as_ref() != source_read.map(|handle| handle.authorization()) {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "restored source differs from immutable operation approval",
        });
    }
    let reader_guard;
    let reader_before = io.owned_bytes()?;
    let reader =
        crate::store::HistoryReader::new(stream, parent.authority(), verifier, &mut observer)?;
    reader_guard = io.guard_owned(io.owned_bytes()?.checked_sub(reader_before).ok_or(
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "history reader custody underflows",
        },
    )?)?;
    let retained_history = reader
        .fork_history(
            &parent,
            request.0.preparation.inherited_through_sequence,
            history_limits,
            &mut observer,
        )
        .await?;
    drop(reader);
    drop(reader_guard);
    cancellation
        .check()
        .map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
    let owned_verifier = verifier.copy(&mut observer)?;
    let owned_scope = signed.copy(&mut observer)?;
    let project = caller_project.copy(&mut observer)?;
    drop(observer);
    drop(history);
    let provider = stream_provider.clone_with_io(&io)?;
    let selected_read = match source_read {
        Some(handle) => {
            let mut history = io.history_io();
            let authorization = handle
                .authorization()
                .clone_with_work_and_custody(&mut Some(&mut history))?
                .0;
            drop(history);
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> =
                Some(&mut history);
            Some(crate::source_authorization::ProjectSourceReadHandle::seal(
                authorization,
                verifier,
                &mut observer,
            )?)
        }
        None => None,
    };
    let retained = io.owned_bytes()?;
    let preparer = FilesystemForkPreparer::new(
        filesystem,
        parent,
        owned_verifier,
        owned_scope,
        project,
        provider,
        resolver,
        Some(retained_history),
        selected_read,
        Some((io.into_native_custody(retained)?, work)),
    )?
    .into_native_shared(work)?;
    Ok((
        RetainedSourceBinding {
            preparer,
            request: request.0,
        },
        request.1,
    ))
}
