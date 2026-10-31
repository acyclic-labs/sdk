use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(super) fn require_unchanged_content<'a>(
        &'a self,
        workspace: &'a Workspace<A, O>,
        original: &'a GenerationRef,
        authorized: &'a GenerationRef,
        paths: [&'a str; 2],
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let owned;
            let io = if let Some(io) = io {
                io
            } else {
                owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &owned
            };
            let original = self.generation(workspace, original, Some(io)).await?;
            let authorized = self.generation(workspace, authorized, Some(io)).await?;
            // The protected metadata distinguishes publications of equal bytes;
            // the file payload also catches writes that bypassed that metadata.
            for path in paths {
                if !io.sdk(
                    authorized
                        .matches_file_payload(&original, path, io, io.cancellation())
                        .await,
                )? {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Conflict,
                        "source file changed before content authorization",
                    )
                    .tool_input());
                }
            }
            Ok(())
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[tokio::test]
async fn changed_content_refuses_as_tool_feedback_without_reclassifying_native_faults() -> Result<()>
{
    let filesystem = Fs::memory();
    let provider = ProviderRef::new("fixture".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
    filesystem
        .create_workspace(
            "stale-content",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| map_error(failure.error))?;
    let reference = workspace_ref(provider, "stale-content")?;
    let base = host.resolve(&reference, None).await?.generation;
    let original = host
        .apply(
            &reference,
            Some(&base),
            &[WorkspaceMutation::PutFile {
                path: "/file.txt".into(),
                bytes: b"read".to_vec().into(),
            }],
            &IdempotencyKey::new("source")?,
        )
        .await?;
    let changed = host
        .apply(
            &reference,
            Some(&original),
            &[WorkspaceMutation::PutFile {
                path: "/file.txt".into(),
                bytes: b"changed".to_vec().into(),
            }],
            &IdempotencyKey::new("change")?,
        )
        .await?;
    let workspace = filesystem
        .open_workspace(
            "stale-content",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| map_error(failure.error))?;
    let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
    host.require_unchanged_content(
        &workspace,
        &original,
        &original,
        ["/file.txt", "/file.txt"],
        Some(&io),
    )?
    .await?;
    let error = host
        .require_unchanged_content(
            &workspace,
            &original,
            &changed,
            ["/file.txt", "/file.txt"],
            Some(&io),
        )?
        .await
        .unwrap_err();
    assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
    assert!(error.is_tool_failure());
    assert!(!error.uncertain());
    assert!(
        !Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: "native accounting failed"
        }
        .is_tool_failure()
    );
    assert_eq!(host.resolve(&reference, None).await?.generation, changed);
    Ok(())
}
