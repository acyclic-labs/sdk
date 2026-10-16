use super::TestResult;
use acyclic_fs::{Fs, LocalAuthorityBackend, LocalObjectBackend, LocalOptions};
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentVerifier, FilesystemHost, WorkspaceMutation},
    resources::WorkspaceRef,
};
use std::{path::Path, sync::Arc};

pub(super) type Host = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
pub(super) type Reader = FilesystemContentVerifier<LocalAuthorityBackend, LocalObjectBackend>;

pub(super) fn temporary() -> std::io::Result<tempfile::TempDir> {
    // Real native files, including journal syncs and cold reopen, without
    // competing with the build box's persistent disk journal.
    #[cfg(target_os = "linux")]
    return tempfile::tempdir_in("/dev/shm");
    #[cfg(not(target_os = "linux"))]
    tempfile::tempdir()
}

pub(super) async fn seed(
    directory: &Path,
    seed: &std::collections::BTreeMap<String, String>,
) -> TestResult<(Arc<Host>, VolumeRef, WorkspaceRef, Arc<Reader>)> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let provider = acyclic_harness::resources::ProviderRef::new(
        "diff-e2e".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(
        Fs::local(LocalOptions::new(directory)).await?,
        &provider,
        None,
    )?;
    let volume = VolumeRef::new(
        provider,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("diff-e2e".into()),
        &mut None,
    )?;
    let workspace = host.create_volume(&volume).await?.workspace;
    let mut writes = vec![WorkspaceMutation::CreateDirectory {
        path: "/plans".into(),
    }];
    writes.extend(seed.iter().map(|(path, text)| WorkspaceMutation::PutFile {
        path: format!("/{path}"),
        bytes: text.as_bytes().to_vec().into(),
    }));
    host.apply(&workspace, None, &writes, &IdempotencyKey::new("seed")?)
        .await?;
    let issuer = AuthorityIssuer::new(
        "diff-reader",
        [9; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "diff-reader".into(),
        },
    );
    let scope = issuer.issue(
        "reader",
        &Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.directory_read_capability("plans", &mut None)?,
        ]),
        Some(AgentId::new()),
        None,
        &mut None,
    )?;
    let reader = FilesystemContentVerifier::new(
        host.clone(),
        &issuer.verifier(&mut None)?,
        &scope,
        65_536,
        None,
    )?;
    Ok((host, volume, workspace, reader))
}
