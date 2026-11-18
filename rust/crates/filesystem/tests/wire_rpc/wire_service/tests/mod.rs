use acyclic_fs::{FilesystemWireLimits, FilesystemWireService, Fs};
use acyclic_fs_wire::wire::filesystem::v2 as wire;
use futures::StreamExt;
use std::sync::Arc;
use tonic::{Request, Status};
use wire::filesystem_service_server::FilesystemService as _;

#[path = "../../../support/wire_requests.rs"]
mod requests;
use requests::{mutation, operation, operation_bytes};

mod authority_error_tests;
mod status_tests;
mod transfer_tests;

#[tokio::test]
async fn handshake_and_creation_expose_every_canonical_profile()
-> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let advertised = service
        .handshake(Request::new(wire::HandshakeRequest { protocol: None }))
        .await?
        .into_inner()
        .capabilities
        .ok_or("missing capabilities")?;
    let profiles = [
        wire::FilesystemProfile::Portable,
        wire::FilesystemProfile::Posix,
        wire::FilesystemProfile::Windows,
        wire::FilesystemProfile::Browser,
    ];
    assert_eq!(advertised.profiles, profiles.map(|profile| profile as i32));
    for (index, selected) in profiles.into_iter().enumerate() {
        let created = service
            .create_workspace(Request::new(wire::CreateWorkspaceRequest {
                name: format!("profile-{index}"),
                profile: selected as i32,
                operation: operation(u8::try_from(index + 1)?),
            }))
            .await?
            .into_inner()
            .workspace
            .ok_or("missing workspace")?;
        assert_eq!(created.profile, selected as i32);
        let observed = service
            .observe(Request::new(wire::ObserveRequest {
                workspace: created.workspace.clone(),
                operation_id: operation_bytes(u8::try_from(index + 1)?),
            }))
            .await?
            .into_inner();
        assert_eq!(observed.state, "completed");
        assert_eq!(
            observed.outcome.and_then(|outcome| outcome.generation),
            created.head
        );
    }
    Ok(())
}
