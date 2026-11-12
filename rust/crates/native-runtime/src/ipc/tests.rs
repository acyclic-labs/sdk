use super::*;
use std::ffi::OsStr;
#[cfg(unix)]
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

const ENDPOINT: &str = "ACYCLIC_PEER_CREDENTIAL_TEST_ENDPOINT";
const PARENT: &str = "ACYCLIC_PEER_CREDENTIAL_TEST_PARENT";
const TEST: &str = "ipc::tests::native_connection_credentials_reject_a_foreign_owner_on_both_ends";

trait TestIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> TestIo for T {}
type Channel = Box<dyn TestIo>;

#[cfg(windows)]
fn pipe_name() -> Result<std::path::PathBuf, getrandom::Error> {
    let mut unique = [0; 16];
    getrandom::fill(&mut unique)?;
    Ok(std::path::PathBuf::from(format!(
        r"\\.\pipe\acyclic-peer-test-{:032x}",
        u128::from_ne_bytes(unique)
    )))
}

#[cfg(windows)]
#[test]
fn contending_clients_wait_for_pipe_availability_without_probing()
-> Result<(), Box<dyn std::error::Error>> {
    use std::{future::Future as _, task::Poll};
    crate::coordination_runtime()?.block_on(async {
        let endpoint = pipe_name()?;
        let owner = PeerCredentials::current()?;
        let mut listener = PipeListener::bind(endpoint.as_os_str())?;
        let first = owner.connect_pipe(endpoint.as_os_str()).await?;
        let mut second = Box::pin(owner.connect_pipe(endpoint.as_os_str()));
        std::future::poll_fn(|context| {
            assert!(
                second.as_mut().poll(context).is_pending(),
                "busy pipe must wait on availability"
            );
            Poll::Ready(())
        })
        .await;
        let first_server = listener.accept().await?;
        let (second, second_server) = tokio::try_join!(second, listener.accept())?;
        for pipe in [&first, &second] {
            PeerCredentials::from_pipe_client(pipe)?.require_same_user(&owner)?;
        }
        for pipe in [&first_server, &second_server] {
            PeerCredentials::from_pipe_server(pipe)?.require_same_user(&owner)?;
        }
        Ok(())
    })
}

fn reject_foreign(peer: &PeerCredentials, owner: &PeerCredentials) {
    peer.require_same_user(owner).unwrap();
    let mut foreign = owner.clone();
    #[cfg(unix)]
    {
        let User::Unix(uid) = &mut foreign.user;
        *uid ^= 1;
    }
    #[cfg(windows)]
    {
        let User::Windows(sid) = &mut foreign.user;
        *sid.last_mut().expect("owner SID") ^= 1;
    }
    assert!(
        matches!(peer.require_same_user(&foreign), Err(error) if error.kind() == io::ErrorKind::PermissionDenied)
    );
}

async fn connect(endpoint: &OsStr) -> io::Result<(Channel, PeerCredentials)> {
    #[cfg(unix)]
    {
        let stream = PeerCredentials::current()?
            .connect_unix(Path::new(endpoint))
            .await?;
        let peer = PeerCredentials::from_unix(&stream)?;
        Ok((Box::new(stream), peer))
    }
    #[cfg(windows)]
    {
        let stream = PeerCredentials::current()?.connect_pipe(endpoint).await?;
        let peer = PeerCredentials::from_pipe_client(&stream)?;
        Ok((Box::new(stream), peer))
    }
}

#[cfg(unix)]
#[test]
fn listener_refuses_public_directory_custody() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    crate::coordination_runtime()?.block_on(async {
        let directory = tempfile::tempdir()?;
        let endpoint = directory.path().join("node.sock");
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755))?;
        assert!(matches!(super::endpoint(directory.path()), Err(error) if error.kind() == io::ErrorKind::PermissionDenied));
        assert!(matches!(UnixListener::bind(&endpoint), Err(error) if error.kind() == io::ErrorKind::PermissionDenied));
        assert!(!endpoint.exists());
        Ok(())
    })
}

#[cfg(unix)]
proptest::proptest! {
    #[test]
    fn stray_writes_ignored(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096)) {
        let directory = tempfile::tempdir().unwrap();
        let endpoint = directory.path().join("node.sock");
        std::fs::write(&endpoint, &bytes).unwrap();
        let owner = PeerCredentials::current().unwrap();
        let runtime = crate::coordination_runtime().unwrap();
        let (bound, connected) = runtime.block_on(async {
            (UnixListener::bind(&endpoint).is_ok(), owner.connect_unix(&endpoint).await.is_ok())
        });
        proptest::prop_assert!(!bound && !connected);
        proptest::prop_assert!(owner.retire_unix(&endpoint).is_err());
        proptest::prop_assert_eq!(std::fs::read(&endpoint).unwrap(), bytes);
    }
}

#[test]
fn native_connection_credentials_reject_a_foreign_owner_on_both_ends()
-> Result<(), Box<dyn std::error::Error>> {
    crate::coordination_runtime()?.block_on(async {
        let owner = PeerCredentials::current()?;
        if let Some(endpoint) = std::env::var_os(ENDPOINT) {
            let (mut channel, peer) = connect(&endpoint).await?;
            reject_foreign(&peer, &owner);
            if let Some(pid) = peer.process_id() {
                assert_eq!(pid, std::env::var(PARENT)?.parse::<u32>()?);
            }
            channel.write_all(&std::process::id().to_le_bytes()).await?;
            assert_eq!(channel.read_u8().await?, 1);
            return Ok(());
        }
        #[cfg(unix)]
        let directory = tempfile::tempdir()?;
        #[cfg(unix)]
        let state = directory.path().join("nested-state/".repeat(32));
        #[cfg(unix)]
        crate::private_file::create_directory_all(&state)?;
        #[cfg(unix)]
        let endpoint = super::endpoint(&state)?;
        #[cfg(unix)]
        let listener = UnixListener::bind(&endpoint)?;
        #[cfg(windows)]
        let endpoint = pipe_name()?;
        #[cfg(windows)]
        let mut listener = PipeListener::bind(endpoint.as_os_str())?;
        let accepted = async {
            #[cfg(unix)]
            { let stream = listener.accept().await?;
                let peer = PeerCredentials::from_unix(&stream)?;
                Ok::<_, io::Error>((Box::new(stream) as Channel, peer)) }
            #[cfg(windows)]
            { let stream = listener.accept().await?;
                let peer = PeerCredentials::from_pipe_server(&stream)?;
                Ok::<_, io::Error>((Box::new(stream) as Channel, peer)) }
        };
        let mut command = crate::ProcessCommand::new(std::env::current_exe()?);
        command.args(["--exact", TEST, "--nocapture"])
            .env(ENDPOINT, endpoint.as_os_str()).env(PARENT, std::process::id().to_string())
            .stdout(crate::ProcessStdio::null()).stderr(crate::ProcessStdio::inherit());
        let mut child = crate::ProcessTree::spawn(&mut command)?;
        let exited = child.readiness()?;
        let (mut channel, peer) = tokio::select! {
            connection = accepted => connection?,
            exit = exited => { exit.into_result()?; return Err("credential client exited before connecting".into()); }
        };
        reject_foreign(&peer, &owner);
        let mut pid = [0; 4];
        channel.read_exact(&mut pid).await?;
        let pid = u32::from_le_bytes(pid);
        assert_ne!(pid, std::process::id());
        if let Some(observed) = peer.process_id() { assert_eq!(observed, pid); }
        channel.write_all(&[1]).await?;
        child.readiness()?.await.into_result()?;
        assert!(child.try_wait(None)?.ok_or("client exit was not retained")?.success());
        child.terminate(None)?;
        #[cfg(unix)]
        {
            drop(listener);
            assert!(!endpoint.exists(), "the owning listener must retire its socket");
        }
        Ok(())
    })
}
