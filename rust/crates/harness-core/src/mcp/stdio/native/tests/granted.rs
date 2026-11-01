use super::*;
use acyclic_native_runtime::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork};
use std::path::PathBuf;

#[tokio::test]
async fn original_stdio_connection_enforces_the_host_grant_before_initialization() -> Result<()> {
    let root = tempfile::tempdir().map_err(Error::process_io)?;
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).map_err(Error::process_io)?;
    let outside = root.path().join("outside");
    let inside = workspace.join("inside");
    let read: Vec<_> = ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect();

    // Reuse the original protocol conversation; only the real process's effects
    // differ. No provider/tool output fixture is introduced by this control.
    for access in [
        None,
        Some(ExecutionAccess::ReadOnly),
        Some(ExecutionAccess::WorkspaceWrite),
    ] {
        std::fs::write(&outside, "original").map_err(Error::process_io)?;
        match std::fs::remove_file(&inside) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::process_io(error)),
        }
        let mut command = fixture::command(&workspace)?;
        let script = workspace.join("server.sh");
        let original = std::fs::read(&script).map_err(Error::process_io)?;
        let mut program =
            b"{ printf changed > ../outside; printf created > inside; } 2>/dev/null\n".to_vec();
        program.extend_from_slice(&original);
        std::fs::write(script, program).map_err(Error::process_io)?;
        command.env_clear().stderr(ProcessStdio::null());
        let grant = access
            .map(|access| {
                ExecutionGrant::new(&workspace, access, read.clone(), ExecutionNetwork::Off)
            })
            .transpose()
            .map_err(Error::process_io)?;
        let transport = match StdioMcpTransport::spawn(command, grant, 4096, 5_000).await {
            Err(Error::Unsupported(_)) if access.is_some() => {
                assert_eq!(
                    std::fs::read_to_string(&outside).map_err(Error::process_io)?,
                    "original"
                );
                assert!(!inside.exists());
                continue;
            }
            other => other?,
        };
        let tools = transport
            .rpc(
                OperationId::from_bytes([5; 16]),
                "tools/list",
                json!({}),
                None,
            )
            .await?;
        assert_eq!(tools["tools"], json!([]));
        assert!(!transport.service.is_closed());
        assert!(transport._process.is_some());
        assert_eq!(
            std::fs::read_to_string(&outside).map_err(Error::process_io)?,
            if access.is_none() {
                "changed"
            } else {
                "original"
            }
        );
        assert_eq!(inside.exists(), access != Some(ExecutionAccess::ReadOnly));
        drop(transport);
    }
    Ok(())
}
