use super::*;
use acyclic_native_runtime::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork};
use std::{path::PathBuf, sync::Arc};

#[test]
fn capture_keeps_the_host_grant_and_returns_its_original_custody()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let root = tempfile::tempdir()?;
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace)?;
    let outside = root.path().join("outside");
    let inside = workspace.join("inside");
    let read: Vec<_> = ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect();
    let process_custody = Arc::new(acyclic_native_runtime::ProcessCustody::open(
        &root.path().join("custody"),
    )?);
    let owner = Arc::new(());
    for retention in [OutputRetention::Collect, OutputRetention::Observe] {
        for custody in [None, Some(Arc::clone(&process_custody))] {
            // The identical unrestricted invocation is the effect control.
            for access in [
                None,
                Some(ExecutionAccess::ReadOnly),
                Some(ExecutionAccess::WorkspaceWrite),
            ] {
                std::fs::write(&outside, "original")?;
                match std::fs::remove_file(&inside) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                let invocation = Invocation {
                    executable: "/bin/sh".into(),
                    argv: vec![
                        "-c".into(),
                        "{ printf changed > ../outside; printf created > inside; } 2>/dev/null; printf complete".into(),
                    ],
                    cwd: workspace.clone(),
                    environment: Default::default(),
                    inherit_environment: false,
                    execution_grant: access
                        .map(|access| {
                            ExecutionGrant::new(
                                &workspace,
                                access,
                                read.clone(),
                                ExecutionNetwork::Off,
                            )
                        })
                        .transpose()?,
                    schedule: acyclic_native_runtime::CaptureSchedule::Unbounded,
                    maximum_output_bytes: Some(1024),
                    standard_input: None,
                    process_custody: custody.clone(),
                };
                let (result, (returned, observed)) = runtime.block_on(capture_with_custody(
                    invocation,
                    |(_, observed): &mut (Arc<()>, Vec<u8>), chunk| {
                        if let acyclic_native_runtime::ProcessOutput::Stdout(bytes) = chunk {
                            observed.extend_from_slice(bytes);
                        }
                        Ok(())
                    },
                    || Ok(()),
                    (Arc::clone(&owner), Vec::new()),
                    None,
                    match retention {
                        OutputRetention::Collect => OutputRetention::Collect,
                        OutputRetention::Observe => OutputRetention::Observe,
                    },
                ))?;
                assert!(Arc::ptr_eq(&owner, &returned));
                let output = match result {
                    Err(Error::Unsupported(_)) if access.is_some() => {
                        assert_eq!(std::fs::read_to_string(&outside)?, "original");
                        assert!(!inside.exists());
                        assert!(observed.is_empty());
                        continue;
                    }
                    other => other?.ok_or("native capture was interrupted")?,
                };
                assert!(output.success, "{output:?}");
                assert_eq!(observed, b"complete");
                match retention {
                    OutputRetention::Collect => assert_eq!(output.stdout, observed),
                    OutputRetention::Observe => assert!(output.stdout.is_empty()),
                }
                assert_eq!(
                    std::fs::read_to_string(&outside)?,
                    if access.is_none() {
                        "changed"
                    } else {
                        "original"
                    },
                );
                assert_eq!(inside.exists(), access != Some(ExecutionAccess::ReadOnly));
            }
        }
    }
    Ok(())
}
