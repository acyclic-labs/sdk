use super::*;
#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
use crate::{ProcessCommand as Command, ProcessStdio as Stdio, ProcessTree};

// One native policy matrix, with the same probe and assertions on every OS.
// The ordinary-launch control proves that the host ACLs and listener work.
#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
#[test]
fn sandbox_policy() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let volume = root.path().join("volume");
    let outside = root.path().join("outside");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&volume).unwrap();
    std::fs::write(workspace.join("readable"), b"workspace data").unwrap();
    std::fs::write(volume.join("readable"), b"volume data").unwrap();
    std::fs::write(&outside, b"private").unwrap();
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let executable = std::env::current_exe().unwrap();
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    for (confined, access, primary_write, volume_write) in [
        (false, ExecutionAccess::WorkspaceWrite, true, true),
        (true, ExecutionAccess::WorkspaceWrite, true, true),
        (true, ExecutionAccess::ReadOnly, false, true),
        (true, ExecutionAccess::ReadOnly, false, false),
    ] {
        for custodied in [false, true] {
            let grant = ExecutionGrant::workspace_default(
                &workspace,
                access,
                [volume.clone()],
                [executable.clone()],
            )
            .unwrap();
            let grant = if volume_write {
                grant
            } else {
                grant.into_read_only()
            };
            let mut command = Command::new(&executable);
            command
                .args([
                    "--exact",
                    "execution::tests::sandbox_policy_probe",
                    "--nocapture",
                ])
                .env_clear()
                .env("ACYCLIC_POLICY_OUTSIDE", &outside)
                .env("ACYCLIC_POLICY_VOLUME", &volume)
                .env("ACYCLIC_POLICY_SHELL", crate::platform_shell().unwrap())
                .env(
                    "ACYCLIC_POLICY_DESTINATION",
                    listener.local_addr().unwrap().to_string(),
                )
                .env(
                    "ACYCLIC_POLICY_CONFINED",
                    if confined { "yes" } else { "no" },
                )
                .env(
                    "ACYCLIC_POLICY_PRIMARY_WRITE",
                    if primary_write { "yes" } else { "no" },
                )
                .env(
                    "ACYCLIC_POLICY_VOLUME_WRITE",
                    if volume_write { "yes" } else { "no" },
                )
                .current_dir(grant.workspace())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            #[cfg(windows)]
            command.env("SystemRoot", std::env::var_os("SystemRoot").unwrap());
            let mut tree = if confined {
                // Native capability absence is a failure, never a skipped case.
                ProcessTree::spawn_granted(command, &grant, custodied.then_some(&custody)).unwrap()
            } else {
                ProcessTree::spawn(&mut command).unwrap()
            };
            assert!(
                tree.wait(crate::CaptureSchedule::Unbounded)
                    .unwrap()
                    .success()
            );
            if confined {
                assert_eq!(
                    listener.accept().unwrap_err().kind(),
                    io::ErrorKind::WouldBlock
                );
                assert_eq!(std::fs::read(&outside).unwrap(), b"private");
            } else {
                listener.accept().unwrap();
                std::fs::write(&outside, b"private").unwrap();
            }
        }
    }
}

#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
#[test]
fn sandbox_policy_probe() {
    let Some(outside) = std::env::var_os("ACYCLIC_POLICY_OUTSIDE") else {
        return;
    };
    let confined = std::env::var("ACYCLIC_POLICY_CONFINED").unwrap() == "yes";
    let primary_write = std::env::var("ACYCLIC_POLICY_PRIMARY_WRITE").unwrap() == "yes";
    let volume_write = std::env::var("ACYCLIC_POLICY_VOLUME_WRITE").unwrap() == "yes";
    let allowed = |result: io::Result<()>, expected| {
        if expected {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        }
    };
    allowed(std::fs::read(&outside).map(|_| ()), !confined);
    allowed(std::fs::write(&outside, b"escaped"), !confined);
    assert_eq!(std::fs::read("readable").unwrap(), b"workspace data");
    assert_eq!(
        std::fs::read(
            PathBuf::from(std::env::var_os("ACYCLIC_POLICY_VOLUME").unwrap()).join("readable")
        )
        .unwrap(),
        b"volume data"
    );
    // Exercise the host-selected shell's actual runtime dependencies. On Unix
    // this also opens the null device for shell input redirection.
    let source = if cfg!(windows) {
        "[Console]::Write('runtime')"
    } else {
        "/bin/cat </dev/null && printf runtime"
    };
    let output = std::process::Command::new(std::env::var_os("ACYCLIC_POLICY_SHELL").unwrap())
        .args(acyclic_host_platform::shell::PLATFORM.arguments)
        .arg(source)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stdout, b"runtime");
    allowed(std::fs::write("inside", b"inside"), primary_write);
    allowed(
        std::fs::write(
            PathBuf::from(std::env::var_os("ACYCLIC_POLICY_VOLUME").unwrap()).join("inside"),
            b"volume",
        ),
        volume_write,
    );
    allowed(
        std::net::TcpStream::connect(std::env::var("ACYCLIC_POLICY_DESTINATION").unwrap())
            .map(|_| ()),
        !confined,
    );
}

#[cfg(windows)]
#[test]
fn windows_refuses_unrepresentable_network_grants() {
    let root = tempfile::tempdir().unwrap();
    for network in [
        ExecutionNetwork::Off,
        ExecutionNetwork::TcpPorts([443].into()),
        ExecutionNetwork::TcpPorts([].into()),
    ] {
        let grant =
            ExecutionGrant::new(root.path(), ExecutionAccess::WorkspaceWrite, [], network).unwrap();
        assert_eq!(
            grant.compile_native().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
#[allow(
    unsafe_code,
    reason = "FFI negative control creates a genuinely inheritable descriptor"
)]
fn sandbox_denies_outside_write() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace \"quoted\"");
    let volume = temp.path().join("volume");
    let outside = temp.path().join("outside");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&volume).unwrap();
    std::fs::write(&outside, "original").unwrap();
    std::os::unix::fs::symlink(&outside, workspace.join("escape")).unwrap();
    #[cfg(target_os = "macos")]
    let fifo = {
        use std::os::unix::ffi::OsStrExt;
        let path = workspace.join("fifo");
        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: a terminated fixture pathname; no reader or writer is opened.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        path.canonicalize().unwrap()
    };
    let inherited = std::fs::OpenOptions::new()
        .write(true)
        .open(&outside)
        .unwrap();
    use std::os::fd::AsRawFd;
    // SAFETY: this exclusively owned fixture descriptor remains open for the
    // test. Clearing CLOEXEC makes the negative control a real ambient leak.
    assert_ne!(
        unsafe { libc::fcntl(inherited.as_raw_fd(), libc::F_SETFD, 0) },
        -1
    );
    for (access, primary_write, volume_write) in [
        (ExecutionAccess::WorkspaceWrite, true, true),
        (ExecutionAccess::ReadOnly, false, true),
        (ExecutionAccess::ReadOnly, false, false),
    ] {
        let grant = ExecutionGrant::composed(
            &workspace,
            access,
            [volume.clone()],
            [PathBuf::from("/")],
            ExecutionNetwork::Off,
        )
        .unwrap();
        let grant = if volume_write {
            grant
        } else {
            grant.into_read_only()
        }
        .compile_native()
        .unwrap();
        #[cfg(target_os = "macos")]
        {
            // A real FIFO with no writer must refuse as cwd without waiting;
            // final regular files must still work in read-root attenuation.
            assert!(grant.prepare_launch(Some(&fifo), None).is_err());
            let narrower = grant
                .attenuate(
                    grant.workspace(),
                    ExecutionAccess::ReadOnly,
                    [outside.clone()],
                    ExecutionNetwork::Off,
                )
                .unwrap();
            grant.check_attenuation(&narrower).unwrap();
            let fifo_read = grant
                .attenuate(
                    grant.workspace(),
                    ExecutionAccess::ReadOnly,
                    [fifo.clone()],
                    ExecutionNetwork::Off,
                )
                .unwrap();
            grant.check_attenuation(&fifo_read).unwrap();
        }
        let expanded = ExecutionGrant::new(
            &workspace,
            ExecutionAccess::WorkspaceWrite,
            [],
            ExecutionNetwork::Off,
        )
        .unwrap();
        assert_eq!(grant.check_attenuation(&expanded).is_ok(), primary_write);
        let mut bypass = Command::new("/bin/sh");
        bypass
            .args(["-c", "printf leaked"])
            .env_clear()
            .current_dir(grant.workspace())
            .stdin(Stdio::null())
            .stdout(inherited.try_clone().unwrap())
            .stderr(Stdio::null());
        let refusal = ProcessTree::spawn_granted(bypass, &grant, None)
            .err()
            .expect("file-backed standard streams must refuse");
        assert_eq!(refusal.kind(), io::ErrorKind::PermissionDenied);
        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-c",
                r#"
            (printf changed > escape) 2>/dev/null && exit 10
            /bin/sh -c 'printf changed > "$1"' sh "$OUTSIDE" 2>/dev/null && exit 11
            (eval "printf leaked >&$EXTRA_FD") 2>/dev/null && exit 12
            (printf inside > inside) 2>/dev/null
            (printf volume > "$VOLUME/written") 2>/dev/null
            printf confined
        "#,
            ])
            .env_clear()
            .env("OUTSIDE", &outside)
            .env("VOLUME", &volume)
            .env("EXTRA_FD", inherited.as_raw_fd().to_string())
            .current_dir(grant.workspace())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut tree = ProcessTree::spawn_granted(command, &grant, None).unwrap();
        let mut output = String::new();
        use std::io::Read;
        tree.take_stdout()
            .unwrap()
            .read_to_string(&mut output)
            .unwrap();
        assert!(
            tree.wait(crate::CaptureSchedule::Unbounded)
                .unwrap()
                .success(),
            "{output}"
        );
        assert_eq!(output, "confined");
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "original");
        assert_eq!(workspace.join("inside").exists(), primary_write);
        assert_eq!(volume.join("written").exists(), volume_write);
        if primary_write {
            std::fs::remove_file(workspace.join("inside")).unwrap();
        }
        if volume_write {
            std::fs::remove_file(volume.join("written")).unwrap();
        }
    }
}

#[cfg(target_os = "macos")]
#[test]
fn confined_commands_preserve_custom_arg0_and_original_arguments() {
    use std::{io::Read, os::unix::ffi::OsStrExt};
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let workspace = workspace.canonicalize().unwrap();
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    let grant = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        [PathBuf::from("/")],
        ExecutionNetwork::Off,
    )
    .unwrap();
    let arg0 = std::ffi::OsStr::from_bytes(b"custom \xff ' $(never)");
    let command = |program, renamed| {
        let mut command = Command::new(program);
        command
            .args(["-c", "printf '%s|' \"$0\" \"$@\" \"$VISIBLE\""])
            .env_clear()
            .env("PATH", "/bin:/usr/bin")
            .env("VISIBLE", "literal ' $(never)")
            .current_dir(&workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if renamed {
            command.arg0(arg0);
        }
        command
    };
    let output = |mut command: Command, retained, confined| {
        let mut tree = if confined {
            ProcessTree::spawn_granted(command, &grant, retained).unwrap()
        } else if let Some(custody) = retained {
            ProcessTree::spawn_in_custody(&mut command, custody).unwrap()
        } else {
            ProcessTree::spawn(&mut command).unwrap()
        };
        let mut output = Vec::new();
        tree.take_stdout()
            .unwrap()
            .read_to_end(&mut output)
            .unwrap();
        assert!(
            tree.wait(crate::CaptureSchedule::Unbounded)
                .unwrap()
                .success()
        );
        output
    };
    let mut expected = arg0.as_bytes().to_vec();
    expected.extend_from_slice(b"|literal ' $(never)|");
    for program in ["/bin/sh", "sh"] {
        for retained in [None, Some(&custody)] {
            // Both original native owners preserve the requested argv[0].
            assert_eq!(output(command(program, true), retained, false), expected);
            // Negative control: removing the override changes observable argv[0].
            assert_ne!(output(command(program, false), retained, true), expected);
            assert_eq!(output(command(program, true), retained, true), expected);
            // Positional arguments after -c replace the shell's $0, so test
            // their exact bytes independently from the invocation name.
            let mut arguments = command(program, true);
            arguments.args(["positional-zero", "a b", "", "$(never)"]);
            assert_eq!(
                output(arguments, retained, true),
                b"positional-zero|a b||$(never)|literal ' $(never)|",
            );
        }
    }
    // Removing the wrapper must also retain execvp's headerless-script path.
    let script = workspace.join("plain");
    std::fs::write(&script, b"printf '%s|' \"$0\" \"$@\" \"$VISIBLE\"\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let original = output(command(script.to_str().unwrap(), true), None, false);
    assert!(original.ends_with(b"literal ' $(never)|"));
    assert_eq!(
        output(command(script.to_str().unwrap(), true), None, true),
        original
    );
}

#[cfg(target_os = "linux")]
mod ipc;

#[cfg(target_os = "linux")]
#[test]
fn composed_grants_attenuate_all_write_roots_and_fence_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let volume = temp.path().join("volume");
    let foreign = temp.path().join("foreign");
    for path in [&workspace, &volume, &foreign] {
        std::fs::create_dir(path).unwrap();
    }
    let grant = ExecutionGrant::composed(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        [volume.clone()],
        system_read_roots(),
        ExecutionNetwork::Off,
    )
    .unwrap();
    let child = grant
        .attenuate(
            &volume,
            ExecutionAccess::WorkspaceWrite,
            [],
            ExecutionNetwork::Off,
        )
        .unwrap();
    grant.check_attenuation(&child).unwrap();
    assert!(
        grant
            .clone()
            .into_read_only()
            .check_attenuation(&child)
            .is_err()
    );
    let escape = ExecutionGrant::composed(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        [foreign],
        [],
        ExecutionNetwork::Off,
    )
    .unwrap();
    assert!(grant.check_attenuation(&escape).is_err());
    // A retained ruleset pins the original inode. Replacing its path must
    // refuse launch, rather than refresh the policy around the new directory.
    let grant = grant.compile_native().unwrap();
    std::fs::rename(&volume, temp.path().join("retired-volume")).unwrap();
    std::fs::create_dir(&volume).unwrap();
    assert!(
        grant
            .prepare()
            .unwrap_err()
            .to_string()
            .contains("root was replaced")
    );
}

#[cfg(target_os = "linux")]
pub(crate) fn system_read_roots() -> Vec<PathBuf> {
    ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect()
}

#[test]
fn child_grants_cannot_reacquire_revoked_capabilities() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let nested = workspace.join("nested");
    let external = root.path().join("external");
    let external_nested = external.join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(&external_nested).unwrap();
    // Reject invalid authority before resolving a workspace or consuming read
    // roots. A missing workspace detects filesystem work before the refusal;
    // the real workspace detects consumption of the supplied read roots.
    let missing = root.path().join("missing");
    invalid_network_binds_no_roots(&workspace, &missing, &external);
    let ports = BTreeSet::from([443, 8443]);
    let parent = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        [external.clone(), external_nested.clone(), nested.clone()],
        ExecutionNetwork::TcpPorts(ports),
    )
    .unwrap();
    // Retaining explicit roots must not first require subtree support from an
    // overlapping read root or workspace. Keep this before the subtree case,
    // which may correctly refuse on platforms without rooted resolution.
    let readonly = parent
        .attenuate(
            &workspace,
            ExecutionAccess::ReadOnly,
            [external_nested, nested.clone()],
            ExecutionNetwork::Off,
        )
        .unwrap();
    parent.check_attenuation(&readonly).unwrap();
    assert!(readonly.check_attenuation(&parent).is_err());
    rejected_increases_bind_no_roots(&readonly, &workspace, &missing, &nested);
    let child = match parent.attenuate(
        &nested,
        ExecutionAccess::ReadOnly,
        [external],
        ExecutionNetwork::TcpPorts(BTreeSet::from([443])),
    ) {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
            assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
            return;
        }
        Err(error) => panic!("grant attenuation failed: {error}"),
    };
    for (workspace, access, read, network) in [
        (
            &nested,
            ExecutionAccess::WorkspaceWrite,
            vec![],
            ExecutionNetwork::Off,
        ),
        (
            &workspace,
            ExecutionAccess::ReadOnly,
            vec![],
            ExecutionNetwork::Off,
        ),
        (
            &nested,
            ExecutionAccess::ReadOnly,
            vec![],
            ExecutionNetwork::On,
        ),
        (
            &nested,
            ExecutionAccess::ReadOnly,
            vec![],
            ExecutionNetwork::TcpPorts(BTreeSet::from([80])),
        ),
        (
            &nested,
            ExecutionAccess::ReadOnly,
            vec![root.path().to_path_buf()],
            ExecutionNetwork::Off,
        ),
    ] {
        let error = child
            .attenuate(workspace, access, read, network)
            .unwrap_err();
        assert!(error.to_string().starts_with("tool execution failed: "));
    }
    child
        .attenuate(
            &nested,
            ExecutionAccess::ReadOnly,
            [],
            ExecutionNetwork::Off,
        )
        .unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn kernel_boundary_confines_descendants_and_preserves_host_authority() {
    let root = tempfile::tempdir().unwrap();
    retained_policy_preparation(root.path());
    let workspace = root.path().join("workspace");
    let external = root.path().join("external");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&external).unwrap();
    let sentinel = external.join("sentinel");
    std::fs::write(&sentinel, "original").unwrap();
    let readable = b"workspace bytes\n";
    std::fs::write(workspace.join("readable"), readable).unwrap();
    std::os::unix::fs::symlink(&sentinel, workspace.join("escape")).unwrap();
    let read = system_read_roots();
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    let mut admitted = 0;
    let mut unsupported = 0;
    for (access, custody, compiled) in [ExecutionAccess::WorkspaceWrite, ExecutionAccess::ReadOnly]
        .into_iter()
        .flat_map(|access| [None, Some(&custody)].map(|custody| (access, custody)))
        .flat_map(|(access, custody)| [false, true].map(|compiled| (access, custody, compiled)))
    {
        let grant = ExecutionGrant::new(
            &workspace,
            ExecutionAccess::WorkspaceWrite,
            read.clone(),
            ExecutionNetwork::Off,
        )
        .unwrap();
        let grant = match access {
            ExecutionAccess::ReadOnly => {
                let readonly = grant.clone().into_read_only();
                let error = readonly.check_attenuation(&grant).unwrap_err();
                assert!(error.to_string().contains("exceeds its authority ceiling"));
                readonly
            }
            ExecutionAccess::WorkspaceWrite => grant,
        };
        let grant = if compiled {
            match grant.compile_native() {
                Ok(grant) => grant,
                Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                    assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                    unsupported += 1;
                    continue;
                }
                Err(error) => panic!("native policy compilation: {error}"),
            }
        } else {
            grant
        };
        let make_command = || {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", "read value < readable; printf '%s\\n' \"$value\"; (printf changed > escape) 2>/dev/null; (printf created > new) 2>/dev/null; cat escape 2>/dev/null; true"])
                .current_dir(&workspace).stdin(Stdio::null())
                .stdout(Stdio::piped()).stderr(Stdio::piped());
            command
        };
        refused_launches_bind_no_roots(make_command, &grant, custody, &workspace, &sentinel);
        let mut command = make_command();
        command.env_clear();
        let original = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        let mut retained = None;
        match ProcessTree::spawn_granted_controlled(
            command,
            &grant,
            custody,
            Some(&original),
            &mut retained,
        ) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                // An unavailable boundary must return a concrete refusal and
                // must not run even the workspace-authorized part of the guest.
                assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                assert!(retained.is_none());
                assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "original");
                assert!(!workspace.join("new").exists());
                unsupported += 1;
                continue;
            }
            Err(error) => panic!("confined launch failed: {error}"),
        }
        admitted += 1;
        admitted_tree_preserves_custody(
            retained, &grant, custody, &original, &workspace, &sentinel, access,
        );
    }
    eprintln!("execution boundary cases: admitted={admitted}, unsupported={unsupported}");
    // Enforcement must affect only the child, never the launching host thread.
    std::fs::write(&sentinel, "host").unwrap();
}

#[cfg(target_os = "linux")]
fn retained_policy_preparation(root: &Path) {
    use std::sync::atomic::Ordering::Acquire;
    let workspace = root.join("retained");
    let nested = workspace.join("nested");
    let read = root.join("retained-read");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(&read, "original read root").unwrap();
    let original = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        [read.clone()],
        ExecutionNetwork::TcpPorts(BTreeSet::from([443, 8443])),
    )
    .unwrap();
    let compiled = match original.clone().compile_native() {
        Ok(grant) => grant,
        Err(error) if error.kind() == io::ErrorKind::Unsupported => return,
        Err(error) => panic!("retained policy compilation: {error}"),
    };
    for cwd in [None, Some(nested.as_path())] {
        // The original cold compiler is a real-kernel cost negative control.
        let cold = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        drop(original.prepare_launch(cwd, Some(&cold)).unwrap());
        assert!(cold.write_attempts.load(Acquire) > 0);
        let operations = 4 + u64::from(cwd.is_some());
        let limits = (0..=operations).map(|limit| (limit, None));
        let late = (1..=operations).map(|failure| (u64::MAX, Some(failure)));
        for (limit, failure) in limits.chain(late) {
            let mut warm = crate::tests::ReadBudgetMeter::new(limit, u64::MAX);
            warm.fail_recorded_operation = failure;
            let result = compiled.prepare_launch(cwd, Some(&warm));
            if limit == operations && failure.is_none() {
                drop(result.unwrap());
            } else {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
            }
            assert_eq!(warm.used_operations.load(Acquire), failure.unwrap_or(limit));
            assert_eq!(warm.write_attempts.load(Acquire), 0);
            assert_eq!(warm.memory.load(Acquire), 0);
        }
        eprintln!(
            "retained policy preparation: operations {} -> {operations}, writes {} -> 0",
            cold.used_operations.load(Acquire),
            cold.write_attempts.load(Acquire),
        );
    }
    // A view retaining a writable cache must use the ordinary narrower
    // compiler, never that cache. Recompiling the view enables warm reuse.
    let readonly = compiled.clone().into_read_only();
    let narrow = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    drop(readonly.prepare_launch(None, Some(&narrow)).unwrap());
    assert!(narrow.write_attempts.load(Acquire) > 0);
    let readonly = readonly.compile_native().unwrap();
    let warm = crate::tests::ReadBudgetMeter::new(4, 0);
    drop(readonly.prepare_launch(None, Some(&warm)).unwrap());
    assert_eq!(warm.write_attempts.load(Acquire), 0);
    // Both cached root classes retain replacement fences. Native policies
    // keep the old inode alive, but cannot authorize its replacement at a path.
    for path in [&read, &workspace] {
        std::fs::rename(path, path.with_extension("previous")).unwrap();
        if path == &read {
            std::fs::write(path, "replacement").unwrap();
        } else {
            std::fs::create_dir(path).unwrap();
        }
        let error = compiled.prepare().unwrap_err();
        assert!(error.to_string().contains("root was replaced"));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn native_preparation_preserves_original_operation_admission_and_late_failures() {
    use std::sync::atomic::Ordering;
    let root = tempfile::tempdir().unwrap();
    let read = root.path().join("source");
    let nested = root.path().join("nested");
    std::fs::write(&read, "read-only root").unwrap();
    std::fs::create_dir(&nested).unwrap();
    let nested = std::fs::canonicalize(nested).unwrap();
    let grant = ExecutionGrant::new(
        root.path(),
        ExecutionAccess::ReadOnly,
        [read],
        ExecutionNetwork::TcpPorts(BTreeSet::from([443])),
    )
    .unwrap();
    // Workspace and regular-file open/identity/rules, the ABI query and ruleset
    // creation, two device open/metadata/rules, and one TCP port rule.
    let roots = 2;
    for cwd in [None, Some(nested.as_path())] {
        // A nested cwd adds exactly its single admitted openat2 operation.
        let directory_open = u64::from(cwd.is_some());
        let operations = 3 * roots + 2 + 2 * 3 + 1 + directory_open;
        let admission = (0..=operations).map(|limit| (limit, None));
        let recording = (1..=operations).map(|failure| (u64::MAX, Some(failure)));
        for (limit, failure) in admission.chain(recording) {
            let mut meter = crate::tests::ReadBudgetMeter::new(limit, u64::MAX);
            meter.fail_recorded_operation = failure;
            let prepared = match grant.prepare_launch(cwd, Some(&meter)) {
                Ok(prepared) => prepared,
                Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                    // No supported-kernel claim is made on this path. Admission
                    // failures before the facility query still run on any kernel.
                    assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                    return;
                }
                Err(error) => {
                    assert!(failure.is_some() || limit < operations, "{error}");
                    assert_eq!(error.kind(), io::ErrorKind::Other, "{error}");
                    assert_eq!(
                        meter.used_operations.load(Ordering::Acquire),
                        failure.unwrap_or(limit),
                    );
                    continue;
                }
            };
            assert_eq!((limit, failure), (operations, None));
            assert_eq!(
                meter.read_attempts.load(Ordering::Acquire),
                2 * roots + 5 + directory_open
            );
            assert_eq!(meter.write_attempts.load(Ordering::Acquire), roots + 4);
            drop(prepared);
            // The identical native preparation without an invocation ceiling is
            // the real-kernel negative control for the rejected funded attempts.
            grant.prepare_launch(cwd, None).unwrap();
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn compiled_prefix_grant_proofs_use_original_operation_and_allocation_credits() {
    use std::sync::atomic::Ordering;
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let nested = workspace.join("nested");
    let read = root.path().join("source");
    let external = root.path().join("external");
    let external_nested = external.join("nested");
    let descendant = external_nested.join("leaf");
    let neighbor = root.path().join("external-neighbor");
    let unrelated = root.path().join("unrelated");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(&descendant).unwrap();
    std::fs::create_dir(&neighbor).unwrap();
    std::fs::write(&read, "read-only root").unwrap();
    std::fs::write(&unrelated, "unrelated root").unwrap();
    let ceiling = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        [
            read.clone(),
            external,
            external_nested.clone(),
            unrelated.clone(),
        ],
        ExecutionNetwork::On,
    )
    .unwrap();
    // An unrelated stale root confers no authority on this proof and must
    // never be rebound. The original broad ancestor still authorizes new
    // descendants after its separately granted nested root is replaced.
    std::fs::remove_file(&unrelated).unwrap();
    std::fs::rename(&external_nested, root.path().join("previous")).unwrap();
    std::fs::create_dir_all(&descendant).unwrap();
    for (path, selection, operations, allocations) in [
        (&workspace, &read, 4, 0),
        (&nested, &read, 6, 1),
        (&workspace, &descendant, 6, 1),
        (&nested, &descendant, 8, 2),
    ] {
        let requested = ExecutionGrant::new(
            path,
            ExecutionAccess::ReadOnly,
            [selection.clone()],
            ExecutionNetwork::Off,
        )
        .unwrap();
        // The identical real-object proof without a credit ceiling is the
        // admitted control; no process or Landlock facility is required.
        match ceiling.check_attenuation(&requested) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                return;
            }
            Err(error) => panic!("bound-object control failed: {error}"),
        }
        prefix_operation_credits(
            &ceiling,
            &requested,
            path,
            &workspace,
            operations,
            allocations,
        );
        prefix_refusal_credits(&ceiling, &requested, path, &nested, allocations);
    }
    // An exact stale root must refuse even though its broader ancestor still
    // authorizes newly created descendants. Indexing cannot skip that fence.
    let replaced = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::ReadOnly,
        [external_nested],
        ExecutionNetwork::Off,
    )
    .unwrap();
    let error = ceiling.check_attenuation(&replaced).unwrap_err();
    assert!(error.to_string().contains("root was replaced"));

    // A shared textual prefix is not a component ancestor. Neither that
    // sibling nor the parent of every selected root acquires read authority.
    for selection in [neighbor.as_path(), root.path()] {
        let requested = ExecutionGrant::new(
            &workspace,
            ExecutionAccess::ReadOnly,
            [selection.to_path_buf()],
            ExecutionNetwork::Off,
        )
        .unwrap();
        let meter = crate::tests::ReadBudgetMeter::new(2, 0);
        let error = ceiling
            .check_attenuation_controlled(&requested, Some(&meter))
            .unwrap_err();
        assert!(error.to_string().contains("exceeds its authority ceiling"));
        assert_eq!(meter.read_attempts.load(Ordering::Acquire), 2);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn physical_grant_admission_yields_and_release_loss_refuses() -> io::Result<()> {
    use std::sync::{Mutex, atomic::Ordering, mpsc};
    crate::initialize_native_io_services()?;
    let _inline = crate::InlineBlocking::enter();
    let root = tempfile::tempdir()?;
    let grant = ExecutionGrant::new(
        root.path(),
        ExecutionAccess::ReadOnly,
        [],
        ExecutionNetwork::Off,
    )?;
    for release_producer in [true, false] {
        let coordinator = std::thread::current().id();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = mpsc::channel();
        let step = Mutex::new(Some((entered, released)));
        let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        meter.operation_recorded = Some(Box::new(move || {
            if let Some((entered, released)) = step.lock().unwrap().take() {
                // The inline negative control fails before waiting, rather
                // than hanging the executor that produces the release.
                assert_ne!(std::thread::current().id(), coordinator);
                entered.send(()).map_err(|_| io::ErrorKind::BrokenPipe)?;
                released.recv().map_err(|_| io::ErrorKind::BrokenPipe)?;
            }
            Ok(())
        }));
        let meter = Arc::new(meter);
        let proof_meter = meter.clone();
        let ceiling = grant.clone();
        let requested = grant.clone();
        let mut proof = tokio::spawn(async move {
            ceiling
                .check_attenuation_async(&requested, Some(proof_meter))
                .await
        });
        // The very executor waiting for native I/O must produce this event.
        tokio::select! {
            entered = started => entered.map_err(|_| io::ErrorKind::BrokenPipe)?,
            result = &mut proof => {
                result.map_err(io::Error::other)??;
                return Err(io::Error::other("proof completed before entering its root operation"));
            }
        }
        if release_producer {
            release.send(()).map_err(|_| io::ErrorKind::BrokenPipe)?;
        }
        drop(release);
        let result = proof.await.map_err(io::Error::other)?;
        if release_producer {
            result?;
        } else {
            let error = result.unwrap_err();
            assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        }
        assert_eq!(
            meter.read_attempts.load(Ordering::Acquire),
            if release_producer { 2 } else { 1 }
        );
        println!(
            "deferred proof: native reads={}, task allocation reservations={}",
            meter.read_attempts.load(Ordering::Acquire),
            meter.used_allocations.load(Ordering::Acquire)
        );
    }
    // Refused task storage and cancelled requests never reach a native root
    // operation. The successful identical real-object proof above is control.
    for cancelled in [false, true] {
        let meter = Arc::new(crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX));
        meter.cancelled.store(cancelled, Ordering::Release);
        meter.deny_allocations.store(!cancelled, Ordering::Release);
        let error = grant
            .check_attenuation_async(&grant, Some(meter.clone()))
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            if cancelled {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::PermissionDenied
            }
        );
        assert_eq!(
            error
                .get_ref()
                .is_some_and(|cause| cause.is::<ExecutionRefusal>()),
            !cancelled
        );
        assert_eq!(meter.read_attempts.load(Ordering::Acquire), 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn replaced_grant_root_refuses_before_guest_execution() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let external = root.path().join("external");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&external).unwrap();
    for replaced in [&workspace, &external] {
        let grant = ExecutionGrant::new(
            &workspace,
            ExecutionAccess::WorkspaceWrite,
            [external.clone()],
            ExecutionNetwork::Off,
        )
        .unwrap();
        // The grant itself must pin the deleted object. A numeric identity
        // alone could otherwise be reused by the replacement directory.
        std::fs::remove_dir(replaced).unwrap();
        std::fs::create_dir(replaced).unwrap();
        // Exact-root selection still verifies the original object; matching
        // a declared name cannot refresh authority after replacement.
        let error = grant
            .attenuate(
                &workspace,
                ExecutionAccess::ReadOnly,
                [external.clone()],
                ExecutionNetwork::Off,
            )
            .unwrap_err();
        assert!(error.to_string().contains("root was replaced"), "{error}");
        let replacement = ExecutionGrant::new(
            &workspace,
            ExecutionAccess::WorkspaceWrite,
            [external.clone()],
            ExecutionNetwork::Off,
        )
        .unwrap();
        let error = grant.check_attenuation(&replacement).unwrap_err();
        assert!(error.to_string().contains("root was replaced"), "{error}");
        // A current ceiling cannot accept a stale requested object at the
        // same name, even though binding the ceiling itself succeeds.
        replacement.check_attenuation(&replacement).unwrap();
        for (ceiling, requested) in [(&replacement, &grant), (&grant, &grant)] {
            let error = ceiling.check_attenuation(requested).unwrap_err();
            assert!(error.to_string().contains("root was replaced"), "{error}");
        }
        // A lexical prefix alone used to refresh the replaced root's identity
        // and mint another child grant, including for a read-only root.
        let nested = replaced.join("nested");
        std::fs::create_dir(&nested).unwrap();
        let (child, read) = if replaced == &workspace {
            (&nested, vec![])
        } else {
            (&workspace, vec![nested.clone()])
        };
        let error = grant
            .attenuate(
                child,
                ExecutionAccess::ReadOnly,
                read,
                ExecutionNetwork::Off,
            )
            .unwrap_err();
        assert!(error.to_string().contains("root was replaced"), "{error}");
        let Err(error) = grant.prepare() else {
            panic!("replaced root acquired the previous grant");
        };
        if error.kind() == io::ErrorKind::Unsupported && replaced == &external {
            // Without Landlock, preparation stops before adding read roots.
            assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        } else {
            assert!(error.to_string().contains("root was replaced"), "{error}");
        }
        std::fs::remove_dir(&nested).unwrap();
    }
}

#[cfg(target_os = "linux")]
#[test]
fn native_attenuation_resolution_cannot_follow_a_replaced_ancestor() {
    let original = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let nested = original.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    std::fs::write(external.path().join("secret"), "outside").unwrap();
    let parent = BoundPath::new(original.path())
        .unwrap()
        .bind_controlled(None)
        .unwrap();
    std::fs::rename(&nested, original.path().join("previous")).unwrap();
    std::os::unix::fs::symlink(external.path(), &nested).unwrap();
    let error = linux::open_beneath(&parent, Path::new("nested/secret"), None).unwrap_err();
    assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
    // Negative control: ordinary path resolution follows the same link.
    assert_eq!(
        std::fs::read_to_string(nested.join("secret")).unwrap(),
        "outside"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn kernel_network_grants_attenuate_without_a_network_service() {
    use crate::ProcessTree;
    use crate::{ProcessCommand as Command, ProcessStdio as Stdio};
    let root = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let destination = listener.local_addr().unwrap();
    let destination_value = destination.to_string();
    let other = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let other_port = other.local_addr().unwrap().port();
    let executable = std::env::current_exe().unwrap();
    let mut read = system_read_roots();
    read.push(executable.clone());
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    let mut admitted = 0;
    let mut unsupported = 0;
    for (network, allowed, custody) in [
        (ExecutionNetwork::Off, false),
        (ExecutionNetwork::TcpPorts(BTreeSet::new()), false),
        (ExecutionNetwork::On, true),
        (
            ExecutionNetwork::TcpPorts(BTreeSet::from([destination.port()])),
            true,
        ),
        (
            ExecutionNetwork::TcpPorts(BTreeSet::from([other_port])),
            false,
        ),
    ]
    .into_iter()
    .flat_map(|(network, allowed)| {
        [None, Some(&custody)].map(move |custody| (network.clone(), allowed, custody))
    }) {
        let grant = ExecutionGrant::new(
            root.path(),
            ExecutionAccess::ReadOnly,
            read.clone(),
            network,
        )
        .unwrap();
        let mut command = Command::new(&executable);
        command
            .args(["--exact", "execution::tests::network_probe", "--nocapture"])
            .current_dir(root.path())
            .env_clear()
            .env("ACYCLIC_BOUNDARY_PROBE_DESTINATION", &destination_value)
            .env(
                "ACYCLIC_BOUNDARY_PROBE_ALLOWED",
                if allowed { "yes" } else { "no" },
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut tree = match ProcessTree::spawn_granted(command, &grant, custody) {
            Ok(tree) => tree,
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                assert_eq!(
                    listener.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
                unsupported += 1;
                continue;
            }
            Err(error) => panic!("confined launch failed: {error}"),
        };
        admitted += 1;
        let output = tree
            .wait_with_output(
                std::time::Duration::from_secs(5),
                65536,
                None,
                || Ok(()),
                |_| Ok(Default::default()),
                None,
                None,
                None,
            )
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if allowed {
            listener.accept().unwrap();
        } else {
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }
    eprintln!("network boundary cases: admitted={admitted}, unsupported={unsupported}");
}

#[cfg(target_os = "linux")]
#[test]
fn inherited_file_stdio_cannot_bypass_the_workspace_boundary() {
    use crate::{ProcessCommand, ProcessStdio, ProcessTree};
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let sentinel = root.path().join("outside");
    std::fs::write(&sentinel, "original").unwrap();
    let read = system_read_roots();
    let grant = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        read,
        ExecutionNetwork::Off,
    )
    .unwrap();
    let command = || {
        let mut command = ProcessCommand::new("/bin/sh");
        command
            .args(["-c", "printf escaped"])
            .current_dir(&workspace)
            .env_clear()
            .stdin(ProcessStdio::null())
            .stderr(ProcessStdio::null())
            .stdout(
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&sentinel)
                    .unwrap(),
            );
        command
    };
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    for custody in [None, Some(&custody)] {
        let original = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        let mut retained = None;
        let error = ProcessTree::spawn_granted_controlled(
            command(),
            &grant,
            custody,
            Some(&original),
            &mut retained,
        )
        .expect_err("an outside writable stdio descriptor entered the guest");
        // Unsupported preparation is definitive. A native launch rejection
        // can retain a warden or custody record and must stay a process error.
        assert_eq!(
            error
                .get_ref()
                .is_some_and(|cause| cause.is::<ExecutionRefusal>()),
            error.kind() == io::ErrorKind::Unsupported
        );
        assert!(matches!(
            error.kind(),
            io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
        ));
        if let Some(tree) = retained.as_mut() {
            // A failed guest admission can retain the original warden/record.
            // Cleanup is a separate admitted operation, never a hidden retry
            // against the failed invocation or an unmetered convenience path.
            let recovery = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
            tree.terminate(Some(&recovery)).unwrap();
            assert!(tree.is_reaped());
        }
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "original");
    }
    // Real negative control: the identical trusted command writes through
    // this descriptor without the boundary, even though it names no path.
    let mut tree = ProcessTree::spawn(&mut command()).unwrap();
    let output = tree
        .wait_with_output(
            std::time::Duration::from_secs(5),
            65536,
            None,
            || Ok(()),
            |_| Ok(Default::default()),
            None,
            None,
            None,
        )
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        std::fs::read_to_string(&sentinel).unwrap(),
        "originalescaped"
    );
}

// The same compiled test executable is the real native child, so the probe
// depends on no installed interpreter, external network or test-only launcher.
#[cfg(target_os = "linux")]
#[test]
fn network_probe() {
    let Some(destination) = std::env::var_os("ACYCLIC_BOUNDARY_PROBE_DESTINATION") else {
        return;
    };
    let connected = std::net::TcpStream::connect(destination.to_str().unwrap());
    if std::env::var("ACYCLIC_BOUNDARY_PROBE_ALLOWED").unwrap() == "yes" {
        connected.expect("admitted connection reaches the original listener");
    } else {
        assert_eq!(
            connected
                .expect_err("denied connection reached the original listener")
                .kind(),
            io::ErrorKind::PermissionDenied,
            "an unrelated transport failure does not prove network denial"
        );
    }
}

fn rejected_increases_bind_no_roots(
    readonly: &ExecutionGrant,
    workspace: &Path,
    missing: &Path,
    nested: &Path,
) {
    // A rejected authority increase must not bind even one requested root.
    // A missing workspace detects path resolution before the same refusal;
    // a real workspace detects consumption of the supplied read roots.
    for (access, network) in [
        (ExecutionAccess::WorkspaceWrite, ExecutionNetwork::Off),
        (ExecutionAccess::ReadOnly, ExecutionNetwork::On),
        (
            ExecutionAccess::ReadOnly,
            ExecutionNetwork::TcpPorts(BTreeSet::from([443])),
        ),
    ] {
        for workspace in [workspace, missing] {
            let visited = std::cell::Cell::new(0);
            let read =
                std::iter::once(nested.to_path_buf()).inspect(|_| visited.set(visited.get() + 1));
            let error = readonly
                .attenuate(workspace, access, read, network.clone())
                .unwrap_err();
            assert_eq!(visited.get(), 0);
            assert_eq!(
                error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<ExecutionRefusal>()
                    .unwrap()
                    .reason(),
                "execution grant exceeds its authority ceiling"
            );
        }
    }
    let visited = std::cell::Cell::new(0);
    readonly
        .attenuate(
            workspace,
            ExecutionAccess::ReadOnly,
            std::iter::once(nested.to_path_buf()).inspect(|_| visited.set(visited.get() + 1)),
            ExecutionNetwork::Off,
        )
        .unwrap();
    assert_eq!(visited.get(), 1);
}

#[cfg(target_os = "linux")]
fn refused_launches_bind_no_roots(
    make_command: impl Fn() -> crate::ProcessCommand,
    grant: &ExecutionGrant,
    custody: Option<&crate::ProcessCustody>,
    workspace: &Path,
    sentinel: &Path,
) {
    // A valid filesystem grant cannot expose ambient host secrets. Refuse
    // before native preparation; the same real command below is admitted
    // only after its environment has been made explicit.
    let Err(error) = ProcessTree::spawn_granted(make_command(), grant, custody) else {
        panic!("confined command inherited the host environment");
    };
    assert!(
        error
            .to_string()
            .contains("requires an explicit environment")
    );
    assert_eq!(std::fs::read_to_string(sentinel).unwrap(), "original");
    assert!(!workspace.join("new").exists());
    let cancelled = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    cancelled
        .cancelled
        .store(true, std::sync::atomic::Ordering::Release);
    let mut retained = None;
    let mut cancelled_command = make_command();
    cancelled_command.env_clear();
    let error = ProcessTree::spawn_granted_controlled(
        cancelled_command,
        grant,
        custody,
        Some(&cancelled),
        &mut retained,
    )
    .expect_err("a cancelled invocation prepared a native boundary");
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert!(retained.is_none());
    assert!(!workspace.join("new").exists());
    // The actual launch must use this same ledger for preparation. Its
    // first root open is refused before a process can acquire custody.
    for (limit, failure) in [(0, None), (1, Some(1))] {
        let mut exhausted = crate::tests::ReadBudgetMeter::new(limit, u64::MAX);
        exhausted.fail_recorded_operation = failure;
        let mut exhausted_command = make_command();
        exhausted_command.env_clear();
        let error = ProcessTree::spawn_granted_controlled(
            exhausted_command,
            grant,
            custody,
            Some(&exhausted),
            &mut retained,
        )
        .expect_err("an unfunded invocation prepared an execution boundary");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        assert_eq!(
            error.to_string().matches("tool execution failed: ").count(),
            1
        );
        assert!(retained.is_none());
        assert!(!workspace.join("new").exists());
    }
}

#[cfg(target_os = "linux")]
fn admitted_tree_preserves_custody(
    mut retained: Option<crate::ProcessTree>,
    grant: &ExecutionGrant,
    custody: Option<&crate::ProcessCustody>,
    original: &crate::tests::ReadBudgetMeter,
    workspace: &Path,
    sentinel: &Path,
    access: ExecutionAccess,
) {
    // A real admitted tree still needs cleanup. Refusing that operation
    // must not turn it into a pre-launch capability refusal or retire it.
    let denied_cleanup = crate::tests::ReadBudgetMeter::new(0, 0);
    let tree = retained.as_mut().expect("admitted tree");
    let cleanup = tree.terminate(Some(&denied_cleanup)).unwrap_err();
    assert!(
        !cleanup
            .get_ref()
            .is_some_and(|cause| cause.is::<ExecutionRefusal>())
    );
    assert!(!tree.is_reaped());
    // An existing recovery slot is authority, not replaceable output.
    // The rejected command would create another observable file if the
    // consuming entry accidentally launched or overwrote that slot.
    let mut rejected = Command::new("/bin/sh");
    rejected
        .args(["-c", "printf replaced > unexpected"])
        .env_clear()
        .current_dir(workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let error = ProcessTree::spawn_granted_controlled(
        rejected,
        grant,
        custody,
        Some(original),
        &mut retained,
    )
    .expect_err("an occupied recovery slot was replaced");
    assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
    let mut tree = retained.expect("admitted tree retains its original recovery slot");
    let output = tree
        .wait_with_output(
            std::time::Duration::from_secs(5),
            65536,
            None,
            || Ok(()),
            |_| Ok(Default::default()),
            None,
            None,
            None,
        )
        .expect("confined process output");
    assert!(!workspace.join("unexpected").exists());
    assert_eq!(std::fs::read_to_string(sentinel).unwrap(), "original");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        output.stdout, b"workspace bytes\n",
        "workspace reads must succeed without disclosing outside bytes"
    );
    match access {
        ExecutionAccess::WorkspaceWrite => {
            assert_eq!(
                std::fs::read_to_string(workspace.join("new")).unwrap(),
                "created"
            );
            std::fs::remove_file(workspace.join("new")).unwrap();
        }
        ExecutionAccess::ReadOnly => assert!(!workspace.join("new").exists()),
    }
}

#[cfg(target_os = "linux")]
fn prefix_operation_credits(
    ceiling: &ExecutionGrant,
    requested: &ExecutionGrant,
    path: &Path,
    workspace: &Path,
    operations: u64,
    allocations: u64,
) {
    use std::sync::atomic::Ordering;
    if path == workspace {
        let mut denied = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        denied.item_limit = Some(0);
        let error = ceiling
            .check_attenuation_controlled(requested, Some(&denied))
            .unwrap_err();
        assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        // The workspace proof completes; denied index examinations cannot
        // reach a selected read root's open or native subtree resolution.
        assert_eq!(denied.read_attempts.load(Ordering::Acquire), 2);
        assert_eq!(denied.memory.load(Ordering::Acquire), 0);
    }
    let admission = (0..=operations).map(|limit| (limit, None));
    let recording = (1..=operations).map(|failure| (u64::MAX, Some(failure)));
    for (limit, failure) in admission.chain(recording) {
        let mut meter = crate::tests::ReadBudgetMeter::new(limit, u64::MAX);
        meter.fail_recorded_operation = failure;
        let result = ceiling.check_attenuation_controlled(requested, Some(&meter));
        if limit == operations && failure.is_none() {
            result.unwrap();
            assert_eq!(meter.used_allocations.load(Ordering::Acquire), allocations,);
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        }
        assert_eq!(
            meter.used_operations.load(Ordering::Acquire),
            failure.unwrap_or(limit),
        );
        assert_eq!(
            meter.read_attempts.load(Ordering::Acquire),
            failure.unwrap_or(limit),
        );
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
}

fn invalid_network_binds_no_roots(workspace: &Path, missing: &Path, external: &Path) {
    for workspace in [workspace, missing] {
        let visited = std::cell::Cell::new(0);
        let error = ExecutionGrant::new(
            workspace,
            ExecutionAccess::ReadOnly,
            std::iter::once(external.to_path_buf()).inspect(|_| visited.set(visited.get() + 1)),
            ExecutionNetwork::TcpPorts(BTreeSet::from([0, 443])),
        )
        .unwrap_err();
        assert_eq!(visited.get(), 0);
        assert_eq!(
            error
                .get_ref()
                .unwrap()
                .downcast_ref::<ExecutionRefusal>()
                .unwrap()
                .reason(),
            "TCP port zero is not a destination grant"
        );
    }
}

#[cfg(target_os = "linux")]
fn prefix_refusal_credits(
    ceiling: &ExecutionGrant,
    requested: &ExecutionGrant,
    path: &Path,
    nested: &Path,
    allocations: u64,
) {
    use std::sync::atomic::Ordering;
    for already_cancelled in [true, false] {
        let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        meter.cancelled.store(already_cancelled, Ordering::Release);
        meter.cancel_after_operation = !already_cancelled;
        let error = ceiling
            .check_attenuation_controlled(requested, Some(&meter))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(
            meter.read_attempts.load(Ordering::Acquire),
            u64::from(!already_cancelled),
        );
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
    let denied = crate::tests::ReadBudgetMeter::new(0, 0);
    let error = requested
        .check_attenuation_controlled(ceiling, Some(&denied))
        .unwrap_err();
    assert!(error.to_string().contains("exceeds its authority ceiling"));
    assert_eq!(denied.read_attempts.load(Ordering::Acquire), 0);
    if allocations != 0 {
        let denied = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        denied.deny_allocations.store(true, Ordering::Release);
        let error = ceiling
            .check_attenuation_controlled(requested, Some(&denied))
            .unwrap_err();
        assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        let before_allocation = if path == nested { 2 } else { 4 };
        assert_eq!(
            denied.used_operations.load(Ordering::Acquire),
            before_allocation,
        );
        assert_eq!(
            denied.read_attempts.load(Ordering::Acquire),
            before_allocation,
        );
        assert_eq!(denied.memory.load(Ordering::Acquire), 0);
    }
}
