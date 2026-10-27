//! Public Bound admission and the standalone daemon, using real VMM frontends.
#![cfg(target_os = "linux")]

use acyclic_fs::{
    CancellationToken, Fs, LocalOptions, NativeMaterializationWork, WorkspaceError,
    performance::work_budget,
};
use acyclic_harness::{
    LocalHarness, LocalHarnessConfig,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::hosting::LocalGuestEnrollment,
    local::host_owner::HostWorkspaceOwner,
    resources::ProviderRef,
};
use acyclic_native_runtime::{ProcessInput, ProcessTree};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn config(root: &Path) -> Result<(PathBuf, serde_json::Value)> {
    acyclic_native_runtime::initialize_native_io_services()?;
    acyclic_fs::initialize_native_mount_services()?;
    let directory = root.join("filesystem");
    let volume = VolumeRef::new(
        ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?,
        "virtiofs".into(),
        VolumeClass::Project,
        VolumeOwner::Project("self-host".into()),
        &mut None,
    )?;
    let budget = work_budget(1 << 20, 1 << 26);
    let fs = Fs::local(LocalOptions::new(&directory)).await?;
    fs.create_workspace_with_operation::<_, (), WorkspaceError>(
        &volume.storage_name(&mut None)?,
        acyclic_fs::VolumeInitialization {
            config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
            access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
        },
        acyclic_fs::OperationId::new(),
        budget,
        &CancellationToken::new(),
        None,
    )
    .await
    .map_err(|failure| failure.error)?;
    drop(fs);
    let value = serde_json::json!({
        "directory": directory, "state_directory": root.join("owner"),
        "enrollment": { "volume": volume, "posix": { "identities": [[0, 0], [1000, 1000]], "metadata": true }, "writable": true },
        "budget": budget,
    });
    let path = root.join("enrollment.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&value)?)?;
    Ok((path, value))
}

#[tokio::test]
async fn local_owner_bound_produces_the_public_mount_free_packet_entry() -> Result {
    let temporary = tempfile::tempdir()?;
    let (_, config) = config(temporary.path()).await?;
    let enrollment: LocalGuestEnrollment = serde_json::from_value(config["enrollment"].clone())?;
    let directory: PathBuf = serde_json::from_value(config["directory"].clone())?;
    let state: PathBuf = serde_json::from_value(config["state_directory"].clone())?;
    let local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(directory)
                .volume(enrollment.volume.clone())
                .state_directory(state)
                .hermetic(),
        )
        .await?,
    );
    let owner = HostWorkspaceOwner::new(local)?;
    let work =
        NativeMaterializationWork::new(work_budget(1 << 20, 1 << 26), &CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
    let admission = owner.guest_session(enrollment, &work).await?;
    let capture: serde_json::Value = serde_json::from_str(include_str!(
        "../../filesystem/fixtures/fuse/linux-kernel-packets.json"
    ))?;
    let packet: Vec<u8> = serde_json::from_value(capture["packets"][0]["request"].clone())?;
    let mut response = vec![0; admission.packet_capacity()];
    let mut delivered = false;
    admission.packet(&packet, &mut response, |bytes| {
        let bytes = bytes.ok_or(acyclic_fs::NativeMountError::Lifecycle(
            "INIT reply missing",
        ))?;
        assert_eq!(&bytes[4..8], &[0; 4]);
        delivered = true;
        Ok(())
    })?;
    assert!(delivered);
    assert!(admission.take_callback_failure().is_none());
    drop(admission);
    owner.retire().await?;
    Ok(())
}

#[test]
#[ignore = "requires prepared Linux guest artifacts, KVM, QEMU and cloud-hypervisor"]
fn standalone_daemon_qemu_and_cloud_hypervisor_read_write_delete() -> Result {
    let artifacts = PathBuf::from(std::env::var_os("ACYCLIC_FS_GUEST_ARTIFACTS").ok_or("step=select guest artifacts; recovery: python3 scripts/prepare-virtiofs-guest.py --output /tmp/acyclic-guest; ACYCLIC_FS_GUEST_ARTIFACTS=/tmp/acyclic-guest cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,test-support --test virtiofs -- --include-ignored --nocapture")?);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    for vmm in ["qemu", "cloud-hypervisor"] {
        let temporary = tempfile::tempdir()?;
        let (config, _) = runtime.block_on(config(temporary.path()))?;
        let socket = temporary.path().join("fs.sock");
        let mut daemon_command = Command::new(env!("CARGO_BIN_EXE_acyclic-virtiofsd"));
        daemon_command
            .arg("--socket")
            .arg(&socket)
            .arg("--config")
            .arg(config)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut daemon = ProcessTree::spawn(&mut daemon_command)?;
        let result = (|| {
            let start = Instant::now();
            while !socket.exists() {
                if start.elapsed() >= Duration::from_secs(5) {
                    return Err("step=wait for admitted daemon socket; recovery: cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,test-support --test virtiofs -- --include-ignored --nocapture".into());
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let mut command = frontend(vmm, &artifacts, &socket);
            let mut guest = ProcessTree::spawn(&mut command)?;
            let output = capture(&mut guest, Duration::from_secs(20))?;
            eprintln!(
                "BOUND_VIRTIOFS_GUEST vmm={vmm} status={} stdout={} stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.status.success());
            assert!(
                output
                    .stdout
                    .windows(b"ACYCLIC_VIRTIOFS_PASS".len())
                    .any(|bytes| bytes == b"ACYCLIC_VIRTIOFS_PASS")
            );
            Ok::<_, Box<dyn std::error::Error>>(())
        })();
        // Capture always positively reaps the daemon, including guest failure.
        let retired = capture(&mut daemon, Duration::from_secs(5));
        if let Ok(output) = &retired {
            eprintln!(
                "BOUND_VIRTIOFS_DAEMON vmm={vmm} status={} stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        result?;
        assert!(retired?.status.success());
        assert!(!socket.exists(), "the daemon must retire its own socket");
    }
    Ok(())
}

fn capture(process: &mut ProcessTree, duration: Duration) -> Result<std::process::Output> {
    process.wait_with_output(duration, 64 * 1024, None, || Ok(()), |_| Ok(ProcessInput::default()), None, None, None)
        .map_err(|failure| format!("step=capture owned VMM/daemon; cleanup_completed={}; error={}; stdout={}; stderr={}; recovery: cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,test-support --test virtiofs -- --include-ignored --nocapture", failure.cleanup_completed, failure.error, String::from_utf8_lossy(&failure.stdout), String::from_utf8_lossy(&failure.stderr)).into())
}

fn frontend(vmm: &str, artifacts: &Path, socket: &Path) -> Command {
    let mut command = if vmm == "qemu" {
        let mut command = Command::new("qemu-system-x86_64");
        command
            .args([
                "-enable-kvm",
                "-cpu",
                "host",
                "-nodefaults",
                "-no-reboot",
                "-display",
                "none",
                "-serial",
                "stdio",
                "-monitor",
                "none",
                "-m",
                "256M",
                "-object",
                "memory-backend-memfd,id=mem,size=256M,share=on",
                "-numa",
                "node,memdev=mem",
                "-append",
                "console=ttyS0 rdinit=/init panic=-1 quiet",
                "-device",
                "vhost-user-fs-pci,chardev=fs,tag=acyclic,queue-size=512",
            ])
            .arg("-kernel")
            .arg(artifacts.join("kernel"))
            .arg("-initrd")
            .arg(artifacts.join("initramfs.gz"))
            .arg("-chardev")
            .arg(format!("socket,id=fs,path={}", socket.display()));
        command
    } else {
        let mut command = Command::new(artifacts.join("cloud-hypervisor"));
        command
            .args([
                "--cpus",
                "boot=1",
                "--memory",
                "size=256M,shared=on",
                "--serial",
                "tty",
                "--console",
                "off",
                "--cmdline",
                "console=ttyS0 rdinit=/init panic=-1 quiet",
            ])
            .arg("--kernel")
            .arg(artifacts.join("kernel"))
            .arg("--initramfs")
            .arg(artifacts.join("initramfs.gz"))
            .arg("--fs")
            .arg(format!(
                "tag=acyclic,socket={},num_queues=1,queue_size=512",
                socket.display()
            ));
        command
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}
