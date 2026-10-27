//! Real standard VMM frontends over the same packet engine as the host mount.
//!
//! This uses an authenticated test source. It exercises transport I/O, not a
//! public hosted admission or revocation of private guest mappings.

use super::super::packet_router::PacketRouter;
use super::super::packet_session::PacketSession;
use super::super::vhost_session::VhostSession;
use super::super::{FuseProjection, ProjectionCache, ProjectionConfig};
use super::*;
use acyclic_native_runtime::{ProcessInput, ProcessTree};
use std::num::{NonZeroU16, NonZeroU32};
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Command, Stdio};

#[test]
#[ignore = "requires prepared Linux guest artifacts, KVM, QEMU and cloud-hypervisor"]
fn standard_qemu_and_cloud_hypervisor_guests_read_write_and_delete()
-> Result<(), Box<dyn std::error::Error>> {
    let artifacts = std::env::var_os("ACYCLIC_FS_GUEST_ARTIFACTS").ok_or(
        "step=locate guest artifacts; recovery: python3 scripts/prepare-virtiofs-guest.py --output /tmp/acyclic-guest; ACYCLIC_FS_GUEST_ARTIFACTS=/tmp/acyclic-guest cargo test --locked -p acyclic-fs --lib -- --include-ignored --nocapture",
    )?;
    let artifacts = Path::new(&artifacts).canonicalize()?;
    let provenance = std::fs::read(artifacts.join("provenance.json"))?;
    for vmm in ["qemu", "cloud-hypervisor"] {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(FINITE_HANDLER_POLICY, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let (source, _) = shared_sources(posix_config())?;
        let source = Arc::new(source);
        let root = source
            .lookup_with_meter(&MountPath::root(), &work)?
            .ok_or("guest test volume root missing")?;
        let stamp = source.view_stamp_with_meter(&work)?;
        let core = ProjectionCore::new(
            source.clone(),
            root,
            stamp,
            ProjectionConfig {
                writable: true,
                owner: (0, 0),
                inode_base: 0,
                cache: ProjectionCache::Direct,
            },
            &work,
        )?;
        let protocol = PacketSession::new(
            PacketRouter::Single(FuseProjection { core }),
            64 * 1024,
            NonZeroU32::new(4096).ok_or("nonzero guest page size")?,
        )?;
        let temporary = tempfile::tempdir()?;
        let socket = temporary.path().join("fs.sock");
        let listener = UnixListener::bind(&socket)?;
        let server_work = work.clone();
        let server = std::thread::spawn(move || {
            let (connection, _) = listener.accept()?;
            VhostSession::new(
                protocol,
                connection,
                NonZeroU16::new(512).ok_or(libc::EINVAL)?,
                b"acyclic",
                &server_work,
            )
            .map(VhostSession::run)
        });

        let mut command = frontend(vmm, &artifacts, &socket);
        let result = (|| {
            let mut guest = ProcessTree::spawn(&mut command)?;
            guest
                .wait_with_output(
                    Duration::from_secs(20),
                    64 * 1024,
                    None,
                    || Ok(()),
                    |_| Ok(ProcessInput::default()),
                    None,
                    None,
                    None,
                )
                .map_err(|failure| {
                    std::io::Error::other(format!(
                        "step={vmm} guest capture; cleanup_completed={}; {}; stdout={}; stderr={}; recovery: {command:?}",
                        failure.cleanup_completed,
                        failure.error,
                        String::from_utf8_lossy(&failure.stdout),
                        String::from_utf8_lossy(&failure.stderr),
                    ))
                })
        })();
        // Stop after the actual VMM has exited or capture has positively
        // contained it. Connecting also releases accept if spawn failed before
        // the frontend connected; this connection carries no test traffic.
        cancellation.cancel();
        drop(UnixStream::connect(&socket));
        let retired = server
            .join()
            .map_err(|_| "standard guest socket worker panicked")?;
        let callback_failure = match &retired {
            Ok(retired) => {
                let failure = retired.take_callback_failure();
                eprintln!(
                    "VIRTIOFS_GUEST vmm={vmm} callback_work={:?} transport={:?} callback_failure={failure:?}",
                    retired.completed_callback_work(),
                    retired.transport
                );
                failure
            }
            Err(error) => {
                eprintln!("VIRTIOFS_GUEST vmm={vmm} socket_start_failure={error:?}");
                None
            }
        };
        let output = result?;
        eprintln!(
            "VIRTIOFS_GUEST vmm={vmm} status={} provenance={} stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&provenance),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(output.status.success(), "{vmm} guest process failed");
        assert!(
            output
                .stdout
                .windows(b"ACYCLIC_VIRTIOFS_PASS".len())
                .any(|bytes| bytes == b"ACYCLIC_VIRTIOFS_PASS"),
            "{vmm} guest did not complete read/write/delete"
        );
        let _retired = retired?;
        assert!(callback_failure.is_none());
        // A serial marker alone cannot prove that the source was exercised.
        // Every guest operation has physically completed before this lookup.
        assert!(source.lookup(&name("qualification"))?.is_none());
        assert!(work.receipt().items_examined > 0);
    }
    Ok(())
}

fn frontend(vmm: &str, artifacts: &Path, socket: &Path) -> Command {
    let kernel = artifacts.join("kernel");
    let initramfs = artifacts.join("initramfs.gz");
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
            .arg(kernel)
            .arg("-initrd")
            .arg(initramfs)
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
            .arg(kernel)
            .arg("--initramfs")
            .arg(initramfs)
            .arg("--fs")
            .arg(format!(
                "tag=acyclic,socket={},num_queues=1,queue_size=512",
                socket.display()
            ));
        command
    };
    command
        .env_remove("RUST_LOG")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}
