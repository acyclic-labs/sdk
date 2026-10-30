use super::*;

pub(super) fn macos_nfs_xattrs_and_toolchain(mount: &Path, metadata: &Path) -> Result<(), Failure> {
    #[cfg(target_os = "macos")]
    {
        let write = tool_output(
            Command::new("/usr/bin/xattr")
                .args(["-w", "com.acyclic.qualifier", "xattr-value"])
                .arg(metadata),
        )?;
        if !write.status.success() {
            return Err(format!(
                "xattr write failed: {}",
                String::from_utf8_lossy(&write.stderr)
            )
            .into());
        }
        let read = tool_output(
            Command::new("/usr/bin/xattr")
                .args(["-px", "com.acyclic.qualifier"])
                .arg(metadata),
        )?;
        let encoded = String::from_utf8_lossy(&read.stdout)
            .replace([' ', '\n'], "")
            .to_ascii_lowercase();
        if !read.status.success() || encoded != "78617474722d76616c7565" {
            return Err(format!(
                "xattr round trip through NFS was not exact: status={} hex={encoded:?} stderr={}",
                read.status,
                String::from_utf8_lossy(&read.stderr)
            )
            .into());
        }
        let resource = tool_output(
            Command::new("/usr/bin/xattr")
                .args([
                    "-wx",
                    "com.apple.ResourceFork",
                    "7265736f757263652d666f726b",
                ])
                .arg(metadata),
        )?;
        if !resource.status.success() {
            return Err(format!(
                "resource fork write failed: {}",
                String::from_utf8_lossy(&resource.stderr)
            )
            .into());
        }
        let read = tool_output(
            Command::new("/usr/bin/xattr")
                .args(["-px", "com.apple.ResourceFork"])
                .arg(metadata),
        )?;
        let encoded = String::from_utf8_lossy(&read.stdout)
            .replace([' ', '\n'], "")
            .to_ascii_lowercase();
        if !read.status.success() || encoded != "7265736f757263652d666f726b" {
            let listed = tool_output(Command::new("/usr/bin/xattr").args(["-l"]).arg(metadata))?;
            let sidecar = metadata.with_file_name("._metadata.txt");
            let sidecar_bytes = fs::read(&sidecar).ok();
            let sidecar_payload_offset = sidecar_bytes.as_ref().and_then(|bytes| {
                bytes
                    .windows(b"resource-fork".len())
                    .position(|window| window == b"resource-fork")
            });
            return Err(format!(
                "resource fork round trip through NFS was not exact: status={} hex={encoded:?} stderr={} listed={} sidecar_bytes={:?} sidecar_header={:?} sidecar_payload_offset={sidecar_payload_offset:?}",
                read.status,
                String::from_utf8_lossy(&read.stderr),
                String::from_utf8_lossy(&listed.stdout),
                sidecar_bytes.as_ref().map(Vec::len),
                sidecar_bytes.as_ref().map(|bytes| bytes.iter().take(16).collect::<Vec<_>>())
            )
            .into());
        }
        macos_nfs_toolchain(mount)?;
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (mount, metadata);
        Err("macOS NFS qualification ran on a non-macOS target".into())
    }
    #[cfg(target_os = "macos")]
    Ok(())
}

pub(super) async fn macos_nfs_handoff_and_mount_loss<A, O>(
    workspace: &acyclic_fs::Workspace<A, O>,
    root: &Path,
) -> Result<(), Failure>
where
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    let destination = root.join("handoff-mount");
    fs::create_dir(&destination)?;
    let remount = workspace
        .mount(&destination, MountOptions::read_write())
        .await
        .map_err(stop_failed_mount)?;
    if fs::read(destination.join("nested/created.txt"))? != b"created-v2" {
        return Err("service handoff remount did not expose the published generation".into());
    }
    #[cfg(target_os = "macos")]
    {
        let detached = tool_output(Command::new("/sbin/umount").arg("-f").arg(&destination))?;
        if !detached.status.success() {
            return Err(format!(
                "forced mount-loss detach failed: {}",
                String::from_utf8_lossy(&detached.stderr)
            )
            .into());
        }
    }
    remount.unmount().await?;
    if !destination.is_dir() || fs::read_dir(&destination)?.next().is_some() {
        return Err("mount-loss cleanup did not restore an empty directory".into());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_nfs_toolchain(mount: &Path) -> Result<(), Failure> {
    let source = mount.join("nested/toolchain.c");
    let binary = mount.join("nested/toolchain");
    fs::write(&source, b"int main(void) { return 0; }\n")?;
    let compiled = tool_output(
        Command::new("/usr/bin/clang")
            .arg(&source)
            .arg("-o")
            .arg(&binary),
    )?;
    if !compiled.status.success() {
        return Err(format!(
            "clang over NFS failed: {}",
            String::from_utf8_lossy(&compiled.stderr)
        )
        .into());
    }
    if !acyclic_native_runtime::process_status(
        &mut Command::new(&binary),
        acyclic_native_runtime::CaptureSchedule::Unbounded,
    )?
    .success()
    {
        return Err("compiled NFS subprocess failed".into());
    }
    Ok(())
}
