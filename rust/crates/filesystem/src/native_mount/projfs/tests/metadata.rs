use super::*;
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn metadata_edit_after_a_boundary_through_a_handle_opened_before_it_is_captured()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let (root, destination, mut session) = mount_source(&source)?;
    let file = destination.join("late.txt");
    std::fs::write(root.path().join("seed"), b"late")?;
    let copied = acyclic_native_runtime::process_status(
        std::process::Command::new("cmd.exe")
            .args(["/D", "/C", "copy /Y seed projection\\late.txt > nul"])
            .current_dir(root.path()),
        std::time::Duration::from_secs(120),
    )?;
    assert!(copied.success());
    let script = "$ErrorActionPreference = 'Stop'; Add-Type -Namespace Probe -Name Kernel -MemberDefinition \
        '[DllImport(\"kernel32.dll\")] public static extern bool SetFileTime(\
        Microsoft.Win32.SafeHandles.SafeFileHandle h, IntPtr c, IntPtr a, ref long w);'; \
        $h = [IO.File]::Open($env:ACYCLIC_FS_TEST_FILE, 'Open', 'ReadWrite', 'ReadWrite,Delete'); \
        New-Item -ItemType File $env:ACYCLIC_FS_TEST_READY | Out-Null; \
        while (-not (Test-Path $env:ACYCLIC_FS_TEST_GO)) { Start-Sleep -Milliseconds 10 }; \
        $w = [DateTimeOffset]::FromUnixTimeSeconds(978307200).UtcDateTime.ToFileTimeUtc(); \
        if (-not [Probe.Kernel]::SetFileTime($h.SafeFileHandle, [IntPtr]::Zero, [IntPtr]::Zero, [ref]$w)) { exit 2 }; \
        $h.Close()";
    let ready = root.path().join("ready");
    let go = root.path().join("go");
    let mut editor = acyclic_native_runtime::ProcessTree::spawn(
        std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", script])
            .env("ACYCLIC_FS_TEST_FILE", &file)
            .env("ACYCLIC_FS_TEST_READY", &ready)
            .env("ACYCLIC_FS_TEST_GO", &go),
    )?;
    while !ready.exists() {
        assert!(editor.try_wait(None)?.is_none(), "editor exited early");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    session.flush_callbacks(&work)?;
    std::fs::write(&go, b"")?;
    assert!(
        editor.wait(std::time::Duration::from_secs(30))?.success(),
        "metadata edit failed"
    );
    session.flush_callbacks(&work)?;
    let captured = source
        .lookup(&windows_path("late.txt"))?
        .ok_or("late.txt was not captured")?;
    assert_eq!(
        captured.metadata.modified_ns,
        crate::kernel::MetadataField::Value(978_307_200_000_000_000),
        "the edit made after the boundary was lost"
    );
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_time_set_through_a_writable_handle_stays() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    const FILES: usize = 120;
    const TICKS: i64 = crate::native_host::UNIX_EPOCH_FILETIME + 1_700_000_000 * 10_000_000;
    let source = windows_checkout_source().await?;
    for index in 0..FILES {
        let path = windows_path(&format!("f{index}.txt"));
        source.create_file(&path, FileMetadata::default(), None, &work)?;
        source.write_range(&path, 0, Bytes::from(vec![b'x'; 4096 * (index % 2)]))?;
    }
    let (_root, destination, mut session) = mount_source(&source)?;
    let directory = destination.display();
    let reader = powershell(&format!(
        r"1..10 | % {{ 0..{last} | % {{ try {{ [void][IO.File]::ReadAllBytes('{directory}\f' + $_ + '.txt') }} catch {{}} }} }}",
        last = FILES - 1
    ))?;
    let editor = powershell(&format!(
        r#"Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; using Microsoft.Win32.SafeHandles; public static class T {{ [DllImport("kernel32.dll", SetLastError=true)] public static extern bool SetFileTime(SafeFileHandle h, IntPtr c, IntPtr a, ref long w); }}'
0..{last} | % {{
  $path = '{directory}\f' + $_ + '.txt'
  if ($_ % 3 -eq 0) {{ [void][IO.File]::ReadAllBytes($path) }}
  # Refused while the reader holds the file without sharing writes, as any
  # editor would be: tried again; failing otherwise.
  $file = $null
  for ($try = 0; $file -eq $null; $try++) {{
try {{ $file = [IO.File]::Open($path, 'Open', 'ReadWrite', 'ReadWrite') }}
catch [IO.IOException] {{ if ($try -ge 500) {{ exit 2 }}; Start-Sleep -Milliseconds 10 }}
  }}
  $time = [long]{TICKS}
  if (-not [T]::SetFileTime($file.SafeFileHandle, [IntPtr]::Zero, [IntPtr]::Zero, [ref]$time)) {{ exit 1 }}
  $file.Close()
}}"#,
        last = FILES - 1
    ))?;
    succeeded(editor)?;
    succeeded(reader)?;
    session.flush_callbacks(&work)?;
    session.revalidate(&work)?;
    let wanted = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    let mut lost = Vec::new();
    for index in 0..FILES {
        let on_disk = std::fs::metadata(destination.join(format!("f{index}.txt")))?.modified()?;
        let recorded = source
            .lookup(&windows_path(&format!("f{index}.txt")))?
            .ok_or("file gone from the source")?
            .metadata
            .modified_ns;
        if on_disk != wanted
            || recorded != crate::kernel::MetadataField::Value(1_700_000_000_000_000_000)
        {
            lost.push(index);
        }
    }
    assert!(lost.is_empty(), "times lost: {lost:?}");
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn external_metadata_only_change_survives_remount() -> Result<(), Box<dyn std::error::Error>>
{
    let work = setup_work()?;
    let root = tempfile::tempdir()?;
    let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
    let workspace = engine
        .create_workspace(
            "metadata-projfs",
            WorkBudget::UNBOUNDED,
            work.cancellation(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace
        .write("/metadata.txt", Bytes::from_static(b"unchanged contents"))
        .await?;

    let first = root.path().join("first-mount");
    std::fs::create_dir(&first)?;
    let mount = workspace
        .mount(
            &first,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await?;
    let projected = first.join("metadata.txt");
    for _ in 0..3 {
        assert_eq!(std::fs::read(&projected)?, b"unchanged contents");
    }
    let status = tokio::task::spawn_blocking(move || {
        acyclic_native_runtime::process_status(
            std::process::Command::new("attrib")
                .arg("+R")
                .arg(projected),
            std::time::Duration::from_secs(120),
        )
    })
    .await??;
    assert!(status.success(), "attrib failed with {status}");
    mount.sync().await?;
    mount.unmount().await?;

    let second = root.path().join("second-mount");
    std::fs::create_dir(&second)?;
    let remount = workspace
        .mount(
            &second,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await?;
    assert!(
        std::fs::metadata(second.join("metadata.txt"))?
            .permissions()
            .readonly(),
        "metadata-only edits must survive synchronization and remount"
    );
    remount.unmount().await?;
    Ok(())
}
