use super::*;

pub(super) fn verify_receipt(args: &[std::ffi::OsString]) -> Result<(), Failure> {
    let receipt_path = option(args, "--verify-receipt")
        .map(PathBuf::from)
        .ok_or("--verify-receipt requires a path")?;
    let executable = option(args, "--release-executable")
        .map(PathBuf::from)
        .ok_or("--verify-receipt requires --release-executable")?;
    let required_kind =
        option(args, "--require-kind").ok_or("--verify-receipt requires --require-kind")?;
    let release_version =
        option(args, "--release-version").ok_or("--verify-receipt requires --release-version")?;
    let expected_arch =
        option(args, "--release-arch").ok_or("--verify-receipt requires --release-arch")?;
    let os = match required_kind.as_str() {
        "linux-fuse" => "linux",
        "macos-nfs" => "macos",
        "windows-projfs" => "windows",
        _ => return Err(format!("unsupported receipt backend: {required_kind}").into()),
    };
    acyclic_fs::mount_qualification::verify(
        File::open(receipt_path)?,
        File::open(executable)?,
        acyclic_fs::mount_qualification::ReleaseTarget {
            os,
            arch: &expected_arch,
            version: &release_version,
        },
    )?;
    Ok(())
}
