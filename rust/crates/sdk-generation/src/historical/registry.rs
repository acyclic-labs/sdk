use super::*;

pub(super) fn registry_response(path: &str) -> Result<Option<Vec<u8>>, String> {
    let url = format!("https://crates.io/api/v1/crates/{path}");
    // Resolve PATH explicitly: Windows otherwise searches System32 before PATH,
    // bypassing the caller's selected registry transport executable.
    let executable = crate::command_var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|path| path.join(if cfg!(windows) { "curl.exe" } else { "curl" }))
                .find(|path| path.is_file())
        })
        .ok_or("registry transport curl is absent from PATH")?;
    let bytes = command_output(sdk_docs::command(executable).args([
        "--silent",
        "--show-error",
        "--location",
        "--user-agent",
        concat!(
            "acyclic-sdk-docs/",
            env!("CARGO_PKG_VERSION"),
            " (https://github.com/acyclic-labs/sdk)"
        ),
        "--max-time",
        "60",
        "--write-out",
        "\n%{http_code}",
        &url,
    ]))?;
    let split = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .ok_or("registry response lacks HTTP status")?;
    let status = &bytes[split + 1..];
    if status == b"404" {
        return Ok(None);
    }
    if status != b"200" {
        return Err(format!(
            "registry lookup failed with HTTP {}",
            String::from_utf8_lossy(status)
        ));
    }
    Ok(Some(bytes[..split].to_vec()))
}

/// Verify archive bytes, VCS provenance and exact historical Cargo identity.
pub fn verify_archive(
    metadata: &Metadata,
    root: &Path,
    revision: &str,
    release: &RegistryVersion,
    archive: &Path,
) -> Result<(ReleasedPackage, BTreeMap<String, String>), String> {
    validate_name(&release.package)?;
    validate_version(&release.num)?;
    if release.checksum.len() != 64
        || !release
            .checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("invalid registry archive checksum".into());
    }
    let actual = crate::sha256_file(archive).map_err(|e| e.to_string())?;
    if actual != format!("sha256:{}", release.checksum) {
        return Err("released registry archive checksum differs".into());
    }
    let members = archive_members(release, archive)?;
    let vcs = archive_vcs(&members)?.ok_or("clean Git binding requires publisher VCS facts")?;
    if vcs.dirty {
        return Err(
            "released archive records dirty source; clean Git provenance is not valid".into(),
        );
    }
    if vcs.revision != revision
        || revision.len() != 40
        || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("released archive VCS revision differs from historical source".into());
    }
    let path = Path::new(&vcs.path_in_vcs);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err("released archive VCS path escapes source root".into());
    }
    let package = metadata
        .packages
        .iter()
        .find(|package| {
            package.name.as_ref() == release.package && package.version.to_string() == release.num
        })
        .ok_or("released archive has no matching historical Cargo package/version")?;
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let source = root.join(path).canonicalize().map_err(|e| e.to_string())?;
    if !source.starts_with(&root)
        || package
            .manifest_path
            .parent()
            .ok_or("package manifest has no parent")?
            .as_std_path()
            .canonicalize()
            .map_err(|e| e.to_string())?
            != source
    {
        return Err("released archive VCS path differs from Cargo package source".into());
    }
    let files = verify_archive_sources(&root, package, &source, archive, &members)?;
    if crate::sha256_file(archive).map_err(|e| e.to_string())? != actual {
        return Err("released archive changed during verification".into());
    }
    Ok((
        ReleasedPackage {
            package: release.package.clone(),
            version: release.num.clone(),
            yanked: release.yanked,
            registry_checksum: release.checksum.clone(),
            source_revision: revision.into(),
            path_in_vcs: vcs.path_in_vcs,
        },
        files,
    ))
}

// Read entries without extracting or rewriting historical sources. Every retained
// native Rust/Markdown byte must agree with the claimed clean checkout.
pub(super) fn verify_archive_sources(
    root: &Path,
    package: &cargo_metadata::Package,
    source: &Path,
    archive: &Path,
    members: &BTreeMap<String, Vec<u8>>,
) -> Result<BTreeMap<String, String>, String> {
    let mut source_inputs = BTreeMap::new();
    let mut retained = BTreeSet::new();
    let mut has_manifest = false;
    for (member, released) in members {
        let relative = member.as_str();
        let original_manifest = relative == "Cargo.toml.orig";
        // Cargo creates the normalized manifest and standalone package lock.
        // The exact archive checksum retains these packaging products; native
        // generation uses Cargo.toml.orig and the frozen workspace Cargo.lock.
        if matches!(
            relative,
            "Cargo.toml" | "Cargo.lock" | ".cargo_vcs_info.json"
        ) {
            continue;
        }
        let relative = if original_manifest {
            "Cargo.toml"
        } else {
            relative
        };
        if !retained.insert(relative.to_owned()) {
            return Err("released archive repeats an authoritative source entry".into());
        }
        let checkout = source.join(relative).canonicalize().map_err(|e| {
            format!("released archive source is absent from checkout: {relative}: {e}")
        })?;
        if !checkout.starts_with(source) {
            return Err("released archive source escapes checkout package".into());
        }
        if fs::read(checkout).map_err(|e| e.to_string())? != *released {
            return Err(format!(
                "released archive source bytes differ from checkout: {relative}"
            ));
        }
        let logical = crate::path_string(
            source
                .join(relative)
                .strip_prefix(root)
                .map_err(|e| e.to_string())?,
        );
        source_inputs.insert(logical, crate::sha256_bytes(released));
        has_manifest |= original_manifest;
    }
    if !has_manifest {
        return Err("released archive has no original Cargo manifest".into());
    }
    // Cargo's explicit sources and the maintained docs source closure must be
    // present too: matching a subset of the archive cannot prove a checkout.
    for target in &package.targets {
        let target_source = target
            .src_path
            .as_std_path()
            .canonicalize()
            .map_err(|e| e.to_string())?;
        let relative = target_source
            .strip_prefix(source)
            .map_err(|_| "historical Cargo target escapes package")?;
        if !retained.contains(&crate::path_string(relative)) {
            return Err(format!(
                "released archive is missing Cargo Rust source: {}",
                relative.display()
            ));
        }
    }
    let mut source_files = Vec::new();
    crate::collect_source_files(root, source, archive, &mut source_files)
        .map_err(|e| e.to_string())?;
    for file in source_files {
        if matches!(
            file.extension().and_then(|ext| ext.to_str()),
            Some("rs" | "md")
        ) {
            let relative =
                crate::path_string(file.strip_prefix(source).map_err(|e| e.to_string())?);
            if !retained.contains(&relative) {
                return Err(format!(
                    "released archive is missing native docs source: {relative}"
                ));
            }
        }
    }
    Ok(source_inputs)
}
