//! Pi installation and diagnostics over the existing configuration owner.

use super::*;

pub(crate) fn pi_settings_path(project: bool) -> Result<PathBuf, String> {
    if project {
        Ok(env::current_dir()
            .map_err(display)?
            .join(".pi/settings.json"))
    } else {
        let directory = match env::var_os("PI_CODING_AGENT_DIR") {
            Some(directory) => PathBuf::from(directory),
            None => home_directory()?.join(".pi/agent"),
        };
        Ok(directory.join("settings.json"))
    }
}

fn pi_extension_asset(assets: &Path, executable: &Path) -> Result<(PathBuf, String), String> {
    let source = include_str!("../pi/index.ts").replace(
        "\"__ACYCLIC_EXECUTABLE__\"",
        &serde_json::to_string(executable).map_err(display)?,
    );
    // Content-addressing lets settings retain a pinned asset across upgrades.
    // A preexisting different asset is corruption, never something to overwrite.
    let extension = assets.join(format!("{}.ts", blake3::hash(source.as_bytes()).to_hex()));
    Ok((extension, source))
}

fn verify_pi_extension_asset(extension: &Path, source: &str) -> Result<(), String> {
    let mut installed_bytes = Vec::new();
    fs::File::open(extension)
        .and_then(|file| {
            file.take(source.len() as u64 + 1)
                .read_to_end(&mut installed_bytes)
        })
        .map_err(|error| {
            format!(
                "cannot read Pi extension asset at {}: {error}",
                extension.display()
            )
        })?;
    if installed_bytes != source.as_bytes() {
        return Err(format!(
            "Pi extension asset at {} differs from its content identity",
            extension.display()
        ));
    }
    Ok(())
}

pub(crate) fn install_pi_at(
    settings: &Path,
    assets: &Path,
    executable: &Path,
) -> Result<(), String> {
    let (extension, source) = pi_extension_asset(assets, executable)?;
    fs::create_dir_all(assets).map_err(display)?;
    let temporary = assets.join(format!("{}.next", uuid::Uuid::new_v4()));
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
    {
        Ok(mut file) => {
            let publish = (|| {
                file.write_all(source.as_bytes())?;
                sync_file(&file, Durability::Full, None)?;
                drop(file);
                durable_rename(&temporary, &extension, RenameMode::NoReplace, None)
            })();
            let _ = fs::remove_file(&temporary);
            if let Err(error) = publish
                && error.kind() != io::ErrorKind::AlreadyExists
            {
                return Err(display(error));
            }
        }
        Err(error) => return Err(display(error)),
    }
    verify_pi_extension_asset(&extension, &source)?;
    install_owned_json(settings, "pi-extension", "Pi settings", |prior| {
        let mut document = prior.cloned().unwrap_or_else(|| json!({}));
        let object = document
            .as_object_mut()
            .ok_or_else(|| "Pi settings must contain an object".to_owned())?;
        let extensions = object
            .entry("extensions")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| "Pi extensions must be an array".to_owned())?;
        let path = serde_json::to_value(&extension).map_err(display)?;
        if !extensions.contains(&path) {
            extensions.push(path);
        }
        Ok(document)
    })
}
