//! The existing startup lock and readiness pipe precede any client request.
use super::*;
use fs2::FileExt as _;
use std::{
    fs::{File, OpenOptions},
    io::Write as _,
};

pub(super) fn publish(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    if !replace && let Ok(existing) = read(path) {
        return if existing == bytes {
            Ok(())
        } else {
            Err(Error::Conflict("local owner file identity changed".into()))
        };
    }
    let temporary = path.with_extension(format!("{}.next", uuid::Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(storage)?;
    file.write_all(bytes).map_err(storage)?;
    acyclic_native_runtime::sync_file(&file, acyclic_native_runtime::Durability::Full, None)
        .map_err(storage)?;
    drop(file);
    let result = acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        if replace {
            acyclic_native_runtime::RenameMode::Replace
        } else {
            acyclic_native_runtime::RenameMode::NoReplace
        },
        None,
    );
    if let Err(error) = result {
        let _ = std::fs::remove_file(&temporary);
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(storage(error));
        }
        if read(path)? != bytes {
            return Err(Error::Conflict("local owner file identity changed".into()));
        }
    }
    Ok(())
}

pub(super) fn lock(root: &Path, name: &str) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(name))
        .map_err(storage)
}

pub(super) fn secret(root: &Path) -> Result<[u8; 32]> {
    let path = root.join("credential");
    match std::fs::File::open(&path) {
        Ok(file) => super::super::native::read_bounded(file, 32)?
            .try_into()
            .map_err(|_| Error::Invalid("invalid local owner key".into())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut bytes = [0; 32];
            getrandom::fill(&mut bytes).map_err(storage)?;
            publish(&path, &bytes, false)?;
            Ok(bytes)
        }
        Err(error) => Err(storage(error)),
    }
}

pub(in crate::external_executor) async fn binding(
    workspace: &Path,
) -> Result<(Client, String, PathBuf)> {
    let root = directory()?;
    let (workspace, path) = credential::workspace(workspace)?;
    let configurations = crate::local::private_directory(&root.join("workspaces"))?;
    publish(
        &configurations.join(format!("{workspace}.json")),
        &serde_json::to_vec(&path).map_err(storage)?,
        false,
    )?;
    let (client, workspace) = connect_workspace(root, workspace).await?;
    Ok((client, workspace, path))
}

pub(in crate::external_executor) async fn existing_binding(
    workspace: &str,
    selected: Option<&Path>,
) -> Result<(Client, String)> {
    super::super::handoff::workspace_key(workspace)?;
    let root = directory()?;
    let path: PathBuf = super::super::volume::decode(&read(
        &root.join("workspaces").join(format!("{workspace}.json")),
    )?)
    .map_err(storage)?;
    let (key, canonical) = credential::workspace(&path)?;
    if key != workspace {
        return Err(Error::Unauthorized("workspace binding was replaced".into()));
    }
    require_selection(&canonical, selected)?;
    connect_workspace(root, workspace.into()).await
}

fn require_selection(canonical: &Path, selected: Option<&Path>) -> Result<()> {
    if let Some(selected) = selected {
        if selected != canonical && std::fs::canonicalize(selected).map_err(storage)? != canonical {
            return Err(Error::Unauthorized(
                "native result belongs to another bound workspace".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

async fn connect_workspace(root: PathBuf, workspace: String) -> Result<(Client, String)> {
    if let Ok(bytes) = read(&root.join("credential"))
        && let Ok(key) = <[u8; 32]>::try_from(bytes.as_slice())
        && let Ok(bytes) = read(&root.join("address.json"))
    {
        let address = Address::from_bytes(&bytes)?;
        if let Ok(client) = connect(&address, &credential::credential(&key, &workspace)).await {
            return Ok((client, workspace));
        }
    }
    let gate = lock(&root, "startup.lock")?;
    let gate = tokio::task::spawn_blocking(move || {
        gate.lock_exclusive().map_err(storage)?;
        Ok::<_, Error>(gate)
    })
    .await
    .map_err(storage)??;
    let key = secret(&root)?;
    let credential = credential::credential(&key, &workspace);
    if let Ok(bytes) = read(&root.join("address.json")) {
        let address = Address::from_bytes(&bytes)?;
        if let Ok(client) = connect(&address, &credential).await {
            drop(gate);
            return Ok((client, workspace));
        }
    }
    let readiness =
        acyclic_native_runtime::spawn_service_process(&std::env::current_exe().map_err(storage)?)
            .map_err(storage)?;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("acyclic-owner-ready".into())
        .spawn(move || {
            let _ = sender.send(readiness.wait());
        })
        .map_err(storage)?;
    let ready = tokio::time::timeout(std::time::Duration::from_secs(15), receiver)
        .await
        .map_err(storage)?
        .map_err(storage)?
        .map_err(storage)?;
    if !ready {
        return Err(Error::Conflict(
            "local owner exited before readiness".into(),
        ));
    }
    let address = Address::from_bytes(&read(&root.join("address.json"))?)?;
    let client = connect(&address, &credential).await?;
    drop(gate);
    Ok((client, workspace))
}
