//! Native package publication. One stable kernel lock owns recovery and every
//! fixed sidecar; lock ownership never depends on PIDs, clocks or lock deletion.
use acyclic_native_runtime::{FileLock, FileLockMode, RenameMode, private_file};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    fs::{self, File},
    io::{self, Read as _, Seek as _},
    path::{Path, PathBuf},
};

/// Immutable release identity, also the complete recovery intent. It contains
/// no paths, so a journal cannot redirect recovery outside the package.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Package release version.
    pub version: String,
    /// Manifest target identifier.
    pub target: String,
    /// Expected lowercase hexadecimal artifact digest.
    pub sha256: String,
}

/// Artifact paths and manifest identity admitted by the package bootstrap.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Verified native executable supplied by the package.
    pub source: PathBuf,
    /// Existing package command directory.
    pub directory: PathBuf,
    /// Immutable manifest identity to publish.
    pub identity: Identity,
}

/// Durable boundaries exposed for crash/recovery qualification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Stage {
    /// Recovery intent is durable; the previous command remains available.
    Intent,
    /// Complete private binary staging bytes are durable.
    Ready,
    /// Atomic replacement is durable, before executable permissions finish.
    Published,
    /// The complete replacement is executable and durable.
    Activated,
    /// Both executable and identity are durable; recovery cleanup follows.
    IdentityPublished,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn checksum(file: &mut File) -> io::Result<String> {
    file.rewind()?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 16_384];
    loop {
        let length = file.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        hash.update(buffer.get(..length).ok_or(io::ErrorKind::InvalidData)?);
    }
    file.rewind()?;
    Ok(hex::encode(hash.finalize()))
}

fn read_identity(path: &Path) -> io::Result<Option<Identity>> {
    match private_file::read(path, 65_536) {
        Ok(bytes) => serde_json::from_slice(&bytes.0)
            .map(Some)
            .map_err(|_| invalid("unsafe Acyclic install transaction or identity")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn publish_record(path: &Path, temporary: &Path, record: &impl Serialize) -> io::Result<()> {
    private_file::validate_destination(path)?;
    private_file::discard_unpublished(temporary)?;
    private_file::publish(
        path,
        temporary,
        RenameMode::Replace,
        |file| serde_json::to_writer(file, record).map_err(io::Error::other),
        |_| Ok(()),
    )
}

fn executable(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(fs::Permissions::from_mode(0o755))?;
        acyclic_native_runtime::flush_file_all(file)?;
    }
    #[cfg(windows)]
    let _ = file;
    Ok(())
}

fn publish_executable(
    request: &Request,
    installed: &Path,
    temporary: &Path,
    observe: &mut impl FnMut(Stage) -> io::Result<()>,
) -> io::Result<()> {
    let mut source = File::open(&request.source)?;
    if checksum(&mut source)? != request.identity.sha256 {
        return Err(invalid("source binary checksum verification failed"));
    }
    let mut held = None;
    private_file::publish(
        installed,
        temporary,
        RenameMode::Replace,
        |file| {
            io::copy(&mut source, file)?;
            if checksum(file)? != request.identity.sha256 {
                return Err(invalid("staged binary checksum verification failed"));
            }
            held = Some(file.try_clone()?);
            Ok(())
        },
        |stage| {
            match stage {
                private_file::PublicationStage::Synced => observe(Stage::Ready)?,
                private_file::PublicationStage::Published => observe(Stage::Published)?,
                // Installed precedes the directory barrier. Only Published
                // is the durable package boundary exposed to this observer.
                private_file::PublicationStage::Created
                | private_file::PublicationStage::Installed => (),
            }
            Ok(())
        },
    )?;
    executable(&held.ok_or(io::ErrorKind::InvalidData)?)?;
    observe(Stage::Activated)
}

fn finish_executable(installed: &Path, identity: &Identity) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if fs::metadata(installed)?.permissions().mode() & 0o777 != 0o755 {
            // Only our private, unaliased published stage may need chmod.
            // A completed executable (or an alias to it) is never mutated.
            let mut held = private_file::open(installed, false)?;
            if checksum(&mut held)? != identity.sha256 {
                return Err(invalid("activated binary checksum verification failed"));
            }
            executable(&held)?;
        }
    }
    #[cfg(windows)]
    let _ = (installed, identity);
    Ok(())
}

fn admit_identity(
    identity: &Identity,
    previous: Option<&Identity>,
    pending: Option<&Identity>,
    current_hash: Option<&str>,
) -> io::Result<bool> {
    if pending.is_some_and(|intent| intent != identity) {
        return Err(invalid(
            "interrupted install belongs to different release bytes",
        ));
    }
    if previous.is_some_and(|old| old.version == identity.version && old.sha256 != identity.sha256)
    {
        return Err(invalid(
            "immutable Acyclic release has conflicting binary bytes",
        ));
    }
    let activated = current_hash == Some(identity.sha256.as_str());
    if previous.is_some_and(|old| current_hash.is_some_and(|hash| hash != old.sha256.as_str()))
        && !(pending.is_some() && activated)
    {
        return Err(invalid(
            "installed Acyclic binary does not match its durable identity",
        ));
    }
    Ok(activated)
}

/// Install or finish an interrupted publication. The executable is atomically
/// replaced; there is no backup rename or interval with a missing command.
/// A durable identity intent precedes activation. Recovery finishes that same
/// release, including an interrupted executable-permission update.
///
/// Errors retain the intent until both executable and identity are durable.
/// Callers may observe boundaries, but cannot release the lock before return.
pub fn install(
    request: &Request,
    mut observe: impl FnMut(Stage) -> io::Result<()>,
) -> io::Result<PathBuf> {
    let Request {
        directory,
        identity,
        ..
    } = request;
    if identity.sha256.len() != 64
        || !identity
            .sha256
            .bytes()
            .all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(invalid("invalid release SHA-256"));
    }
    let _lock = FileLock::acquire(
        private_file::open_or_create(&directory.join("install.lock"))?,
        FileLockMode::Exclusive,
    )?;
    let installed = directory.join(if cfg!(windows) {
        "acyclic.exe"
    } else {
        "acyclic"
    });
    let identity_path = directory.join("installed-binary.json");
    let intent_path = directory.join("install-transaction.json");
    let binary_next = directory.join(".install-binary.next");
    let identity_next = directory.join(".installed-binary.next");
    let intent_next = directory.join(".install-transaction.next");
    for path in [&binary_next, &identity_next, &intent_next] {
        private_file::discard_unpublished(path)?;
    }
    let previous = read_identity(&identity_path)?;
    let pending = read_identity(&intent_path)?;
    let mut current = match File::open(&installed) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let current_hash = current.as_mut().map(checksum).transpose()?;
    let activated = admit_identity(
        identity,
        previous.as_ref(),
        pending.as_ref(),
        current_hash.as_deref(),
    )?;
    if !activated {
        publish_record(&intent_path, &intent_next, identity)?;
        observe(Stage::Intent)?;
        // Release inspection handles before replacing on Windows. Only the
        // newly created stage's retained descriptor crosses publication.
        drop(current.take());
        publish_executable(request, &installed, &binary_next, &mut observe)?;
    } else if pending.is_some() || previous.is_none() {
        finish_executable(&installed, identity)?;
    }
    if previous.as_ref() != Some(identity) {
        publish_record(&identity_path, &identity_next, identity)?;
    }
    observe(Stage::IdentityPublished)?;
    #[cfg(windows)]
    {
        match fs::remove_file(directory.join("acyclic")) {
            Ok(()) => acyclic_native_runtime::sync_parent(
                directory,
                acyclic_native_runtime::Durability::Full,
            )?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(error) => return Err(error),
        }
    }
    private_file::discard_unpublished(&intent_path)?;
    Ok(installed)
}
