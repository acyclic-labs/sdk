pub(super) use super::owner_record::{Owner, RootWitness};
use super::*;
use std::{fs, io::Read};

/// Root of every project's default harness state: the per-user local data directory. This is the only place the
/// harness consults that directory. Unit tests cannot reach it: a test that does not inject
/// `LocalHarnessConfig::state_directory` fails here instead of sharing the real per-user store with every other
/// test and lane on the machine.
#[cfg(not(test))]
pub(super) fn user_state_root() -> Result<PathBuf> {
    crate::native_environment::state(None)
        .map(|root| root.join("acyclic/harness"))
        .ok_or_else(|| Error::Unsupported("per-user local data directory is unavailable".into()))
}

/// Unit tests have no per-user store; see the production definition.
#[cfg(test)]
pub(super) fn user_state_root() -> Result<PathBuf> {
    Err(Error::Unsupported(UNIT_TESTS_INJECT_STATE.into()))
}

#[cfg(test)]
pub(super) const UNIT_TESTS_INJECT_STATE: &str =
    "unit tests must inject LocalHarnessConfig::state_directory; the per-user store is unreachable";

#[cfg(test)]
thread_local! {
    /// A temporary per-user data root for default state selection, so tests
    /// exercise it without touching real per-user state.
    pub(super) static STATE_ROOT: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
    /// Restores the duplicate-capture defect: owner validation captures the
    /// root again instead of sharing state selection's capture.
    pub(super) static RECAPTURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    // Negative control for accidentally rekeying configured volumes.
    static REKEY_VOLUMES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Replaces a capture's object id and generation, as a filesystem that
    /// reissues ids or renumbers them across remounts.
    pub(super) static INJECTED: std::cell::Cell<Option<([u8; 16], acyclic_fs::NativeRootGeneration)>> =
        const { std::cell::Cell::new(None) };
    /// Negative control: an incarnation without the root's token.
    pub(super) static UNTOKENED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Negative control: a root without stable ids is keyed on its id.
    pub(super) static UNSTABLE_KEYED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Negative control: persist a Unix device number across remounts.
    pub(super) static DEVICE_KEYED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod publication_tests;
#[cfg(test)]
mod state_key_tests;

/// Per-user state is keyed by the checkout's path and its incarnation: a
/// checkout deleted and cloned again at the same path is a new workspace and
/// starts with new state instead of being refused the old one.
pub(super) fn state_directory_in(
    root: &Path,
    project: &LocalProject,
    checkout: Option<&Checkout>,
) -> Result<PathBuf> {
    // The scope separates a volume's state by its storage name and a
    // checkout's by its incarnation.
    let storage_name;
    let (canonical, kind, scope): (PathBuf, &[u8], &[u8]) = match (project, checkout) {
        (LocalProject::Workspace(_), Some(checkout)) => (
            checkout.path.clone(),
            b"checkout\0".as_slice(),
            &checkout.incarnation,
        ),
        (LocalProject::Volume { directory, volume }, None) => {
            storage_name = volume.storage_name(&mut None)?;
            (
                fs::canonicalize(directory).map_err(storage)?,
                b"volume\0".as_slice(),
                storage_name.as_bytes(),
            )
        }
        _ => {
            return Err(Error::Invalid(
                "checkout capture does not match the project".into(),
            ));
        }
    };
    let mut hash = blake3::Hasher::new();
    // Only checkout identity changed. Configured volumes must keep selecting
    // their existing owner, authority and journals.
    let checkout_key = matches!(project, LocalProject::Workspace(_));
    #[cfg(test)]
    let checkout_key = checkout_key || REKEY_VOLUMES.get();
    hash.update(if checkout_key {
        b"acyclic.local.state/v2\0"
    } else {
        b"acyclic.local.state/v1\0"
    });
    hash.update(kind);
    let path_bytes = canonical.as_os_str().as_encoded_bytes();
    hash.update(&(path_bytes.len() as u64).to_le_bytes());
    hash.update(path_bytes);
    hash.update(scope);
    let directory = root.join(hash.finalize().to_hex().as_str());
    let planned = directory::planned_directory(&directory).ok_or_else(|| {
        Error::Invalid("local state directory has an unresolved path suffix".into())
    })?;
    if matches!(project, LocalProject::Workspace(_)) && planned.starts_with(&canonical) {
        return Err(Error::Invalid(
            "per-user harness state would be inside the checkout".into(),
        ));
    }
    Ok(directory)
}

mod directory;
pub(super) use directory::separated_directory;

/// One capture of a checkout root: its canonical path, object identity and
/// incarnation, with the root itself held open. State selection and owner
/// validation share the capture, and workspace binding consumes the held
/// root, so the root that selected and validated state is the root bound.
#[derive(Debug)]
pub(super) struct Checkout {
    pub(super) path: PathBuf,
    /// Evidence validated separately from the token-based state selection.
    pub(super) identity: RootWitness,
    pub(super) incarnation: [u8; 16],
    #[cfg(feature = "native-execution")]
    pub(super) root: acyclic_fs::HeldRoot,
}

#[cfg(test)]
thread_local! {
    /// Root captures on this thread, for exact-count guards.
    pub(super) static CAPTURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Checkout {
    pub(super) fn capture(path: &Path) -> Result<Self> {
        #[cfg(test)]
        CAPTURES.with(|captures| captures.set(captures.get() + 1));
        #[cfg(feature = "native-execution")]
        {
            let path = fs::canonicalize(path).map_err(storage)?;
            let root = acyclic_fs::hold_root(&path).map_err(storage)?;
            let incarnation = root.incarnation().map_err(storage)?;
            let token = incarnation_token(&root)?;
            let (object, generation) = (incarnation.identity.to_bytes(), incarnation.generation);
            #[cfg(test)]
            let (object, generation) = INJECTED.get().unwrap_or((object, generation));
            Ok(Self {
                path,
                identity: RootWitness::capture(object, generation),
                incarnation: incarnation_of(token),
                root,
            })
        }
        #[cfg(not(feature = "native-execution"))]
        {
            let _ = path;
            Err(Error::Unsupported(
                "checkout workspaces require native-execution".into(),
            ))
        }
    }
}

/// The checkout's incarnation token: 128 random bits published once in the
/// held root, exclusively and atomically. A checkout cloned again, even at a
/// recycled object id, has no token and publishes a new one.
#[cfg(feature = "native-execution")]
fn incarnation_token(root: &acyclic_fs::HeldRoot) -> Result<[u8; 16]> {
    super::workspace::volumes::held_directories(
        root,
        Path::new(super::workspace::volumes::INCARNATION_PATH),
    )?
    .read_or_publish(Path::new("token"), || {
        let mut candidate = [0; 16];
        getrandom::fill(&mut candidate)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(candidate)
    })
    .map_err(storage)
}

/// The token locates state across capability changes. Physical root evidence is
/// retained in the owner record and checked before that state is attached.
#[cfg(any(test, feature = "native-execution"))]
pub(super) fn incarnation_of(token: [u8; 16]) -> [u8; 16] {
    #[cfg(test)]
    let token = if UNTOKENED.get() { [0; 16] } else { token };
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.workspace-incarnation/v5\0");
    hash.update(&token);
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    identity
}

impl RootWitness {
    #[cfg(any(test, feature = "native-execution"))]
    pub(super) fn capture(object: [u8; 16], generation: acyclic_fs::NativeRootGeneration) -> Self {
        use acyclic_fs::NativeRootGeneration;
        let object = durable_object(object);
        match generation {
            NativeRootGeneration::FileHandle(handle) => Self::FileHandle { object, handle },
            NativeRootGeneration::StableObjectId => Self::StableObjectId(object),
            NativeRootGeneration::Unstable => {
                #[cfg(test)]
                if UNSTABLE_KEYED.get() {
                    return Self::StableObjectId(object);
                }
                Self::Unstable
            }
        }
    }

    pub(super) fn verify(self, observed: Self) -> Result<()> {
        match (self, observed) {
            // A root first opened without durable native identity has only its
            // token continuity contract. A copied token cannot be distinguished
            // from that root after arbitrary id renumbering across a restart.
            (Self::Unstable, _) => Ok(()),
            (Self::StableObjectId(expected), Self::StableObjectId(actual))
            | (Self::StableObjectId(expected), Self::FileHandle { object: actual, .. })
                if expected == actual => Ok(()),
            (Self::FileHandle { object: expected, handle: expected_handle },
             Self::FileHandle { object: actual, handle: actual_handle })
                if expected == actual && expected_handle == actual_handle => Ok(()),
            (Self::StableObjectId(_), Self::Unstable)
            | (Self::FileHandle { .. }, Self::Unstable | Self::StableObjectId(_)) =>
                Err(Error::Unsupported("checkout root identity capability decreased; retained physical identity cannot be verified".into())),
            _ => Err(Error::Unauthorized("local state belongs to a different checkout root".into())),
        }
    }
}

/// Unix st_dev identifies the current kernel device assignment, not the
/// persisted volume. A remount/reboot may renumber it while the same inode,
/// generation and checkout token remain. The token supplies volume scope.
#[cfg(any(test, feature = "native-execution"))]
fn durable_object(object: [u8; 16]) -> [u8; 16] {
    #[cfg(unix)]
    let object = {
        #[cfg(test)]
        if DEVICE_KEYED.get() {
            return object;
        }
        let mut object = object;
        object[..8].fill(0);
        object
    };
    object
}


impl Owner {
    pub(super) fn open(
        directory: &Path,
        selection: &LocalProject,
        checkout: Option<&Checkout>,
    ) -> Result<Self> {
        let workspace = match (selection, checkout) {
            (LocalProject::Workspace(_), Some(checkout)) => Some(checkout.path.clone()),
            (LocalProject::Volume { .. }, None) => None,
            _ => {
                return Err(Error::Invalid(
                    "checkout capture does not match the project".into(),
                ));
            }
        };
        let path = directory.join("owner.json");
        let workspace_identity = checkout.map(|checkout| checkout.identity);
        let workspace_incarnation = checkout.map(|checkout| checkout.incarnation);
        let owner = match Self::load(&path) {
            Ok(owner) => owner,
            Err(Error::NotFound(_)) => {
                let project = match selection {
                    LocalProject::Volume { volume, .. } => volume.clone(),
                    LocalProject::Workspace(_) => VolumeRef::new(
                        crate::resources::ProviderRef::new(
                            "local".into(),
                            "filesystem".into(),
                            "2".into(),
                            &mut None,
                        )?,
                        crate::identity::fresh_uuid().to_string(),
                        VolumeClass::Project,
                        VolumeOwner::Project(crate::identity::fresh_uuid().to_string()),
                        &mut None,
                    )?,
                };
                let mut key = [0; 32];
                crate::identity::fill_random(&mut key).map_err(storage)?;
                let owner = Self {
                    key,
                    agent: AgentId::new(),
                    task: TaskId::new(),
                    project,
                    workspace: workspace.clone(),
                    workspace_identity,
                    workspace_incarnation,
                };
                key.fill(0);
                if owner.publish(directory, &path)? {
                    owner
                } else {
                    Self::load(&path)?
                }
            }
            Err(error) => return Err(error),
        };
        owner.project.validate(&mut None)?;
        if owner.key == [0; 32]
            || owner.agent.into_bytes() == [0; 16]
            || owner.task.into_bytes() == [0; 16]
            || owner.project.class() != VolumeClass::Project
            || owner.project.provider().family() != "filesystem"
            || owner.workspace != workspace
            || owner.workspace_incarnation != workspace_incarnation
            || matches!(selection, LocalProject::Volume { volume, .. } if volume != &owner.project)
        {
            return Err(Error::Unauthorized(
                "local state belongs to a different workspace or volume".into(),
            ));
        }
        match (owner.workspace_identity, workspace_identity) {
            (Some(retained), Some(observed)) => retained.verify(observed)?,
            (None, None) => {}
            _ => {
                return Err(Error::Unauthorized(
                    "local state checkout identity is missing".into(),
                ));
            }
        }
        Ok(owner)
    }

    fn load(path: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound("local owner".into())
            } else {
                storage(error)
            }
        })?;
        if !metadata.is_file() || metadata.len() > 65_536 {
            return Err(Error::Invalid("invalid local owner record".into()));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.mode() & 0o077 != 0 || metadata.nlink() != 1 {
                return Err(Error::Unauthorized(
                    "local owner record has public permissions".into(),
                ));
            }
        }
        let mut bytes = Vec::new();
        fs::File::open(path)
            .map_err(storage)?
            .take(65_537)
            .read_to_end(&mut bytes)
            .map_err(storage)?;
        let owner =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()));
        bytes.fill(0);
        let owner = owner?;
        // A concurrent publisher may have renamed the complete, synced file
        // but not flushed its name yet. Every reader establishes durability
        // before returning credentials that downstream state will depend on.
        #[cfg(test)]
        let sync = !publication::SKIP_READER_SYNC.get();
        #[cfg(not(test))]
        let sync = true;
        if sync {
            acyclic_native_runtime::sync_parent(
                path.parent()
                    .ok_or_else(|| Error::Invalid("local owner has no parent".into()))?,
                acyclic_native_runtime::Durability::Full,
            )
            .map_err(storage)?;
        }
        Ok(owner)
    }

    // Publication is a fresh temporary file, written and synced, then renamed
    // into place without replacement: a kill at any step leaves either no
    // record or a complete one, never a partial record that refuses reopen.
    fn publish(&self, directory: &Path, path: &Path) -> Result<bool> {
        let temporary = directory.join(format!(".owner-{}.next", crate::identity::fresh_uuid()));
        #[cfg(test)]
        let temporary = if publication::torn() {
            path.to_path_buf()
        } else {
            temporary
        };
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| -> std::io::Result<()> {
            let mut file = options.open(&temporary)?;
            #[cfg(test)]
            publication::step(0)?;
            let mut bytes = serde_json::to_vec(self).map_err(std::io::Error::other)?;
            #[cfg(test)]
            {
                let half = bytes.len() / 2;
                acyclic_native_runtime::write_file_all(&mut file, &bytes[..half])?;
                publication::step(1)?;
                bytes.drain(..half);
            }
            let written = acyclic_native_runtime::write_file_all(&mut file, &bytes);
            bytes.fill(0);
            written?;
            acyclic_native_runtime::flush_file_all(&file)?;
            drop(file);
            #[cfg(test)]
            publication::step(2)?;
            #[cfg(test)]
            if publication::torn() {
                return Ok(());
            }
            acyclic_native_runtime::durable_rename(
                &temporary,
                path,
                acyclic_native_runtime::RenameMode::NoReplace,
                None,
            )?;
            #[cfg(test)]
            publication::step(3)?;
            Ok(())
        })();
        #[cfg(test)]
        if publication::killed(&result) {
            // A killed process runs no cleanup.
            return Err(Error::Storage("killed during owner publication".into()));
        }
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        match result {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(storage(error)),
        }
    }
}

/// Simulated kills at each durable step of owner publication.
#[cfg(test)]
pub(super) mod publication {
    use std::cell::Cell;

    /// The steps a kill can follow: created, half written, synced, renamed.
    pub(in crate::local) const STEPS: usize = 4;
    const KILLED: &str = "killed during owner publication";

    thread_local! {
        pub(super) static SKIP_READER_SYNC: Cell<bool> = const { Cell::new(false) };
        static KILL_AT: Cell<Option<usize>> = const { Cell::new(None) };
        static TORN: Cell<bool> = const { Cell::new(false) };
    }

    /// Kills the next publication on this thread after `step`.
    pub(in crate::local) fn kill_at(step: Option<usize>) {
        KILL_AT.with(|kill| kill.set(step));
    }

    /// Negative control: publish by writing the record in place.
    pub(in crate::local) fn tear(torn: bool) {
        TORN.with(|flag| flag.set(torn));
    }

    pub(super) fn torn() -> bool {
        TORN.with(Cell::get)
    }

    pub(super) fn step(step: usize) -> std::io::Result<()> {
        if KILL_AT.with(Cell::get) == Some(step) {
            KILL_AT.with(|kill| kill.set(None));
            return Err(std::io::Error::other(KILLED));
        }
        Ok(())
    }

    pub(super) fn killed(result: &std::io::Result<()>) -> bool {
        result
            .as_ref()
            .is_err_and(|error| error.to_string() == KILLED)
    }
}
