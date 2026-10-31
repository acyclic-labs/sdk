pub(super) use super::owner_record::{Owner, RootWitness};
use super::*;
use acyclic_native_runtime::signing_key::SigningCustody;
use std::{fs, io::Read};
#[cfg(any(test, feature = "native-execution"))]
pub(in crate::local) mod exclusion;

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
    if let Some(root) = STATE_ROOT.with_borrow(Clone::clone) {
        return Ok(root.join("acyclic/harness"));
    }
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
    /// Negative control: discard the volume component of a strong witness.
    pub(super) static DISCARD_VOLUME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Observe a contending real owner lock without blocking this test thread.
    static TRY_OWNER_LOCK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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

#[cfg(test)]
pub(super) fn directory(path: &Path) -> Result<PathBuf> {
    separated_directory(path, None)
}

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
    #[cfg(any(test, feature = "native-execution"))]
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
        #[cfg(any(test, feature = "native-execution"))]
        {
            let path = fs::canonicalize(path).map_err(storage)?;
            let root = acyclic_fs::hold_root(&path).map_err(storage)?;
            let incarnation = root.incarnation().map_err(storage)?;
            let token = incarnation_token(&root)?;
            // The witness scopes the object id by the durable volume, never by
            // a device number a remount may reassign.
            let mut object = incarnation.identity.to_bytes();
            object[..8].copy_from_slice(&incarnation.volume.to_le_bytes());
            let generation = incarnation.generation;
            #[cfg(test)]
            let (object, generation) = INJECTED.get().unwrap_or((object, generation));
            Ok(Self {
                path,
                identity: RootWitness::capture(object, generation),
                incarnation: incarnation_of(token),
                root,
            })
        }
        #[cfg(not(any(test, feature = "native-execution")))]
        {
            let _ = path;
            Err(Error::Unsupported(
                "checkout workspaces require native-execution".into(),
            ))
        }
    }
}

/// Ignores every entry of the incarnation directory, itself included.
#[cfg(any(test, feature = "native-execution"))]
const INCARNATION_IGNORE: &[u8; 2] = b"*\n";

/// The checkout's incarnation token: 128 random bits published once in the
/// held root, exclusively and atomically. A checkout cloned again, even at a
/// recycled object id, has no token and publishes a new one.
#[cfg(any(test, feature = "native-execution"))]
fn incarnation_token(root: &acyclic_fs::HeldRoot) -> Result<[u8; 16]> {
    // Capture Git metadata before publishing the SDK-owned token. Publication
    // retains those directory handles even if the checkout's name is replaced.
    let exclusion = exclusion::Exclusion::capture(root)?;
    if let Some(exclusion) = exclusion {
        exclusion.publish()?;
    }
    let incarnation = super::workspace::volumes::held_directories(
        root,
        Path::new(super::workspace::volumes::INCARNATION_PATH),
    )?;
    // Reopening observes two immutable records in the same held directory.
    // Settle their names together; no record escapes before that barrier.
    // First publication still persists the ignore rule before exposing a token.
    if let Some((ignored, token)) = incarnation
        .read_published_pair::<2, 16>(Path::new(".gitignore"), Path::new("token"))
        .map_err(storage)?
    {
        if &ignored != INCARNATION_IGNORE {
            return Err(Error::Invalid(
                "checkout incarnation .gitignore no longer ignores its token".into(),
            ));
        }
        return Ok(token);
    }
    // Git applies a directory's own .gitignore over every outer rule for the
    // entries in it, so no negation in the checkout's .gitignore files, which
    // outrank info/exclude, can bring the token back. Published before the
    // token, so a token is never visible to Git without it.
    let ignored = incarnation
        .read_or_publish(Path::new(".gitignore"), || Ok(*INCARNATION_IGNORE))
        .map_err(storage)?;
    if &ignored != INCARNATION_IGNORE {
        return Err(Error::Invalid(
            "checkout incarnation .gitignore no longer ignores its token".into(),
        ));
    }
    incarnation
        .read_or_publish(Path::new("token"), || {
            let mut candidate = [0; 16];
            crate::identity::fill_random(&mut candidate)
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
        #[cfg(test)]
        let object = if DISCARD_VOLUME.get() {
            let mut object = object;
            object[..8].fill(0);
            object
        } else {
            object
        };
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

// Keep the full physical witness, including its durable volume component. A
// token locates state but cannot authenticate the filesystem that contains it.
// The volume is the platform's durable filesystem id where one exists (Linux
// f_fsid, Windows volume serial); on macOS it is st_dev, so a remount that
// renumbers the device refuses attachment rather than trusting a copied token.

impl Owner {
    pub(super) fn open(
        directory: &Path,
        selection: &LocalProject,
        checkout: Option<&Checkout>,
        custody: SigningCustody<'_>,
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
        let owner = match Self::load(&path, custody) {
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
                let key_ref =
                    acyclic_native_runtime::signing_key::SigningKeyReference::create(custody)
                        .map_err(storage)?;
                let owner = Self {
                    key: None,
                    key_ref: Some(key_ref.reference()),
                    agent: AgentId::new(),
                    task: TaskId::new(),
                    project,
                    workspace: workspace.clone(),
                    workspace_identity,
                    workspace_incarnation,
                };
                if owner.publish_as(
                    directory,
                    &path,
                    acyclic_native_runtime::RenameMode::NoReplace,
                    Some(&key_ref),
                )? {
                    owner
                } else {
                    Self::load(&path, custody)?
                }
            }
            Err(error) => return Err(error),
        };
        owner.project.validate(&mut None)?;
        if owner.key_ref.is_none()
            || owner.key.is_some()
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

    fn load(path: &Path, custody: SigningCustody<'_>) -> Result<Self> {
        // The lock's stable inode survives replacing owner.json. Hold it from
        // before reading a legacy seed until publication or custody rollback,
        // so another reader cannot publish a key this reader still owns.
        let lock_path = path.with_extension("lock");
        let file =
            acyclic_native_runtime::private_file::open_or_create(&lock_path).map_err(|error| {
                match error.kind() {
                    std::io::ErrorKind::NotFound => Error::NotFound("local owner".into()),
                    _ => storage(error),
                }
            })?;
        let acquire: fn(
            fs::File,
            acyclic_native_runtime::FileLockMode,
        ) -> std::io::Result<acyclic_native_runtime::FileLock> =
            acyclic_native_runtime::FileLock::acquire;
        #[cfg(test)]
        let acquire = if TRY_OWNER_LOCK.get() {
            acyclic_native_runtime::FileLock::try_acquire
        } else {
            acquire
        };
        let _publication =
            acquire(file, acyclic_native_runtime::FileLockMode::Exclusive).map_err(storage)?;
        Self::load_record(path, custody)
    }

    fn load_record(path: &Path, custody: SigningCustody<'_>) -> Result<Self> {
        use acyclic_native_runtime::{owner_record, private_file};
        let record = private_file::read_for_upgrade_with(path, 65_536, |file, length| {
            let mut bytes = owner_record::SecretBytes(Vec::with_capacity(
                usize::try_from(length + 1).map_err(std::io::Error::other)?,
            ));
            file.read_to_end(&mut bytes.0)?;
            Ok::<_, std::io::Error>(bytes)
        })
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound("local owner".into()),
            std::io::ErrorKind::PermissionDenied => {
                Error::Unauthorized("local owner is not private".into())
            }
            _ => storage(error),
        })?;
        let legacy = record.0.first() == Some(&b'{');
        let plain = if legacy {
            record
        } else {
            owner_record::decode_with(&record.0, 65_536, None, |bytes| {
                Ok::<_, std::io::Error>(owner_record::SecretBytes(bytes.to_vec()))
            })
            .map_err(|error| Error::Invalid(error.to_string()))?
        };
        let mut owner: Self = serde_json::from_slice(&plain.0)
            .map_err(|_| Error::Invalid("invalid local owner record".into()))?;
        let migrated = owner.retain_signing_custody(custody)?;
        if legacy || migrated.is_some() {
            let directory = path
                .parent()
                .ok_or_else(|| Error::Invalid("owner has no parent".into()))?;
            owner.publish_as(
                directory,
                path,
                acyclic_native_runtime::RenameMode::Replace,
                migrated.as_ref(),
            )?;
        }
        Ok(owner)
    }

    /// One migration for both former JSON files and framed private records.
    /// This is reachable only after `private_file` authenticated the source descriptor.
    fn retain_signing_custody(
        &mut self,
        custody: SigningCustody<'_>,
    ) -> Result<Option<acyclic_native_runtime::signing_key::ProvisionalSigningKey>> {
        match (self.key_ref, self.key.as_mut()) {
            (Some(reference), None) => {
                reference.with_seed(custody, |_| ()).map_err(storage)?;
                Ok(None)
            }
            (None, Some(seed)) => {
                if matches!(custody, SigningCustody::PrivateFile(_)) {
                    return Err(Error::Unauthorized(
                        "explicit custody requires an owner created in that store; recovery: use a fresh explicit-config state directory".into(),
                    ));
                }
                let key =
                    acyclic_native_runtime::signing_key::SigningKeyReference::retain(seed, custody)
                        .map_err(storage)?;
                self.key_ref = Some(key.reference());
                acyclic_native_runtime::owner_record::clear_secret(seed);
                self.key = None;
                Ok(Some(key))
            }
            _ => Err(Error::Unauthorized(
                "local owner signing custody is ambiguous or absent".into(),
            )),
        }
    }

    fn publish_as(
        &self,
        directory: &Path,
        path: &Path,
        mode: acyclic_native_runtime::RenameMode,
        signing_key: Option<&acyclic_native_runtime::signing_key::ProvisionalSigningKey>,
    ) -> Result<bool> {
        use acyclic_native_runtime::{owner_record, private_file};
        let temporary = directory.join(format!(".owner-{}.next", crate::identity::fresh_uuid()));
        let plain = owner_record::SecretBytes(
            serde_json::to_vec(self).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        let bytes = owner_record::encode_with(&plain.0, 65_536, None, |parts| {
            Ok::<_, std::io::Error>(owner_record::SecretBytes(parts.concat()))
        })
        .map_err(storage)?;
        let write = |file: &mut fs::File| -> std::io::Result<()> {
            #[cfg(test)]
            {
                let half = bytes.0.len() / 2;
                acyclic_native_runtime::write_file_all(file, &bytes.0[..half])?;
                publication::step(1)?;
                acyclic_native_runtime::write_file_all(file, &bytes.0[half..])
            }
            #[cfg(not(test))]
            acyclic_native_runtime::write_file_all(file, &bytes.0)
        };
        // Negative control alone recreates the old torn-publication mechanism.
        #[cfg(test)]
        if publication::torn() {
            let mut file = private_file::create(path).map_err(storage)?;
            publication::step(0).map_err(storage)?;
            write(&mut file).map_err(storage)?;
            return Ok(true);
        }
        let observe = |stage| {
            if let Some(key) = signing_key {
                key.observe_publication(stage);
            }
            #[cfg(test)]
            publication::step(match stage {
                private_file::PublicationStage::Created => 0,
                private_file::PublicationStage::Synced => 2,
                private_file::PublicationStage::Published => 3,
                private_file::PublicationStage::Installed => return Ok(()),
            })?;
            #[cfg(not(test))]
            let _ = stage;
            Ok(())
        };
        match mode {
            acyclic_native_runtime::RenameMode::NoReplace => {
                private_file::publish_new(path, &temporary, write, observe)
            }
            acyclic_native_runtime::RenameMode::Replace => {
                private_file::publish(path, &temporary, mode, write, observe).map(|()| true)
            }
        }
        .map_err(storage)
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
}
