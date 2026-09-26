//! Crash-safe persisted adapter state.

use super::*;

/// Bounds a watcher fence; a late notification forces a sound rescan.
pub(crate) const WATCH_FENCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
pub(crate) const MAXIMUM_ADAPTER_STATE_BYTES: u64 = 4 * 1024 * 1024;
pub(crate) const ADAPTER_STATE_VERSION: u32 = 4;
pub(crate) const MAXIMUM_ADAPTER_ROOTS: usize = 256;
pub(crate) const MAXIMUM_ADAPTER_ROUTES: usize = 4_096;
pub(crate) const MAXIMUM_ADAPTER_TURNS: usize = 16_384;
pub(crate) const MAXIMUM_ADAPTER_ROOT_TURNS: usize = 16_384;
pub(crate) const MAXIMUM_ADAPTER_PENDING: usize = 4_096;
pub(crate) const MAXIMUM_ADAPTER_LEASES: usize = 16_384;
pub(crate) const MAXIMUM_ADAPTER_DISCARDS: usize = 4_096;

/// Adapter state is saved into self-validating slots rewritten in place.
/// Flushed saves alternate between two slots, and a save only ever overwrites
/// the slot that does not hold the newest flushed save, so a torn write can
/// damage nothing but itself. Unflushed saves go to a third slot that loading
/// prefers only while it is intact and newer, so losing one to a power loss
/// leaves the last flushed save.
pub(crate) const ADAPTER_STATE_SLOTS: [&str; 3] =
    ["adapter-state.a", "adapter-state.b", "adapter-state.v"];

/// The failures an adapter-state transition must survive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Survives {
    /// Written without a flush, as the last action of a request. Nothing acts
    /// on it until it is durable: the session flushes it before its next
    /// request and before publishing it to other sessions, and loading a
    /// session makes it durable first. Losing it to a power loss therefore
    /// leaves exactly the state that a crash just before it leaves.
    ServiceCrash,
    /// Flushed before the caller continues.
    PowerLoss,
}
pub(crate) const ADAPTER_STATE_MAGIC: [u8; 8] = *b"ACYSTAT1";
/// Magic, little-endian generation, then the digest of generation and payload.
pub(crate) const ADAPTER_STATE_HEADER_BYTES: usize = 8 + 8 + 32;

pub(crate) enum StateSlot {
    Missing,
    Torn,
    Saved { generation: u64, payload: Vec<u8> },
}

impl StateSlot {
    pub(crate) const fn generation(&self) -> Option<u64> {
        match self {
            Self::Saved { generation, .. } => Some(*generation),
            Self::Missing | Self::Torn => None,
        }
    }
}

pub(crate) fn state_digest(generation: u64, payload: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&generation.to_le_bytes());
    hasher.update(payload);
    *hasher.finalize().as_bytes()
}

pub(crate) fn read_state_slot(path: &Path) -> Result<StateSlot, String> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(StateSlot::Missing),
        Err(error) => return Err(display(error)),
    };
    let bound = MAXIMUM_ADAPTER_STATE_BYTES + ADAPTER_STATE_HEADER_BYTES as u64;
    let mut bytes = Vec::new();
    file.take(bound + 1)
        .read_to_end(&mut bytes)
        .map_err(display)?;
    if bytes.len() as u64 > bound {
        return Err(format!(
            "adapter state exceeds the {MAXIMUM_ADAPTER_STATE_BYTES} byte bound"
        ));
    }
    let parsed = bytes.split_first_chunk::<8>().and_then(|(magic, rest)| {
        let (generation, rest) = rest.split_first_chunk::<8>()?;
        let (digest, payload) = rest.split_first_chunk::<32>()?;
        Some((magic, u64::from_le_bytes(*generation), digest, payload))
    });
    Ok(match parsed {
        Some((magic, generation, digest, payload))
            if *magic == ADAPTER_STATE_MAGIC && *digest == state_digest(generation, payload) =>
        {
            StateSlot::Saved {
                generation,
                payload: payload.to_vec(),
            }
        }
        _ => StateSlot::Torn,
    })
}

/// What the owning process knows of a session's state slots. It is read once,
/// when the session loads, and kept in step with every save, so a save writes
/// its target slot and reads nothing.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StateSlots {
    /// The generation of each flushed slot's durable save; `None` when the
    /// slot is missing or torn, or its last write did not complete durably.
    /// Such a slot is never the newest flushed save, so it stays the target
    /// until a flushed save completes in it.
    pub(crate) flushed: [Option<u64>; 2],
    /// Whether each slot's directory entry is durable. The unflushed slot's
    /// is known only once this process flushed it.
    pub(crate) durable_entry: [bool; 3],
    /// The highest generation read or ever written, whether or not the write
    /// completed, so every save is newer than anything any slot can hold.
    pub(crate) last_generation: u64,
}

impl StateSlots {
    /// Reads every slot as `[flushed, flushed, unflushed]`.
    pub(crate) fn read(data: &Path) -> Result<(Self, [StateSlot; 3]), String> {
        let [first, second, unflushed] =
            ADAPTER_STATE_SLOTS.map(|name| read_state_slot(&data.join(name)));
        let slots = [first?, second?, unflushed?];
        let known = Self {
            flushed: [slots[0].generation(), slots[1].generation()],
            durable_entry: [
                !matches!(slots[0], StateSlot::Missing),
                !matches!(slots[1], StateSlot::Missing),
                false,
            ],
            last_generation: slots
                .iter()
                .filter_map(StateSlot::generation)
                .max()
                .unwrap_or(0),
        };
        Ok((known, slots))
    }

    /// The flushed slot holding the newest flushed save.
    pub(crate) fn newest_flushed(&self) -> usize {
        usize::from(self.flushed[1] > self.flushed[0])
    }

    /// Makes the unflushed slot's save durable, with its directory entry
    /// until that is.
    pub(crate) fn flush_unflushed(&mut self, data: &Path) -> Result<(), String> {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(data.join(ADAPTER_STATE_SLOTS[2]))
            .map_err(display)?;
        sync_file(&file, Durability::Full).map_err(display)?;
        if !self.durable_entry[2] {
            sync_parent(data, Durability::Full).map_err(display)?;
            self.durable_entry[2] = true;
        }
        Ok(())
    }

    pub(crate) fn save(
        &mut self,
        data: &Path,
        state: &AdapterState,
        survives: Survives,
    ) -> Result<(), String> {
        validate_state_version(state)?;
        validate_state_bounds(state)?;
        let mut serialized = BoundedJsonBuffer::new();
        serde_json::to_writer(&mut serialized, state)
            .ok()
            .filter(|()| serialized.bytes.len() as u64 <= MAXIMUM_ADAPTER_STATE_BYTES)
            .ok_or_else(|| {
                format!("adapter state exceeds the {MAXIMUM_ADAPTER_STATE_BYTES} byte bound")
            })?;
        let generation = self
            .last_generation
            .checked_add(1)
            .ok_or("adapter state generation is exhausted")?;
        self.last_generation = generation;
        let target = match survives {
            Survives::ServiceCrash => 2,
            Survives::PowerLoss => {
                let target = 1 - self.newest_flushed();
                if let Some(flushed) = self.flushed.get_mut(target) {
                    *flushed = None;
                }
                target
            }
        };
        let name = ADAPTER_STATE_SLOTS
            .get(target)
            .ok_or("adapter state slot is out of range")?;
        let mut bytes = Vec::with_capacity(ADAPTER_STATE_HEADER_BYTES + serialized.bytes.len());
        bytes.extend_from_slice(&ADAPTER_STATE_MAGIC);
        bytes.extend_from_slice(&generation.to_le_bytes());
        bytes.extend_from_slice(&state_digest(generation, &serialized.bytes));
        bytes.extend_from_slice(&serialized.bytes);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(data.join(name))
            .map_err(display)?;
        file.write_all(&bytes).map_err(display)?;
        file.set_len(bytes.len() as u64).map_err(display)?;
        if let (Some(flushed), Some(durable_entry)) = (
            self.flushed.get_mut(target),
            self.durable_entry.get_mut(target),
        ) {
            sync_file(&file, Durability::Full).map_err(display)?;
            if !*durable_entry {
                sync_parent(data, Durability::Full).map_err(display)?;
                *durable_entry = true;
            }
            *flushed = Some(generation);
        }
        Ok(())
    }
}

pub(crate) fn load_state(data: &Path) -> Result<(AdapterState, StateSlots), String> {
    let (mut slots, [first, second, unflushed]) = StateSlots::read(data)?;
    let (flushed, other) = if slots.newest_flushed() == 1 {
        (second, first)
    } else {
        (first, second)
    };
    let newest = if unflushed.generation() > flushed.generation() {
        // A process that exited before flushing its last save leaves it only
        // in memory; nothing may act on it before it is durable.
        slots.flush_unflushed(data)?;
        unflushed
    } else {
        flushed
    };
    let state = match (newest, other) {
        (StateSlot::Saved { payload, .. }, _) => {
            let state = serde_json::from_slice(&payload).map_err(display)?;
            validate_state_version(&state)?;
            validate_state_bounds(&state)?;
            state
        }
        // No flushed save was ever made, and no unflushed one survives.
        (StateSlot::Missing, StateSlot::Missing) => AdapterState {
            version: ADAPTER_STATE_VERSION,
            ..AdapterState::default()
        },
        _ => return Err("adapter state holds no completed save".to_owned()),
    };
    Ok((state, slots))
}

/// Loads a session directory's state without keeping its slots.
#[cfg(test)]
pub(crate) fn load_saved_state(data: &Path) -> Result<AdapterState, String> {
    load_state(data).map(|(state, _)| state)
}

/// Saves into a session directory that no loaded session owns.
#[cfg(test)]
pub(crate) fn save_state(
    data: &Path,
    state: &AdapterState,
    survives: Survives,
) -> Result<(), String> {
    StateSlots::read(data)?.0.save(data, state, survives)
}

/// When the session last completed a save, for newest-first recovery order.
pub(crate) fn state_modified(data: &Path) -> std::time::SystemTime {
    ADAPTER_STATE_SLOTS
        .iter()
        .filter_map(|slot| {
            data.join(slot)
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
        })
        .max()
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
}

pub(crate) fn validate_state_version(state: &AdapterState) -> Result<(), String> {
    if state.version == ADAPTER_STATE_VERSION {
        Ok(())
    } else {
        Err(format!(
            "unsupported Acyclic adapter state version {}; expected {ADAPTER_STATE_VERSION}. Leave the old store untouched or remove it manually",
            state.version
        ))
    }
}

pub(crate) fn validate_state_bounds(state: &AdapterState) -> Result<(), String> {
    for (name, observed, maximum) in [
        ("roots", state.roots.len(), MAXIMUM_ADAPTER_ROOTS),
        (
            "pending root adoptions",
            state.pending_root_adoptions.len(),
            MAXIMUM_ADAPTER_ROOTS,
        ),
        ("routes", state.routes.len(), MAXIMUM_ADAPTER_ROUTES),
        ("turns", state.turns.len(), MAXIMUM_ADAPTER_TURNS),
        (
            "root turns",
            state.root_turns.len(),
            MAXIMUM_ADAPTER_ROOT_TURNS,
        ),
        (
            "pending spawns",
            state.pending.len(),
            MAXIMUM_ADAPTER_PENDING,
        ),
        ("leases", state.leases.len(), MAXIMUM_ADAPTER_LEASES),
        (
            "pending discards",
            state.pending_discards.len(),
            MAXIMUM_ADAPTER_DISCARDS,
        ),
    ] {
        if observed > maximum {
            return Err(format!(
                "adapter state has too many {name}: {observed} > {maximum}"
            ));
        }
    }
    if !state
        .pending_root_adoptions
        .iter()
        .all(|key| state.roots.contains_key(key))
    {
        return Err("pending root adoption has no durable binding".to_owned());
    }
    if state
        .roots
        .iter()
        .any(|(key, root)| key != &root_key(WorkspaceRootId::from_bytes(root.root_id)))
        || state.routes.values().any(|route| {
            route
                .roots
                .iter()
                .any(|(key, root)| key != &root_key(WorkspaceRootId::from_bytes(root.root_id)))
        })
        || state.leases.values().any(|lease| {
            lease
                .roots
                .iter()
                .any(|(key, root)| key != &root_key(WorkspaceRootId::from_bytes(root.root_id)))
        })
    {
        return Err("adapter root map key differs from its typed root identity".to_owned());
    }
    if state.pending_root_registration
        && (state.root_session_id.is_empty()
            || state.root_id == [0; 16]
            || state.root_context_id == [0; 16]
            || state.roots.len() != 1
            || !state
                .roots
                .contains_key(&root_key(WorkspaceRootId::from_bytes(state.root_id))))
    {
        return Err("pending root registration is incomplete".to_owned());
    }
    if state
        .routes
        .values()
        .any(|route| route.roots.len() > MAXIMUM_ADAPTER_ROOTS)
        || state
            .pending
            .iter()
            .any(|pending| pending.roots.len() > MAXIMUM_ADAPTER_ROOTS)
        || state
            .leases
            .values()
            .any(|lease| lease.roots.len() > MAXIMUM_ADAPTER_ROOTS)
    {
        return Err("adapter route or lease exceeds the root bound".to_owned());
    }
    if state.pending.iter().any(|pending| {
        let pending_key = pending.mount_name();
        pending
            .mount_path
            .file_name()
            .and_then(|name| name.to_str())
            != Some(pending_key.as_str())
            || pending
                .mount_path
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                != Some("w")
            || pending
                .roots
                .iter()
                .any(|(key, root)| key != &root_key(root.id()))
            || (pending.lifecycle == PendingSpawnLifecycle::Preparing && !pending.roots.is_empty())
            || (matches!(
                pending.lifecycle,
                PendingSpawnLifecycle::Mounting | PendingSpawnLifecycle::Prepared
            ) && pending.roots.is_empty())
    }) {
        return Err("adapter pending spawn is structurally invalid".to_owned());
    }
    if state.pending_discards.values().any(|discard| {
        discard.agents.len() > MAXIMUM_ADAPTER_ROUTES
            || discard.agents.iter().any(|agent| {
                agent.repository_workspace_ids.is_empty()
                    || agent.repository_workspace_ids.len() > MAXIMUM_ADAPTER_ROOTS
                    || agent.workspaces.len() > MAXIMUM_ADAPTER_ROOTS
            })
    }) {
        return Err("adapter discard queue exceeds its structural bound".to_owned());
    }
    Ok(())
}

pub(crate) fn remove_tree_checked(root: &Path, target: &Path) -> Result<(), String> {
    let root = root.canonicalize().map_err(display)?;
    let parent = target
        .parent()
        .ok_or_else(|| "discard target has no parent".to_owned())?
        .canonicalize()
        .map_err(display)?;
    if parent != root {
        return Err("refusing to discard a path outside the plugin workspace root".to_owned());
    }
    if !target.exists() {
        return Ok(());
    }
    let target = match target.canonicalize() {
        Ok(target) => target,
        #[cfg(target_os = "windows")]
        Err(error) if error.raw_os_error() == Some(369) => return Ok(()),
        Err(error) => return Err(display(error)),
    };
    if target.parent() != Some(root.as_path()) {
        return Err("refusing to discard a path outside the plugin workspace root".to_owned());
    }
    match fs::remove_dir_all(target) {
        Ok(()) => Ok(()),
        #[cfg(target_os = "windows")]
        Err(error) if error.raw_os_error() == Some(369) => Ok(()),
        Err(error) => Err(display(error)),
    }
}
