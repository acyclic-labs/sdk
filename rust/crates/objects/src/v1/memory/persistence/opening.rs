use super::*;

pub(crate) fn open(
    root: PathBuf,
    limits: LocalObjectsLimits,
    anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
) -> Result<MemoryObjects, LocalOpenError> {
    let span = obs::span!(
        INFO,
        "acyclic.objects.open",
        frames = obs::Empty,
        bytes = obs::Empty
    );
    obs::scoped(&span, || {
        if limits.maximum_object_bytes == 0
            || limits.maximum_object_bytes > limits.maximum_bytes
            || limits.maximum_journal_operations == 0
            || limits.maximum_journal_bytes < 256
        {
            return Err(LocalOpenError::Invalid);
        }
        let options = MemoryOptions {
            maximum_bytes: usize::try_from(limits.maximum_bytes)
                .map_err(|_| LocalOpenError::Invalid)?,
            maximum_entries: usize::MAX,
        };
        fs::create_dir_all(root.join("segments"))?;
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("owner.lock"))?;
        owner.try_lock_exclusive().map_err(|error| {
            if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
                LocalOpenError::AlreadyOwned
            } else {
                LocalOpenError::Io(error)
            }
        })?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("mutations.log"))?;
        let header = load_header(&mut file, &root, limits, getrandom::fill)?;
        let root = Arc::new(crate::physical::LocalRoot::new(root));
        let (state, operations, inline_bytes) = replay(&mut file, &root, options, limits)?;
        let bytes = file.stream_position()?;
        obs::record!("frames" = operations, "bytes" = bytes);
        let mut references = BTreeSet::new();
        for bucket in state.buckets.values() {
            for object in bucket.objects.values() {
                object.body.local_references(&mut references);
            }
        }
        for upload in state.uploads.values() {
            for (_, body) in upload.parts.values() {
                body.local_references(&mut references);
            }
        }
        crate::physical::validate_referenced_segments(
            &root.path,
            &references,
            limits.maximum_object_bytes,
        )
        .map_err(corrupt)?;
        let key = header.cursor_key.as_slice().try_into().map_err(corrupt)?;
        Ok(MemoryObjects {
            state: Arc::new(Mutex::new(state)),
            options,
            token_key: Arc::new(Mutex::new(Some(key))),
            defer_local: false,
            journal: Some(Arc::new(Journal {
                root,
                limits,
                header,
                tail: Mutex::new(Tail {
                    file,
                    bytes,
                    operations,
                    inline_bytes,
                }),
                poisoned: AtomicBool::new(false),
                _owner: owner,
                _anchor: anchor,
                #[cfg(test)]
                fault_bytes: std::sync::atomic::AtomicU64::new(u64::MAX),
                #[cfg(test)]
                fault_sync: AtomicBool::new(false),
                #[cfg(test)]
                fault_checkpoint: std::sync::atomic::AtomicU64::new(0),
            })),
        })
    })
}

pub(super) fn load_header(
    file: &mut File,
    root: &Path,
    limits: LocalObjectsLimits,
    fill: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>,
) -> Result<Header, LocalOpenError> {
    if file.metadata()?.len() == 0 {
        let key = fresh_cursor_key(fill).map_err(|_| LocalOpenError::Unavailable)?;
        let header = Header {
            object_bytes: limits.maximum_object_bytes,
            total_bytes: limits.maximum_bytes,
            operations: limits.maximum_journal_operations,
            journal_bytes: limits.maximum_journal_bytes,
            cursor_key: key.to_vec(),
        };
        let encoded = header.encode_to_vec();
        let length = u32::try_from(encoded.len()).map_err(corrupt)?;
        file.write_all(MAGIC)?;
        file.write_all(&length.to_le_bytes())?;
        file.write_all(blake3::hash(&encoded).as_bytes())?;
        file.write_all(&encoded)?;
        sync(file, limits)?;
        acyclic_native_runtime::sync_parent(root, native_durability(limits.durability))?;
        return Ok(header);
    }
    let mut magic = [0; MAGIC.len()];
    file.read_exact(&mut magic).map_err(corrupt)?;
    if magic.as_slice() != MAGIC {
        return Err(LocalOpenError::Corrupt);
    }
    let mut length = [0; 4];
    file.read_exact(&mut length).map_err(corrupt)?;
    let length = u32::from_le_bytes(length) as usize;
    if !(1..=512).contains(&length) {
        return Err(LocalOpenError::Corrupt);
    }
    let mut digest = [0; 32];
    file.read_exact(&mut digest).map_err(corrupt)?;
    let mut encoded = vec![0; length];
    file.read_exact(&mut encoded).map_err(corrupt)?;
    if *blake3::hash(&encoded).as_bytes() != digest {
        return Err(LocalOpenError::Corrupt);
    }
    let header = Header::decode(encoded.as_slice()).map_err(corrupt)?;
    if header.encode_to_vec() != encoded
        || header.cursor_key.len() != 32
        || header.object_bytes != limits.maximum_object_bytes
        || header.total_bytes != limits.maximum_bytes
        || header.operations != limits.maximum_journal_operations
        || header.journal_bytes != limits.maximum_journal_bytes
    {
        return Err(LocalOpenError::Invalid);
    }
    Ok(header)
}

pub(super) fn replay(
    file: &mut File,
    root: &Arc<crate::physical::LocalRoot>,
    options: MemoryOptions,
    limits: LocalObjectsLimits,
) -> Result<(State, u64, u64), LocalOpenError> {
    obs::scoped(&obs::span!(DEBUG, "acyclic.objects.journal.replay"), || {
        let mut state = State::default();
        let mut operations = 0;
        let mut inline_bytes = 0_u64;
        let mut inline_bodies = BTreeMap::new();
        let end = file.metadata()?.len();
        if end > limits.maximum_journal_bytes {
            return Err(LocalOpenError::Corrupt);
        }
        loop {
            let start = file.stream_position()?;
            if start == end {
                return Ok((state, operations, inline_bytes));
            }
            let mut prefix = [0; 36];
            if let Err(error) = file.read_exact(&mut prefix) {
                return repair_tail(file, start, error, limits)
                    .map(|()| (state, operations, inline_bytes));
            }
            let size = u32::from_le_bytes(
                prefix
                    .get(..4)
                    .ok_or(LocalOpenError::Corrupt)?
                    .try_into()
                    .map_err(corrupt)?,
            ) as usize;
            if !(1..=RECORD_LIMIT).contains(&size) {
                return Err(LocalOpenError::Corrupt);
            }
            let mut encoded = vec![0; size];
            if let Err(error) = file.read_exact(&mut encoded) {
                return repair_tail(file, start, error, limits)
                    .map(|()| (state, operations, inline_bytes));
            }
            if blake3::hash(&encoded).as_bytes().as_slice()
                != prefix.get(4..).ok_or(LocalOpenError::Corrupt)?
            {
                return Err(LocalOpenError::Corrupt);
            }
            let delta = Delta::decode(encoded.as_slice()).map_err(corrupt)?;
            if delta.encode_to_vec() != encoded
                || delta.ordinal != operations + 1
                || operations >= limits.maximum_journal_operations
            {
                return Err(LocalOpenError::Corrupt);
            }
            inline::authenticate(&delta, start, &encoded, &mut inline_bodies)?;
            inline_bytes = inline_bytes.saturating_add(delta.inline_data.len() as u64);
            apply(&mut state, delta, root, options, limits)?;
            operations += 1;
        }
    })
}

pub(super) fn repair_tail(
    file: &mut File,
    start: u64,
    error: std::io::Error,
    limits: LocalObjectsLimits,
) -> Result<(), LocalOpenError> {
    if error.kind() != std::io::ErrorKind::UnexpectedEof {
        return Err(error.into());
    }
    file.set_len(start)?;
    file.seek(SeekFrom::Start(start))?;
    sync(file, limits)?;
    Ok(())
}
