use super::*;

#[derive(Clone)]
pub(super) struct TreeBackend(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

#[derive(Debug, Error)]
#[error("tree test backend unavailable")]
pub(super) struct TreeBackendError;

impl TreeBackend {
    pub(super) fn source() -> Self {
        Self(Arc::new(Mutex::new(source_tree())))
    }

    pub(super) fn snapshot(&self) -> BTreeMap<String, Vec<u8>> {
        self.0.lock().expect("tree state").clone()
    }
}

pub(super) fn source_tree() -> BTreeMap<String, Vec<u8>> {
    BTreeMap::from([
        ("a".to_owned(), b"old-a".to_vec()),
        ("b".to_owned(), b"old-b".to_vec()),
        ("from".to_owned(), b"old-from".to_vec()),
        ("to".to_owned(), b"old-to".to_vec()),
    ])
}

pub(super) fn target_tree() -> BTreeMap<String, Vec<u8>> {
    BTreeMap::from([
        ("a".to_owned(), b"new-a".to_vec()),
        ("to".to_owned(), b"old-from".to_vec()),
    ])
}

impl MaterializationBackend for TreeBackend {
    type Error = TreeBackendError;
    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        let entries = self.0.lock().map_err(|_| TreeBackendError)?;
        let image = match edit {
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                encode_preimages([entries.get(path).cloned(), None])
            }
            MaterializationEdit::Rename { from, to } => {
                encode_preimages([entries.get(from).cloned(), entries.get(to).cloned()])
            }
            _ => return Err(TreeBackendError),
        };
        Ok(MaterializationPreimage {
            image,
            custody: None,
        })
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        _preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let mut entries = self.0.lock().map_err(|_| TreeBackendError)?;
        match edit {
            MaterializationEdit::Install { path, image } => {
                entries.insert(path.clone(), image.clone());
            }
            MaterializationEdit::Remove { path } => {
                entries.remove(path);
            }
            MaterializationEdit::Rename { from, to } => {
                if let Some(value) = entries.remove(from) {
                    entries.insert(to.clone(), value);
                } else if !entries.contains_key(to) {
                    return Err(TreeBackendError);
                }
            }
            _ => return Err(TreeBackendError),
        }
        Ok(())
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let [first, second] = decode_preimages(&preimage.image)?;
        let mut entries = self.0.lock().map_err(|_| TreeBackendError)?;
        let restore = |entries: &mut BTreeMap<String, Vec<u8>>, path: &str, value| match value {
            Some(value) => {
                entries.insert(path.to_owned(), value);
            }
            None => {
                entries.remove(path);
            }
        };
        match edit {
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                restore(&mut entries, path, first);
            }
            MaterializationEdit::Rename { from, to } => {
                restore(&mut entries, from, first);
                restore(&mut entries, to, second);
            }
            _ => return Err(TreeBackendError),
        }
        Ok(())
    }
}

fn encode_preimages(values: [Option<Vec<u8>>; 2]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for value in values {
        match value {
            Some(bytes) => {
                encoded.push(1);
                encoded.extend_from_slice(
                    &u32::try_from(bytes.len())
                        .expect("small test entry")
                        .to_le_bytes(),
                );
                encoded.extend_from_slice(&bytes);
            }
            None => encoded.push(0),
        }
    }
    encoded
}

fn decode_preimages(bytes: &[u8]) -> Result<[Option<Vec<u8>>; 2], TreeBackendError> {
    let mut cursor = 0;
    let mut values = [None, None];
    for value in &mut values {
        let tag = *bytes.get(cursor).ok_or(TreeBackendError)?;
        cursor += 1;
        if tag == 0 {
            continue;
        }
        if tag != 1 {
            return Err(TreeBackendError);
        }
        let length_bytes: [u8; 4] = bytes
            .get(cursor..cursor + 4)
            .ok_or(TreeBackendError)?
            .try_into()
            .map_err(|_| TreeBackendError)?;
        cursor += 4;
        let length =
            usize::try_from(u32::from_le_bytes(length_bytes)).map_err(|_| TreeBackendError)?;
        let end = cursor.checked_add(length).ok_or(TreeBackendError)?;
        *value = Some(bytes.get(cursor..end).ok_or(TreeBackendError)?.to_vec());
        cursor = end;
    }
    if cursor != bytes.len() {
        return Err(TreeBackendError);
    }
    Ok(values)
}

#[derive(Clone)]
pub(super) struct MutationCrashBackend {
    pub(super) inner: TreeBackend,
    pub(super) fail_capture: Arc<AtomicUsize>,
    pub(super) fail_apply: Arc<AtomicUsize>,
    pub(super) fail_restore: Arc<AtomicUsize>,
}

impl MutationCrashBackend {
    pub(super) fn new(inner: TreeBackend) -> Self {
        Self {
            inner,
            fail_capture: Arc::new(AtomicUsize::new(0)),
            fail_apply: Arc::new(AtomicUsize::new(0)),
            fail_restore: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn before_capture(inner: TreeBackend, ordinal: usize) -> Self {
        let backend = Self::new(inner);
        backend.fail_capture.store(ordinal, Ordering::SeqCst);
        backend
    }

    pub(super) fn after_apply(inner: TreeBackend, ordinal: usize) -> Self {
        let backend = Self::new(inner);
        backend.fail_apply.store(ordinal, Ordering::SeqCst);
        backend
    }

    pub(super) fn after_restore(inner: TreeBackend, ordinal: usize) -> Self {
        let backend = Self::new(inner);
        backend.fail_restore.store(ordinal, Ordering::SeqCst);
        backend
    }

    fn trips(counter: &AtomicUsize) -> bool {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                Some(value.saturating_sub(1))
            })
            .is_ok_and(|previous| previous == 1)
    }
}

impl MaterializationBackend for MutationCrashBackend {
    type Error = TreeBackendError;
    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        if Self::trips(&self.fail_capture) {
            return Err(TreeBackendError);
        }
        self.inner.capture(edit).await
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        self.inner.apply(edit, preimage).await?;
        if Self::trips(&self.fail_apply) {
            return Err(TreeBackendError);
        }
        Ok(())
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        self.inner.restore(edit, preimage).await?;
        if Self::trips(&self.fail_restore) {
            return Err(TreeBackendError);
        }
        Ok(())
    }
}
