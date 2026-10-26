use super::*;

impl LocalCoreStateStore {
    /// Every file record a stored lazy shadow holds, which an object collection keeps: a lazy workspace may still resolve any of them.
    pub(crate) async fn lazy_shadow_records(
        &self,
    ) -> Result<Vec<crate::kernel::FileRecord>, LocalCoreStateStoreError> {
        self.transaction(|namespace| {
            let namespace = &namespace;
            let family = <crate::LazyShadow as LazyNode>::FAMILY;
            let addresses = family_stems(namespace, family, &[".previous.json", ".json"])?
                .into_iter()
                .filter_map(|stem| <[u8; 32]>::try_from(stem).ok());
            let mut records = Vec::new();
            for address in addresses {
                if let Some(crate::LazyShadow::Node { record, .. }) =
                    load_content_addressed::<crate::LazyShadow>(namespace, family, address)?
                {
                    records.push(
                        crate::kernel::decode_file_record(&record)
                            .map_err(|_| LocalCoreStateStoreError::Integrity)?,
                    );
                }
            }
            Ok(records)
        })
        .await
    }
}

/// Distinct record keys with the selected file suffixes.
pub(super) fn family_stems(
    root: &Namespace,
    family: &str,
    suffixes: &[&str],
) -> Result<BTreeSet<Vec<u8>>, LocalCoreStateStoreError> {
    let entries = match std::fs::read_dir(root.family(family)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(error.into()),
    };
    let mut stems = BTreeSet::new();
    for entry in entries {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        let stem = suffixes.iter().find_map(|suffix| name.strip_suffix(suffix));
        if let Some(bytes) = stem.and_then(|stem| hex::decode(stem).ok()) {
            stems.insert(bytes);
        }
    }
    Ok(stems)
}

/// Loads an immutable record whose key is the BLAKE3 digest of its encoding.
pub(super) fn load_content_addressed<T: DeserializeOwned + Serialize>(
    namespace: &Namespace,
    family: &str,
    address: [u8; 32],
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let Some((value, _)) = read_published(&namespace.content_record(family, &address)?)? else {
        return Ok(None);
    };
    if content_address(&value)? != address {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    Ok(Some(value))
}

pub(super) fn content_address<T: Serialize>(
    value: &T,
) -> Result<[u8; 32], LocalCoreStateStoreError> {
    Ok(*blake3::hash(&serde_json::to_vec(value)?).as_bytes())
}
