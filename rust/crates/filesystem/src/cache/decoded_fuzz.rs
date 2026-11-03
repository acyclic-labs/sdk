//! Operation histories through the public cache miss contract and real metadata codec.
use super::*;
use crate::async_storage::{DecodedCacheLookup, DecodedCacheMiss};
use crate::kernel::{CanonicalDecodeError, DecodeLimits, FileMetadata, MetadataField};

fn metadata(id: u8) -> FileMetadata {
    FileMetadata {
        posix_uid: MetadataField::Value(u32::from(id / 2)),
        ..Default::default()
    }
}

fn key(id: u8) -> DecodedCacheKey {
    DecodedCacheKey::new::<FileMetadata>(
        crate::kernel::file_metadata_id(metadata(id)).unwrap(),
        DecodeLimits {
            maximum_page_items: 2 + u32::from(id % 2),
            ..Default::default()
        },
    )
}

fn value(id: u8) -> DecodedCacheValue {
    let bytes = crate::kernel::encode_file_metadata(
        metadata(id),
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )
    .unwrap();
    let decoded = crate::kernel::decode_file_metadata(&bytes, DecodeLimits::default(), &mut |_| {
        Ok::<_, CanonicalDecodeError>(())
    })
    .unwrap();
    assert_eq!(decoded, metadata(id));
    DecodedCacheValue {
        value: Arc::new(decoded),
        logical_bytes: bytes.len() as u64,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    }
}

fn assert_value(value: &DecodedCacheValue, id: u8) {
    assert_eq!(
        value.value.downcast_ref::<FileMetadata>(),
        Some(&metadata(id)),
        "decoded cache identity"
    );
}

fn touch(lru: &mut Vec<u8>, id: u8) -> bool {
    if let Some(index) = lru.iter().position(|entry| *entry == id) {
        lru.remove(index);
        lru.push(id);
        true
    } else {
        false
    }
}

/// Checks arbitrary cache operations; returns a semantic branch mask for corpus qualification.
/// Bytes select entry/byte capacity, followed by (operation, identity) pairs.
pub fn check(input: &[u8]) -> u8 {
    check_inner::<0>(input)
}

pub(super) fn check_inner<const FAULT: u8>(input: &[u8]) -> u8 {
    let Some((&entries, rest)) = input.split_first() else {
        return 0;
    };
    let Some((&bytes, ops)) = rest.split_first() else {
        return 0;
    };
    let entries = 1 + u32::from(entries % 4);
    let length = value(0).logical_bytes;
    let bytes = 1 + u64::from(bytes) % (length * 4);
    let store = CachedObjectStore::new(
        crate::MemoryObjectStore::default(),
        ObjectCacheOptions {
            maximum_entries: entries,
            maximum_bytes: bytes,
        },
    )
    .unwrap();
    // Independent input-identity LRU; it never reads implementation keys or clocks.
    let mut lru = Vec::new();
    let mut epoch = 0u64;
    let mut pending: Option<(u8, u64, DecodedCacheMiss<'_>)> = None;
    let mut coverage = 0u8;
    for pair in ops.chunks_exact(2) {
        let id = pair[1] % 6;
        let mut completing = None;
        match pair[0] % 5 {
            op @ (0 | 1 | 3) => {
                let expected = touch(&mut lru, id);
                let lookup = if FAULT == 1 {
                    match store.decoded_get_matching::<false>(key(id)).unwrap() {
                        Some(value) => DecodedCacheLookup::Hit(value),
                        None => store.decoded_cache_lookup(key(id)).unwrap(),
                    }
                } else {
                    store.decoded_cache_lookup(key(id)).unwrap()
                };
                match lookup {
                    DecodedCacheLookup::Hit(value) => {
                        coverage |= 1;
                        assert!(expected, "decoded cache unexpected hit");
                        assert_value(&value, id);
                    }
                    DecodedCacheLookup::Miss(miss) => {
                        coverage |= 2;
                        assert!(!expected, "decoded cache lost resident identity");
                        if op == 0 {
                            completing = Some((id, epoch, miss));
                        } else if op == 3 {
                            coverage |= 4;
                            pending = Some((id, epoch, miss));
                        }
                    }
                }
            }
            2 => {
                store.clear().unwrap();
                lru.clear();
                epoch += 1;
                coverage |= 8;
            }
            _ => {
                completing = pending.take();
            }
        }
        if let Some((id, issued, miss)) = completing {
            let expected = issued == epoch && length <= bytes;
            let admission = if FAULT == 2 {
                store
                    .decoded_admit_bounded::<false>(key(id), store.shared.fill_epoch(), value(id))
                    .unwrap()
            } else if FAULT == 3 {
                store
                    .decoded_admit_bounded::<true>(key(id), store.shared.fill_epoch(), value(id))
                    .unwrap()
            } else {
                miss.admit(value(id)).unwrap()
            };
            let actual = match admission {
                DecodedCacheAdmission::Shared(value) => {
                    assert_value(&value, id);
                    true
                }
                DecodedCacheAdmission::Uncached(value) => {
                    assert_value(&value, id);
                    false
                }
            };
            assert_eq!(actual, expected, "decoded cache original miss disposition");
            if issued != epoch {
                coverage |= 16;
            } else if length > bytes {
                coverage |= 32;
            } else if lru.contains(&id) {
                coverage |= 64;
            } else {
                while lru.len() >= entries as usize || (lru.len() as u64 + 1) * length > bytes {
                    coverage |= 128;
                    lru.remove(0);
                }
                lru.push(id);
            }
        }
        let stats = store.stats().unwrap();
        assert_eq!(
            (stats.resident_entries, stats.resident_bytes),
            (lru.len() as u64, lru.len() as u64 * length),
            "decoded cache residency"
        );
    }
    coverage
}
