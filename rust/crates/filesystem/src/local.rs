//! Crash-safe local implementations that require no external infrastructure.

use crate::foundation::Digest;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure, ObjectId, ObjectRead, ObjectReadRetention,
    ObjectReceipt, ObjectResult, ObjectStore, ObjectStoreError, object_digest,
};
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use uuid::Uuid;

/// Digest-addressed local object store with atomic no-replace visibility.
pub struct LocalObjectStore {
    root: PathBuf,
    quarantine: PathBuf,
    maximum_object_bytes: u64,
    mutation: Mutex<()>,
    _maintenance_lock: File,
    maintenance_exclusive: bool,
}

/// Exact outcome of one bounded local orphan collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalGarbageCollection {
    /// Canonical object files examined.
    pub examined: u64,
    /// Unreachable objects durably removed.
    pub removed: u64,
    /// Crash-left publication temporaries durably removed.
    pub temporary_files_removed: u64,
}

impl LocalObjectStore {
    /// Opens or creates a bounded object store below `root`.
    ///
    /// # Errors
    ///
    /// Fails when the bound is zero or required directories cannot be made durable.
    pub fn open(
        root: impl AsRef<Path>,
        maximum_object_bytes: u64,
    ) -> Result<Self, ObjectStoreError> {
        Self::open_with_maintenance_mode(root.as_ref(), maximum_object_bytes, false)
    }

    /// Opens the store while excluding every normal local process instance.
    ///
    /// This is reserved for authority-rooted maintenance. The exclusive lock
    /// proves no writer is between durable object staging and authority
    /// publication while unreachable objects are removed.
    pub(crate) fn open_for_maintenance(
        root: impl AsRef<Path>,
        maximum_object_bytes: u64,
    ) -> Result<Self, ObjectStoreError> {
        Self::open_with_maintenance_mode(root.as_ref(), maximum_object_bytes, true)
    }

    fn open_with_maintenance_mode(
        root: &Path,
        maximum_object_bytes: u64,
        exclusive: bool,
    ) -> Result<Self, ObjectStoreError> {
        if maximum_object_bytes == 0 {
            return Err(ObjectStoreError::TooLarge {
                observed: 0,
                maximum: 0,
            });
        }
        let root = root.to_path_buf();
        create_directories_durable(&root, &root.join("objects"))?;
        let quarantine = root.join("quarantine");
        create_directories_durable(&root, &quarantine)?;
        let maintenance_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("maintenance.lock"))?;
        if exclusive {
            fs2::FileExt::try_lock_exclusive(&maintenance_lock)?;
        } else {
            fs2::FileExt::lock_shared(&maintenance_lock)?;
        }
        Ok(Self {
            root,
            quarantine,
            maximum_object_bytes,
            mutation: Mutex::new(()),
            _maintenance_lock: maintenance_lock,
            maintenance_exclusive: exclusive,
        })
    }

    /// Removes only objects absent from the caller's authenticated live set.
    ///
    /// The caller owns closure traversal from authority refs; this local method
    /// owns only bounded physical reclamation.
    ///
    /// # Errors
    ///
    /// Rejects a zero candidate bound, excessive enumeration, malformed store
    /// paths, synchronization failure, or insufficient work budget.
    #[allow(clippy::too_many_lines)]
    pub fn collect_garbage(
        &self,
        reachable: &[ObjectId],
        maximum_candidates: u64,
        budget: WorkBudget,
    ) -> ObjectResult<LocalGarbageCollection> {
        if !self.maintenance_exclusive {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "garbage collection requires exclusive local maintenance".to_owned(),
            )));
        }
        if maximum_candidates == 0 {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "garbage-collection candidate bound must be positive".to_owned(),
            )));
        }
        let _mutation = self.mutation.lock().map_err(|_| ObjectFailure {
            error: ObjectStoreError::Corrupt,
            work: Box::new(WorkCounters::default()),
        })?;
        let mut work = WorkCounters::default();
        let mut report = LocalGarbageCollection {
            examined: 0,
            removed: 0,
            temporary_files_removed: 0,
        };
        let objects = self.root.join("objects");
        for kind_entry in read_directory(&objects, &mut work, budget)? {
            let kind_entry = kind_entry.map_err(|error| ObjectFailure {
                error: error.into(),
                work: Box::new(work),
            })?;
            if !entry_is_directory(&kind_entry, &mut work, budget)? {
                continue;
            }
            let kind_name = kind_entry.file_name();
            let kind_text = kind_name.to_str().ok_or_else(|| ObjectFailure {
                error: ObjectStoreError::Corrupt,
                work: Box::new(work),
            })?;
            let kind_tag = u8::from_str_radix(kind_text, 16).map_err(|_| ObjectFailure {
                error: ObjectStoreError::Corrupt,
                work: Box::new(work),
            })?;
            let kind = crate::storage::ObjectKind::from_canonical_tag(kind_tag).map_err(|_| {
                ObjectFailure {
                    error: ObjectStoreError::Corrupt,
                    work: Box::new(work),
                }
            })?;
            for prefix_entry in read_directory(&kind_entry.path(), &mut work, budget)? {
                let prefix_entry = prefix_entry.map_err(|error| ObjectFailure {
                    error: error.into(),
                    work: Box::new(work),
                })?;
                if !entry_is_directory(&prefix_entry, &mut work, budget)? {
                    continue;
                }
                let prefix_name = prefix_entry.file_name();
                let prefix = prefix_name.to_str().ok_or_else(|| ObjectFailure {
                    error: ObjectStoreError::Corrupt,
                    work: Box::new(work),
                })?;
                let prefix_path = prefix_entry.path();
                for object_entry in read_directory(&prefix_path, &mut work, budget)? {
                    let object_entry = object_entry.map_err(|error| ObjectFailure {
                        error: error.into(),
                        work: Box::new(work),
                    })?;
                    let path = object_entry.path();
                    if !entry_is_file(&object_entry, &mut work, budget)? {
                        continue;
                    }
                    let object_name = object_entry.file_name();
                    let name = object_name.to_str().ok_or_else(|| ObjectFailure {
                        error: ObjectStoreError::Corrupt,
                        work: Box::new(work),
                    })?;
                    report.examined =
                        report
                            .examined
                            .checked_add(1)
                            .ok_or_else(|| ObjectFailure {
                                error: ObjectStoreError::Rejected(
                                    "garbage-collection count overflowed".to_owned(),
                                ),
                                work: Box::new(work),
                            })?;
                    if report.examined > maximum_candidates {
                        return Err(ObjectFailure {
                            error: ObjectStoreError::Rejected(
                                "garbage-collection candidate bound exceeded".to_owned(),
                            ),
                            work: Box::new(work),
                        });
                    }
                    work = work
                        .checked_add(WorkCounters {
                            object_probes: 1,
                            items_examined: 1,
                            ..WorkCounters::default()
                        })
                        .map_err(|error| ObjectFailure {
                            error: error.into(),
                            work: Box::new(work),
                        })?;
                    work.verify(budget).map_err(|error| ObjectFailure {
                        error: error.into(),
                        work: Box::new(work),
                    })?;
                    if let Some(temporary_identity) = name
                        .strip_prefix('.')
                        .and_then(|value| value.strip_suffix(".tmp"))
                    {
                        let parsed =
                            Uuid::parse_str(temporary_identity).map_err(|_| ObjectFailure {
                                error: ObjectStoreError::Corrupt,
                                work: Box::new(work),
                            })?;
                        if parsed.hyphenated().to_string() != temporary_identity {
                            return Err(ObjectFailure {
                                error: ObjectStoreError::Corrupt,
                                work: Box::new(work),
                            });
                        }
                        work = remove_garbage_file(&path, &prefix_path, work, budget)?;
                        report.temporary_files_removed = report
                            .temporary_files_removed
                            .checked_add(1)
                            .ok_or_else(|| ObjectFailure {
                                error: ObjectStoreError::Rejected(
                                    "garbage-collection temporary count overflowed".to_owned(),
                                ),
                                work: Box::new(work),
                            })?;
                        continue;
                    }
                    let Some(suffix) = name.strip_suffix(".object") else {
                        return Err(ObjectFailure {
                            error: ObjectStoreError::Corrupt,
                            work: Box::new(work),
                        });
                    };
                    let object_id = parse_object_id(kind, prefix, suffix, work)?;
                    if reachable.binary_search(&object_id).is_ok() {
                        continue;
                    }
                    work = remove_garbage_file(&path, &prefix_path, work, budget)?;
                    report.removed = report.removed.saturating_add(1);
                }
            }
        }
        Ok(ObjectReceipt {
            value: report,
            work,
        })
    }

    fn object_path(&self, object_id: ObjectId) -> PathBuf {
        let digest = hex::encode(object_id.digest.into_bytes());
        self.root
            .join("objects")
            .join(format!("{:02x}", object_id.kind.canonical_tag()))
            .join(&digest[..2])
            .join(format!("{}.object", &digest[2..]))
    }

    fn read_verified(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
    ) -> ObjectResult<ObjectRead> {
        let path = self.object_path(object_id);
        let work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(error) => {
                return Err(ObjectFailure {
                    error: if has_io_kind(&error, std::io::ErrorKind::NotFound) {
                        ObjectStoreError::Missing
                    } else {
                        error.into()
                    },
                    work: Box::new(work),
                });
            }
        };
        let length = file
            .metadata()
            .map_err(|error| ObjectFailure {
                error: error.into(),
                work: Box::new(work),
            })?
            .len();
        let effective_maximum = maximum_bytes.min(self.maximum_object_bytes);
        if length > effective_maximum {
            return Err(ObjectFailure {
                error: ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: effective_maximum,
                },
                work: Box::new(work),
            });
        }
        let admitted = work
            .checked_add(WorkCounters {
                object_bytes_read: length,
                bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                allocation_operations: u64::from(length != 0),
                peak_allocation_bytes: length,
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure {
                error: error.into(),
                work: Box::new(work),
            })?;
        admitted.verify(budget).map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(work),
        })?;

        let result = read_body_verified(&mut file, object_id, length, work);
        drop(file);
        if matches!(
            result,
            Err(ObjectFailure {
                error: ObjectStoreError::Corrupt,
                ..
            })
        ) {
            self.quarantine_corrupt(&path, object_id)
                .map_err(|error| ObjectFailure {
                    error,
                    work: Box::new(work),
                })?;
        }
        result
    }

    fn quarantine_corrupt(
        &self,
        source: &Path,
        object_id: ObjectId,
    ) -> Result<(), ObjectStoreError> {
        let destination = self.quarantine.join(format!(
            "{:02x}-{}-{}.object",
            object_id.kind.canonical_tag(),
            hex::encode(object_id.digest.into_bytes()),
            Uuid::now_v7()
        ));
        match fs::rename(source, &destination) {
            Ok(()) => {
                let source_parent = source.parent().ok_or_else(|| {
                    ObjectStoreError::Io(std::io::Error::other("corrupt object path has no parent"))
                })?;
                sync_publication(source_parent, source_parent)?;
                sync_publication(&self.quarantine, &destination)
            }
            Err(error) => {
                if has_io_kind(&error, std::io::ErrorKind::NotFound) {
                    Ok(())
                } else {
                    Err(ObjectStoreError::QuarantineFailed(error))
                }
            }
        }
    }

    fn publish_absent(
        &self,
        object_id: ObjectId,
        bytes: &Bytes,
        hash_work: WorkCounters,
        budget: WorkBudget,
    ) -> ObjectResult<()> {
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let destination = self.object_path(object_id);
        let write_work = hash_work
            .checked_add(WorkCounters {
                object_probes: 1,
                backend_write_operations: 1,
                object_bytes_written: length,
                durability_operations: 2,
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure {
                error: error.into(),
                work: Box::new(hash_work),
            })?;
        write_work.verify(budget).map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(hash_work),
        })?;
        let parent = destination.parent().ok_or_else(|| ObjectFailure {
            error: ObjectStoreError::Io(std::io::Error::other("object path has no parent")),
            work: Box::new(hash_work),
        })?;
        create_directories_durable(&self.root, parent).map_err(|error| ObjectFailure {
            error,
            work: Box::new(hash_work),
        })?;
        let temporary = parent.join(format!(".{}.tmp", Uuid::now_v7()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| ObjectFailure {
                error: error.into(),
                work: Box::new(hash_work),
            })?;
        if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            let _ignored = fs::remove_file(&temporary);
            return Err(ObjectFailure {
                error: error.into(),
                work: Box::new(write_work),
            });
        }
        drop(file);
        self.link_temporary(
            &temporary,
            &destination,
            parent,
            object_id,
            length,
            hash_work,
            write_work,
            budget,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn link_temporary(
        &self,
        temporary: &Path,
        destination: &Path,
        parent: &Path,
        object_id: ObjectId,
        length: u64,
        hash_work: WorkCounters,
        write_work: WorkCounters,
        budget: WorkBudget,
    ) -> ObjectResult<()> {
        match classify_entry_creation(fs::hard_link(temporary, destination)) {
            Ok(EntryCreation::Created) => {
                sync_publication(parent, destination).map_err(|error| ObjectFailure {
                    error,
                    work: Box::new(write_work),
                })?;
            }
            Ok(EntryCreation::Existing) => {
                let _ignored = fs::remove_file(temporary);
                let remaining =
                    subtract_budget(budget, hash_work).map_err(|error| ObjectFailure {
                        error: error.into(),
                        work: Box::new(hash_work),
                    })?;
                return merge_existing_receipt(
                    hash_work,
                    self.read_verified(object_id, length, remaining),
                );
            }
            Err(error) => {
                let _ignored = fs::remove_file(temporary);
                return Err(ObjectFailure {
                    error: error.into(),
                    work: Box::new(write_work),
                });
            }
        }
        fs::remove_file(temporary).map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(write_work),
        })?;
        Ok(ObjectReceipt {
            value: (),
            work: write_work,
        })
    }
}

fn remove_garbage_file(
    path: &Path,
    parent: &Path,
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkCounters, ObjectFailure> {
    let next = work
        .checked_add(WorkCounters {
            backend_write_operations: 1,
            durability_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(work),
        })?;
    next.verify(budget).map_err(|error| ObjectFailure {
        error: error.into(),
        work: Box::new(work),
    })?;
    fs::remove_file(path).map_err(|error| ObjectFailure {
        error: error.into(),
        work: Box::new(next),
    })?;
    sync_publication(parent, parent).map_err(|error| ObjectFailure {
        error,
        work: Box::new(next),
    })?;
    Ok(next)
}

fn has_io_kind(error: &std::io::Error, expected: std::io::ErrorKind) -> bool {
    error.kind() == expected
}

impl ObjectStore for LocalObjectStore {
    fn put(&self, object_id: ObjectId, bytes: Bytes, budget: WorkBudget) -> ObjectResult<()> {
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if length > self.maximum_object_bytes {
            return Err(ObjectFailure::before_work(ObjectStoreError::TooLarge {
                observed: length,
                maximum: self.maximum_object_bytes,
            }));
        }
        let hash_work = WorkCounters {
            bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
            ..WorkCounters::default()
        };
        hash_work
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(ObjectFailure {
                error: ObjectStoreError::DigestMismatch,
                work: Box::new(hash_work),
            });
        }
        let _guard = self.mutation.lock().map_err(|_| ObjectFailure {
            error: ObjectStoreError::Corrupt,
            work: Box::new(hash_work),
        })?;
        let destination = self.object_path(object_id);
        if destination.exists() {
            let remaining = subtract_budget(budget, hash_work).map_err(|error| ObjectFailure {
                error: error.into(),
                work: Box::new(hash_work),
            })?;
            let existing =
                merge_existing_receipt(hash_work, self.read_verified(object_id, length, remaining));
            return existing;
        }
        self.publish_absent(object_id, &bytes, hash_work, budget)
    }

    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
    ) -> ObjectResult<ObjectRead> {
        self.read_verified(object_id, maximum_bytes, budget)
    }

    fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        budget: WorkBudget,
    ) -> ObjectResult<Vec<ObjectRead>> {
        crate::storage::read_many_sequential(self, requests, budget)
    }

    fn contains(&self, object_id: ObjectId, budget: WorkBudget) -> ObjectResult<bool> {
        match self.read_verified(object_id, self.maximum_object_bytes, budget) {
            Ok(receipt) => Ok(ObjectReceipt {
                value: true,
                work: receipt.work,
            }),
            Err(ObjectFailure {
                error: ObjectStoreError::Missing,
                work,
            }) => Ok(ObjectReceipt {
                value: false,
                work: *work,
            }),
            Err(failure) => Err(failure),
        }
    }
}

fn read_directory(
    path: &Path,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<fs::ReadDir, ObjectFailure> {
    let next = work
        .checked_add(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(*work),
        })?;
    next.verify(budget).map_err(|error| ObjectFailure {
        error: error.into(),
        work: Box::new(*work),
    })?;
    let directory = fs::read_dir(path).map_err(|error| ObjectFailure {
        error: error.into(),
        work: Box::new(next),
    })?;
    *work = next;
    Ok(directory)
}

fn entry_is_directory(
    entry: &fs::DirEntry,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<bool, ObjectFailure> {
    entry_type(entry, work, budget).map(|file_type| file_type.is_dir())
}

fn entry_is_file(
    entry: &fs::DirEntry,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<bool, ObjectFailure> {
    entry_type(entry, work, budget).map(|file_type| file_type.is_file())
}

fn entry_type(
    entry: &fs::DirEntry,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<fs::FileType, ObjectFailure> {
    let next = work
        .checked_add(WorkCounters {
            backend_read_operations: 1,
            items_examined: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(*work),
        })?;
    next.verify(budget).map_err(|error| ObjectFailure {
        error: error.into(),
        work: Box::new(*work),
    })?;
    let file_type = entry.file_type().map_err(|error| ObjectFailure {
        error: error.into(),
        work: Box::new(next),
    })?;
    *work = next;
    Ok(file_type)
}

fn parse_object_id(
    kind: crate::storage::ObjectKind,
    prefix: &str,
    suffix: &str,
    work: WorkCounters,
) -> Result<ObjectId, ObjectFailure> {
    if prefix.len() != 2 || suffix.len() != 62 {
        return Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            work: Box::new(work),
        });
    }
    let mut encoded = [0_u8; 64];
    encoded[..2].copy_from_slice(prefix.as_bytes());
    encoded[2..].copy_from_slice(suffix.as_bytes());
    let mut bytes = [0_u8; 32];
    hex::decode_to_slice(encoded, &mut bytes).map_err(|_| ObjectFailure {
        error: ObjectStoreError::Corrupt,
        work: Box::new(work),
    })?;
    Ok(ObjectId {
        kind,
        digest: Digest::from_bytes(bytes),
    })
}

fn read_body_verified(
    file: &mut File,
    object_id: ObjectId,
    length: u64,
    mut work: WorkCounters,
) -> ObjectResult<ObjectRead> {
    let allocation = usize::try_from(length).map_err(|_| ObjectFailure {
        error: ObjectStoreError::TooLarge {
            observed: length,
            maximum: length,
        },
        work: Box::new(work),
    })?;
    let mut output = vec![0_u8; allocation];
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-object-v1\0");
    hasher.update(&[object_id.kind.canonical_tag()]);
    hasher.update(&length.to_le_bytes());
    work.peak_allocation_bytes = length;
    work.allocation_operations = u64::from(length != 0);
    work.bytes_hashed = OBJECT_DIGEST_ENVELOPE_BYTES;
    let mut offset = 0_usize;
    while offset < output.len() {
        match file.read(&mut output[offset..]) {
            Ok(0) => {
                return Err(ObjectFailure {
                    error: ObjectStoreError::Corrupt,
                    work: Box::new(work),
                });
            }
            Ok(count) => {
                hasher.update(&output[offset..offset + count]);
                let count_u64 = u64::try_from(count).unwrap_or(u64::MAX);
                work.object_bytes_read = work.object_bytes_read.saturating_add(count_u64);
                work.bytes_hashed = work.bytes_hashed.saturating_add(count_u64);
                offset = offset.saturating_add(count);
            }
            Err(error) => {
                return Err(ObjectFailure {
                    error: error.into(),
                    work: Box::new(work),
                });
            }
        }
    }
    if Digest::from_bytes(*hasher.finalize().as_bytes()) != object_id.digest {
        return Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            work: Box::new(work),
        });
    }
    Ok(ObjectReceipt {
        value: ObjectRead {
            bytes: Bytes::from(output),
            retention: ObjectReadRetention::Owned {
                logical_bytes: length,
            },
        },
        work,
    })
}

fn merge_existing_receipt(
    hash_work: WorkCounters,
    existing: ObjectResult<ObjectRead>,
) -> ObjectResult<()> {
    let receipt = existing?;
    let work = hash_work
        .checked_add(receipt.work)
        .map_err(|error| ObjectFailure {
            error: error.into(),
            work: Box::new(hash_work),
        })?;
    Ok(ObjectReceipt { value: (), work })
}

fn subtract_budget(
    budget: WorkBudget,
    spent: WorkCounters,
) -> Result<WorkBudget, crate::performance::WorkError> {
    spent.verify(budget)?;
    Ok(WorkBudget {
        authority_records_read: remaining(
            budget.authority_records_read,
            spent.authority_records_read,
        )?,
        authority_records_appended: remaining(
            budget.authority_records_appended,
            spent.authority_records_appended,
        )?,
        authority_bytes_read: remaining(budget.authority_bytes_read, spent.authority_bytes_read)?,
        authority_bytes_written: remaining(
            budget.authority_bytes_written,
            spent.authority_bytes_written,
        )?,
        object_probes: remaining(budget.object_probes, spent.object_probes)?,
        backend_read_operations: remaining(
            budget.backend_read_operations,
            spent.backend_read_operations,
        )?,
        backend_write_operations: remaining(
            budget.backend_write_operations,
            spent.backend_write_operations,
        )?,
        durability_operations: remaining(
            budget.durability_operations,
            spent.durability_operations,
        )?,
        page_reads: remaining(budget.page_reads, spent.page_reads)?,
        page_writes: remaining(budget.page_writes, spent.page_writes)?,
        object_bytes_read: remaining(budget.object_bytes_read, spent.object_bytes_read)?,
        object_bytes_written: remaining(budget.object_bytes_written, spent.object_bytes_written)?,
        bytes_hashed: remaining(budget.bytes_hashed, spent.bytes_hashed)?,
        bytes_copied: remaining(budget.bytes_copied, spent.bytes_copied)?,
        bytes_encoded: remaining(budget.bytes_encoded, spent.bytes_encoded)?,
        source_bytes_read: remaining(budget.source_bytes_read, spent.source_bytes_read)?,
        output_bytes: remaining(budget.output_bytes, spent.output_bytes)?,
        items_examined: remaining(budget.items_examined, spent.items_examined)?,
        items_returned: remaining(budget.items_returned, spent.items_returned)?,
        allocation_operations: remaining(
            budget.allocation_operations,
            spent.allocation_operations,
        )?,
        peak_allocation_bytes: budget.peak_allocation_bytes,
        materializations: remaining(budget.materializations, spent.materializations)?,
    })
}

fn remaining(admitted: u64, spent: u64) -> Result<u64, crate::performance::WorkError> {
    admitted
        .checked_sub(spent)
        .ok_or(crate::performance::WorkError::Overflow)
}

pub(crate) fn create_directories_durable(
    root: &Path,
    target: &Path,
) -> Result<(), ObjectStoreError> {
    let mut missing = Vec::new();
    let mut ancestor = root;
    while !ancestor.exists() {
        missing.push(ancestor.to_path_buf());
        ancestor = ancestor.parent().ok_or_else(|| {
            ObjectStoreError::Io(std::io::Error::other(
                "local store root has no existing ancestor",
            ))
        })?;
    }
    for directory in missing.iter().rev() {
        let parent = directory.parent().ok_or_else(|| {
            ObjectStoreError::Io(std::io::Error::other("local store directory has no parent"))
        })?;
        match classify_entry_creation(fs::create_dir(directory)) {
            Ok(EntryCreation::Created) => sync_publication(parent, directory)?,
            Ok(EntryCreation::Existing) => {}
            Err(error) => return Err(error.into()),
        }
    }
    let mut current = root.to_path_buf();
    let relative = target.strip_prefix(root).map_err(|_| {
        ObjectStoreError::Io(std::io::Error::other("object path escaped store root"))
    })?;
    for component in relative.components() {
        let parent = current.clone();
        current.push(component);
        match classify_entry_creation(fs::create_dir(&current)) {
            Ok(EntryCreation::Created) => sync_publication(&parent, &current)?,
            Ok(EntryCreation::Existing) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryCreation {
    Created,
    Existing,
}

fn classify_entry_creation(result: std::io::Result<()>) -> std::io::Result<EntryCreation> {
    match result {
        Ok(()) => Ok(EntryCreation::Created),
        Err(error) if has_io_kind(&error, std::io::ErrorKind::AlreadyExists) => {
            Ok(EntryCreation::Existing)
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn sync_publication(parent: &Path, _entry: &Path) -> Result<(), ObjectStoreError> {
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_publication(parent: &Path, entry: &Path) -> Result<(), ObjectStoreError> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    if entry.is_file() {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(entry)?
            .sync_all()?;
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(parent)?
        .sync_all()?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_publication(_parent: &Path, _entry: &Path) -> Result<(), ObjectStoreError> {
    Err(ObjectStoreError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "durable local publication is unsupported on this platform",
    )))
}

#[cfg(test)]
#[path = "tests/local.rs"]
mod tests;
