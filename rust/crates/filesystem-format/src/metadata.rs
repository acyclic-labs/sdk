//! Canonical cross-profile file metadata values.

use crate::object_reference::ObjectId;

/// A metadata fact that is either unavailable in the source profile or exact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataField<T> {
    /// Source filesystem/profile cannot represent or did not observe the fact.
    Unavailable,
    /// Exact observed value, including zero.
    Value(T),
}

/// Complete profile-independent metadata record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileMetadata {
    /// POSIX permission/type bits when represented by the source.
    pub posix_mode: MetadataField<u32>,
    /// POSIX numeric owner identity.
    pub posix_uid: MetadataField<u32>,
    /// POSIX numeric group identity.
    pub posix_gid: MetadataField<u32>,
    /// POSIX inode flags where available.
    pub posix_flags: MetadataField<u64>,
    /// Windows file-attribute bitset.
    pub windows_attributes: MetadataField<u32>,
    /// Creation/birth time in signed Unix-epoch nanoseconds.
    pub created_ns: MetadataField<i64>,
    /// Last content-modification time in signed Unix-epoch nanoseconds.
    pub modified_ns: MetadataField<i64>,
    /// Last access time in signed Unix-epoch nanoseconds.
    pub accessed_ns: MetadataField<i64>,
    /// Last metadata-change time in signed Unix-epoch nanoseconds.
    pub changed_ns: MetadataField<i64>,
    /// Named xattrs, alternate streams, and resource forks.
    pub named_attributes: MetadataField<ObjectId>,
    /// Canonical ACL bytes when represented independently of a security descriptor.
    pub acl: MetadataField<ObjectId>,
    /// Canonical Windows security descriptor bytes.
    pub security_descriptor: MetadataField<ObjectId>,
}

impl Default for FileMetadata {
    fn default() -> Self {
        Self {
            posix_mode: MetadataField::Unavailable,
            posix_uid: MetadataField::Unavailable,
            posix_gid: MetadataField::Unavailable,
            posix_flags: MetadataField::Unavailable,
            windows_attributes: MetadataField::Unavailable,
            created_ns: MetadataField::Unavailable,
            modified_ns: MetadataField::Unavailable,
            accessed_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            named_attributes: MetadataField::Unavailable,
            acl: MetadataField::Unavailable,
            security_descriptor: MetadataField::Unavailable,
        }
    }
}

impl FileMetadata {
    /// Returns the metadata without any timestamp facts.
    #[must_use]
    pub const fn without_timestamps(self) -> Self {
        Self {
            created_ns: MetadataField::Unavailable,
            modified_ns: MetadataField::Unavailable,
            accessed_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            ..self
        }
    }

    /// Drops the stamps the host derives from other events rather than records as facts: reads advance the access time and every inode change, including to facts capture does not represent, advances the status-change time.
    #[cfg(not(target_arch = "wasm32"))]
    pub const fn without_derived_stamps(self) -> Self {
        Self {
            accessed_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            ..self
        }
    }

    /// Whether this record differs from `prior` only by equal or advanced derived stamps.
    ///
    /// Reads and unrepresented inode changes only ever advance the access and status-change
    /// times; an explicit set can move a stamp backwards, and that is a recorded fact.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn only_advanced_derived_stamps(&self, prior: &Self) -> bool {
        const fn advanced(now: MetadataField<i64>, before: MetadataField<i64>) -> bool {
            match (now, before) {
                (MetadataField::Value(now), MetadataField::Value(before)) => now >= before,
                (MetadataField::Unavailable, MetadataField::Unavailable) => true,
                _ => false,
            }
        }
        self.without_derived_stamps() == prior.without_derived_stamps()
            && advanced(self.accessed_ns, prior.accessed_ns)
            && advanced(self.changed_ns, prior.changed_ns)
    }

    /// Records a content change made at `at_ns`, in signed Unix-epoch nanoseconds: every represented modification and status-change time takes that instant.
    pub fn stamp_content_change(&mut self, at_ns: i64) -> bool {
        let mut stamped = false;
        for time in [&mut self.modified_ns, &mut self.changed_ns] {
            if let MetadataField::Value(time) = time {
                *time = at_ns;
                stamped = true;
            }
        }
        stamped
    }
}
