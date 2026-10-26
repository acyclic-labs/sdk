use super::*;

/// Caller-chosen Streams placement for filesystem authority streams.
///
/// Authorities live at `<root>/authorities/<authority-hex><suffix>` and their
/// operation index at `<root>/operations/<authority-hex><operation-hex>`. A
/// composed commit can therefore keep volume authorities in the caller's own
/// namespace consistency domain, outside any subtree it forks in that commit.
#[derive(Debug, Eq, PartialEq)]
pub struct AuthorityPlacement {
    authorities: Box<[u8]>,
    operations: Box<[u8]>,
    parent: StreamPath,
}

impl AuthorityPlacement {
    /// The default placement under `fs/`.
    pub const DEFAULT_ROOT: &'static str = "fs";

    /// Places authority streams below one canonical Streams path.
    pub fn new(root: &str) -> Result<Self, AuthorityStoreError> {
        let parent = StreamPath::new(format!("{root}/authorities")).map_err(map_stream_error)?;
        if parent.as_str() != format!("{root}/authorities") {
            return Err(AuthorityStoreError::RejectedStatic(
                "authority placement root is not canonical",
            ));
        }
        StreamPath::new(format!("{root}/operations")).map_err(map_stream_error)?;
        Ok(Self {
            authorities: format!("{root}/authorities/")
                .into_bytes()
                .into_boxed_slice(),
            operations: format!("{root}/operations/")
                .into_bytes()
                .into_boxed_slice(),
            parent,
        })
    }

    /// The parent Stream that lists every authority in this placement.
    #[must_use]
    pub fn authorities_parent(&self) -> &StreamPath {
        &self.parent
    }

    pub(super) fn authorities_prefix(&self) -> &[u8] {
        &self.authorities
    }

    pub(super) fn operations_prefix(&self) -> &[u8] {
        &self.operations
    }

    /// Exact Stream path of one authority stream in this placement.
    pub fn stream_path(
        &self,
        authority: AuthorityId,
        suffix: &str,
    ) -> Result<StreamPath, AuthorityStoreError> {
        let prefix = std::str::from_utf8(&self.authorities)
            .map_err(|_| AuthorityStoreError::RejectedStatic("authority placement is not UTF-8"))?;
        StreamPath::new(format!(
            "{prefix}{}{suffix}",
            hex::encode(authority.into_bytes())
        ))
        .map_err(map_stream_error)
    }

    pub(super) fn authority_child(
        &self,
        child: &StreamPath,
    ) -> Result<AuthorityId, AuthorityStoreError> {
        let encoded = child
            .as_str()
            .as_bytes()
            .strip_prefix(&*self.authorities)
            .ok_or_else(|| {
                AuthorityStoreError::Corrupt("Stream authority child escaped its parent".to_owned())
            })?;
        if encoded.len() != 32 || encoded.contains(&b'/') {
            return Err(AuthorityStoreError::Corrupt(
                "Stream authority child has a noncanonical identity".to_owned(),
            ));
        }
        let mut bytes = [0_u8; 16];
        hex::decode_to_slice(encoded, &mut bytes).map_err(|_| {
            AuthorityStoreError::Corrupt(
                "Stream authority child has a noncanonical identity".to_owned(),
            )
        })?;
        Ok(AuthorityId::from_bytes(bytes))
    }
}

impl Default for AuthorityPlacement {
    fn default() -> Self {
        match Self::new(Self::DEFAULT_ROOT) {
            Ok(placement) => placement,
            Err(_) => unreachable!("the default authority placement is canonical"),
        }
    }
}
