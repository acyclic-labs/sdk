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
    key_scope: Box<[u8]>,
    actor_scope: [u8; 16],
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
        // Store-generated retry keys and actor registries are scoped by the
        // root, so separate placements on one provider never share a key or
        // a cached head. The default placement keeps unscoped keys.
        let (key_scope, actor_scope) = if root == Self::DEFAULT_ROOT {
            (Box::default(), [0; 16])
        } else {
            let mut scope = b"placement\0".to_vec();
            scope.extend_from_slice(&(root.len() as u64).to_le_bytes());
            scope.extend_from_slice(root.as_bytes());
            let mut actor = [0; 16];
            actor.copy_from_slice(&blake3::hash(&scope).as_bytes()[..16]);
            (scope.into_boxed_slice(), actor)
        };
        Ok(Self {
            authorities: format!("{root}/authorities/")
                .into_bytes()
                .into_boxed_slice(),
            operations: format!("{root}/operations/")
                .into_bytes()
                .into_boxed_slice(),
            parent,
            key_scope,
            actor_scope,
        })
    }

    /// Prefix of every retry key this placement's store generates.
    pub(super) fn key_scope(&self) -> &[u8] {
        &self.key_scope
    }

    /// Registry scope of this placement's authority actors.
    pub(super) fn actor_scope(&self) -> [u8; 16] {
        self.actor_scope
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
