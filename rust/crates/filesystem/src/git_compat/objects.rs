use super::*;

/// Stable BLAKE3 compatibility commit identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GitCommitId(pub(super) [u8; 32]);

impl GitCommitId {
    /// Restores an exact compatibility identity.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns canonical bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Lower-case hexadecimal spelling accepted by the argv façade.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl Serialize for GitCommitId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for GitCommitId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let bytes = hex::decode(&value).map_err(serde::de::Error::custom)?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            serde::de::Error::custom(format!(
                "Git compatibility commit ID has {} bytes instead of 32",
                bytes.len()
            ))
        })?;
        Ok(Self(bytes))
    }
}

/// A filtered tree's SDK-authenticated fork origin.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitCaptureProof {
    pub(super) fork_parent: GitTreeRef,
    pub(super) initial_generation: crate::GenerationId,
    pub(super) operation_id: OperationId,
}

impl GitCaptureProof {
    pub(super) fn structurally_valid(
        &self,
        tree: GitTreeRef,
        workspace_tree: GitTreeRef,
        previous_tree: Option<GitTreeRef>,
    ) -> bool {
        let GitTreeRef::Exact(tree) = tree else {
            return false;
        };
        let GitTreeRef::Exact(parent) = self.fork_parent else {
            return false;
        };
        tree.workspace_id != workspace_tree.workspace_id()
            && tree.generation != self.initial_generation
            && (self.fork_parent == workspace_tree || previous_tree == Some(self.fork_parent))
            && parent.workspace_id != tree.workspace_id
    }
}

/// One explicit Git-facing commit mapped to an SDK generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitCommit {
    /// Content-addressed compatibility identity.
    pub id: GitCommitId,
    /// Git-visible committed tree. Lazy trees retain unresolved source state.
    pub tree: GitTreeRef,
    /// Complete live tree, including paths excluded from the Git tree.
    pub workspace_tree: GitTreeRef,
    /// SDK-verified fork origin when the Git-visible tree is a filtered fork.
    pub capture_proof: Option<GitCaptureProof>,
    /// Exact portable paths represented by this compatibility snapshot.
    pub tracked_paths: BTreeSet<String>,
    /// Ordered compatibility parents.
    pub parents: Vec<GitCommitId>,
    /// Human or agent identity.
    pub author: String,
    /// Unix epoch timestamp in seconds.
    pub authored_at_seconds: i64,
    /// Commit message.
    pub message: String,
}

impl GitCommit {
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is one field of the recorded commit"
    )]
    /// Constructs and hashes an explicit compatibility commit.
    pub fn new(
        tree: GitTreeRef,
        workspace_tree: GitTreeRef,
        capture_proof: Option<GitCaptureProof>,
        tracked_paths: BTreeSet<String>,
        parents: Vec<GitCommitId>,
        author: impl Into<String>,
        authored_at_seconds: i64,
        message: impl Into<String>,
    ) -> Self {
        let author = author.into();
        let message = message.into();
        let mut hasher = blake3::Hasher::new();
        hasher.update(COMMIT_DOMAIN);
        hash_tree_ref(&mut hasher, tree);
        hash_tree_ref(&mut hasher, workspace_tree);
        if let Some(proof) = &capture_proof {
            hasher.update(&[1]);
            hash_tree_ref(&mut hasher, proof.fork_parent);
            hasher.update(proof.initial_generation.digest().as_bytes());
            hasher.update(&proof.operation_id.into_bytes());
        }
        hasher.update(
            &u64::try_from(tracked_paths.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for path in &tracked_paths {
            hash_bytes(&mut hasher, path.as_bytes());
        }
        hasher.update(
            &u64::try_from(parents.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for parent in &parents {
            hasher.update(&parent.0);
        }
        hash_bytes(&mut hasher, author.as_bytes());
        hasher.update(&authored_at_seconds.to_le_bytes());
        hash_bytes(&mut hasher, message.as_bytes());
        Self {
            id: GitCommitId(*hasher.finalize().as_bytes()),
            tree,
            workspace_tree,
            capture_proof,
            tracked_paths,
            parents,
            author,
            authored_at_seconds,
            message,
        }
    }
}

pub(super) fn hash_tree_ref(hasher: &mut blake3::Hasher, tree: GitTreeRef) {
    match tree {
        GitTreeRef::Exact(reference) => {
            hasher.update(&[0]);
            hasher.update(&reference.workspace_id.into_bytes());
            hasher.update(reference.generation.digest().as_bytes());
        }
        GitTreeRef::Lazy(reference) => {
            hasher.update(&[1]);
            hasher.update(&reference.id.into_bytes());
        }
    }
}

pub(super) fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(bytes);
}

/// Exact compatibility snapshot identity across SDK workspaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitGenerationRef {
    /// Workspace that authenticates the generation.
    pub workspace_id: WorkspaceId,
    /// Immutable generation identity.
    pub generation: GenerationId,
}

/// One Git-visible tree, either fully authored or lazily source-backed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GitTreeRef {
    /// Exact authenticated authored generation.
    Exact(GitGenerationRef),
    /// Constant-size logical snapshot retaining unresolved source semantics.
    Lazy(crate::LazySnapshotRef),
}

impl GitTreeRef {
    /// Constructs an exact authored tree reference.
    #[must_use]
    pub const fn exact(workspace_id: WorkspaceId, generation: GenerationId) -> Self {
        Self::Exact(GitGenerationRef {
            workspace_id,
            generation,
        })
    }
    /// Workspace owning the authored component.
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        match self {
            Self::Exact(reference) => reference.workspace_id,
            Self::Lazy(reference) => reference.workspace_id,
        }
    }

    /// Authenticated authored generation component.
    #[must_use]
    pub const fn authored_generation(self) -> GenerationId {
        match self {
            Self::Exact(reference) => reference.generation,
            Self::Lazy(reference) => reference.authored_generation,
        }
    }
}
