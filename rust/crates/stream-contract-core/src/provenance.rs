/// Original source and the state inherited from it. A created resource has no
/// provenance; a fork remains the same resource type with this additional fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Provenance<Source, Inherited> {
    /// Original resource or immutable checkpoint.
    pub source: Source,
    /// Exact inherited cut or the provider's declared fork fidelity.
    pub inherited: Inherited,
}

/// Exact immutable prefix inherited by a Stream or an Actor's state stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkCut {
    /// Exclusive source prefix, unaffected by later appends.
    pub forked_at: u64,
    /// Fork publication, rather than the last inherited record's publication.
    pub commit_id: crate::CommitId,
}
