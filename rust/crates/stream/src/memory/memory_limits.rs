/// Optional caller-selected process capacity. No product capacity is imposed by default.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
pub struct MemoryLimits {
    /// Maximum created paths, including implicit parents.
    pub paths: Option<usize>,
    /// Maximum unique retained path bytes.
    pub path_bytes: Option<usize>,
    /// Maximum unique committed records.
    pub records: Option<usize>,
    /// Maximum unique retained payload bytes.
    pub payload_bytes: Option<usize>,
    /// Maximum immutable commit envelopes.
    pub commits: Option<usize>,
    /// Maximum indefinitely retained replay identities. New identities fail
    pub idempotency_results: Option<usize>,
}
