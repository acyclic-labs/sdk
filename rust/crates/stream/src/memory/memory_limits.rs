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
    /// Optional maximum indefinitely retained replay identities. With `Some`,
    /// admission of a new identity fails when the selected capacity is full;
    /// existing identities still replay exactly. `None` retains all identities
    /// without a count policy. No replay identity is evicted to admit new work.
    pub idempotency_results: Option<usize>,
}
