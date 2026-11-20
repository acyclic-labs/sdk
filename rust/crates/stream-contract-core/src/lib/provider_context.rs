/// Process-local ownership and durability policy of one provider session.
///
/// Forwarding adapters preserve this value only when they preserve its owner
/// and publication boundaries. A cache over remote storage is not an exclusive
/// process owner. Storage leases still govern independently opened sessions.
#[derive(Clone, Copy)]
pub struct ProviderContext<'a> {
    claims: &'a OperationClaims,
    process_exclusion: bool,
    deferred_durability: bool,
    durable_acknowledgement: bool,
}

impl<'a> ProviderContext<'a> {
    /// Binds claims without asserting process exclusion or deferred durability.
    pub const fn new(claims: &'a OperationClaims) -> Self {
        Self {
            claims,
            process_exclusion: false,
            deferred_durability: false,
            durable_acknowledgement: false,
        }
    }

    /// Declares that every mutation of this storage shares these process claims.
    #[must_use]
    pub const fn with_process_exclusion(mut self) -> Self {
        self.process_exclusion = true;
        self
    }

    /// Declares that the session's durability barrier can seal deferred writes.
    #[must_use]
    pub const fn with_deferred_durability(mut self) -> Self {
        self.deferred_durability = true;
        self
    }

    /// Declares that each write this task makes through the session is
    /// acknowledged only once the device durably holds it and every write the
    /// session accepted before it: the acknowledgement is the durable cut.
    #[must_use]
    pub const fn with_durable_acknowledgement(mut self) -> Self {
        self.durable_acknowledgement = true;
        self
    }

    /// Live ownership shared by clones of this session and its forwarding adapters.
    pub const fn operation_claims(self) -> &'a OperationClaims {
        self.claims
    }

    /// Whether process claims cover every writer to this storage.
    pub const fn process_exclusion(self) -> bool {
        self.process_exclusion
    }

    /// Whether accepted deferred writes can be sealed by this session's barrier.
    pub const fn deferred_durability(self) -> bool {
        self.deferred_durability
    }

    /// Whether this task's own acknowledged writes need no durability barrier.
    /// It certifies nothing about writes observed from other tasks.
    pub const fn durable_acknowledgement(self) -> bool {
        self.durable_acknowledgement
    }
}
