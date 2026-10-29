/// What recovery hands the state machine, in order.
enum Recovered {
    /// The store's revision identity (its scope and durable incarnation),
    /// before any state.
    Incarnation(crate::RevisionScope, u128),
    /// Authorizes disk repair only after every recovered semantic operation passed.
    Validate(tokio::sync::oneshot::Sender<bool>),
    /// The snapshot's state, first.
    State { encoded: Vec<u8> },
    /// Each command in the journal after it.
    WriteRequest {
        committed_at_millis: u64,
        command: WriteRequest,
    },
}
