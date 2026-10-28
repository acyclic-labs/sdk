/// What recovery hands the state machine, in order.
enum Recovered {
    /// The store's durable revision incarnation, before any state.
    Incarnation(u128),
    /// Authorizes disk repair only after every recovered semantic operation passed.
    Validate(tokio::sync::oneshot::Sender<bool>),
    /// The snapshot's state, first.
    State { encoded: Vec<u8> },
    /// Each command in the journal after it.
    Command {
        committed_at_millis: u64,
        command: Command,
    },
}
