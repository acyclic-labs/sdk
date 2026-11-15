//! Hot-path cost sites for the deterministic cost ledger.
//!
//! Every round trip, fsync, journal hop, quorum read, leader hop, lock, copy
//! and auth check on a request path must name the public guarantee it buys
//! (AGENTS.md). Production code marks each one with [`charge`] at the point it
//! is paid. Without `test-support` this compiles to nothing; inside a
//! simulation scope it is counted exactly, per open operation, by
//! `testing::ledger`, so tests can assert per-operation budgets.

/// One accounted hot-path cost.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Cost {
    /// A request to another process or host, awaited for its reply.
    RoundTrip,
    /// A durable flush (data or metadata) of a file or directory.
    Fsync,
    /// A hop through the conversation journal that a step waits on.
    JournalHop,
    /// A record appended to a journal or stream.
    JournalAppend,
    /// A read that must consult a quorum.
    QuorumRead,
    /// A request forwarded to, or redirected by, a leader.
    LeaderHop,
    /// An acquisition of a contended lock on a request path.
    Lock,
    /// A copy of payload bytes; charge it with the bytes copied.
    Copy,
    /// A credential or capability check.
    AuthCheck,
}

/// Counts one `cost` against every operation open in the current simulation
/// scope. Outside a simulation, and in builds without `test-support`, nothing.
#[inline]
pub fn charge(cost: Cost) {
    charge_bytes(cost, 0);
}

/// [`charge`], also counting `bytes` moved by this cost (a copy's length).
#[inline]
pub fn charge_bytes(cost: Cost, bytes: u64) {
    #[cfg(feature = "test-support")]
    crate::testing::ledger::charge(cost, bytes);
    #[cfg(not(feature = "test-support"))]
    let _ = (cost, bytes);
}
