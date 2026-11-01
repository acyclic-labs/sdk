//! Agent checkpoint: persistent membership roots and raw turn bytes.
use super::{AgentState, Error};
use acyclic_actors::guest::{Checkpoint, CodecIdentity};
use prost::Message as _;

/// The checkpoint codec every agent actor commits with. Its state embeds the
/// same durable record shapes, so both boundaries admit the same version.
pub(crate) fn codec() -> CodecIdentity {
    identity().clone()
}

fn identity() -> &'static CodecIdentity {
    static IDENTITY: std::sync::LazyLock<CodecIdentity> =
        std::sync::LazyLock::new(|| CodecIdentity {
            name: "acyclic.harness.agent".into(),
            version: super::records::RECORD_VERSION.to_string(),
        });
    &IDENTITY
}

/// The agent state a committed checkpoint carries.
pub(crate) fn checkpoint_state(checkpoint: &Checkpoint) -> Result<AgentState, Error> {
    acyclic_actors::guest::admit_checkpoint(Some(checkpoint), identity())
        .map_err(|_| Error::IncompatibleCheckpointCodec)?;
    decode(&checkpoint.bytes)
}

#[derive(Clone, PartialEq, prost::Message)]
struct Encoded {
    // Tag 1 retired: Actors owns the committed inbox cursor.
    /// The state as JSON, without the fields below.
    #[prost(bytes = "vec", tag = "2")]
    state: Vec<u8>,
    /// Turn driver checkpoint.
    #[prost(bytes = "vec", optional, tag = "5")]
    turn: Option<Vec<u8>>,
    /// Inbox sequence of the input that started the turn.
    #[prost(uint64, optional, tag = "6")]
    turn_inbox: Option<u64>,
}

/// Retains the exact final checkpoint bytes for atomic publication.
pub(super) fn from_encoded(bytes: Vec<u8>) -> Result<Checkpoint, Error> {
    Ok(Checkpoint {
        codec: codec(),
        bytes: bytes.into(),
    })
}

/// Artifact state as checkpoint bytes. Subscription cuts belong to Actors.
pub(crate) fn encode(state: &AgentState) -> Result<Vec<u8>, Error> {
    let mut rest = state.clone();
    let turn = rest.turn.take();
    if rest.parent.take() != state.path.parent() {
        return Err(Error::Record("agent parent is not its path's parent"));
    }
    Ok(Encoded {
        state: serde_json::to_vec(&rest)
            .map_err(|_| Error::Record("agent checkpoint does not encode"))?,
        turn_inbox: turn.as_ref().map(|turn| turn.inbox),
        turn: turn.map(|turn| turn.checkpoint),
    }
    .encode_to_vec())
}

/// The artifact state checkpoint `bytes` holds.
pub(crate) fn decode(bytes: &[u8]) -> Result<AgentState, Error> {
    let malformed = || Error::Record("agent checkpoint is malformed");
    let encoded = Encoded::decode(bytes).map_err(|_| malformed())?;
    let mut state: AgentState = serde_json::from_slice(&encoded.state).map_err(|_| malformed())?;
    if state.turn.is_some() || state.parent.is_some() {
        return Err(malformed());
    }
    state.parent = state.path.parent();
    state.turn = match (encoded.turn, encoded.turn_inbox) {
        (Some(checkpoint), Some(inbox)) => Some(super::Progress { inbox, checkpoint }),
        (None, None) => None,
        _ => return Err(malformed()),
    };
    Ok(state)
}
