//! Attached devices: a user's machine runs the `shell` commands that agents,
//! wherever they run, dispatch to it.
//!
//! Each device is the actor `targets/<name>` ([`TargetActor`]). The device
//! holds one actor socket to it while it is attached: the socket's Open
//! commits the target's presence record online with a fresh session, and its
//! Close (including the Close(Runtime) a restarted host delivers for every
//! socket it held) commits it offline. There is no heartbeat, lease or poll.
//!
//! A `shell` call naming the device is one [`acyclic_actors::call`]: its
//! request is one [`Run`] record on the device's queue, appended in the same
//! commit as the agent's journal entry that claims the call
//! ([`crate::tool::ToolExecutor::dispatch`]): a claimed call was queued
//! exactly once, an unclaimed one never. Everything after lives in the
//! agent's own call journal ([`call`]). The actor reads the queue in order
//! with the socket's events. A run for the open session is accepted, fenced
//! by that session's end marker, and handed to the device; a run for any
//! other session is settled as an error and never handed over. The device
//! ([`DeviceRunner`]) verifies each run's grant, evaluates its owner's
//! execution policy again and runs it after the committed handover; the
//! actor records its output chunks and its one terminal. The session ends
//! with its fence and offline presence: an unfinished call may have run, it
//! settles `indeterminate` and is never run again. This avoids enumerating
//! commands or growing the Actor checkpoint on disconnect.
//!
//! Commands run by the device's own executor ([`DeviceShell`]): the same
//! native process runner a local `shell` call uses. Credentials never enter
//! the queue: a run carries a sealed grant, which proves its admission but
//! can delegate nothing.

mod actor;
mod directory;
mod lifecycle;
mod output;
mod records;
mod runner;

pub use actor::{TargetActor, TargetComposer, attach, target_actor};
pub use directory::{DeviceGrant, TargetDirectory};
pub use lifecycle::DeviceLifecycle;
pub use records::{
    DeviceMessage, RECORD_VERSION, Run, RunCommand, TargetPaths, TargetStatus, call, chunk_output,
};
pub use runner::{
    Attachment, DeviceApprover, DeviceAuthority, DeviceLink, DeviceRunner, DeviceShell,
};
