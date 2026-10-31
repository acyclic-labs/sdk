//! Attached devices: a user's machine runs the `shell` commands that agents,
//! wherever they run, dispatch to it.
//!
//! Each device is the actor `targets/<name>` ([`TargetActor`]). The device
//! holds one actor socket to it while it is attached: the socket's Open
//! commits the target's presence record online with a fresh session, and its
//! Close (including the Close(Runtime) a restarted host delivers for every
//! socket it held) commits it offline. There is no heartbeat, lease or poll.
//!
//! A `shell` call naming the device is one [`Run`] record on the device's
//! queue, appended in the same commit as the agent's journal entry that
//! claims the call ([`crate::tool::ToolExecutor::dispatch`]): a claimed call
//! was queued exactly once, an unclaimed one never. The actor reads the
//! queue in order with the socket's events. A run for the open session is
//! recorded `Started` and handed to the device; a run for any other session
//! is recorded `Refused` and never handed over. The device
//! ([`DeviceRunner`]) verifies each run's grant, evaluates its owner's
//! execution policy again and runs it after the committed handover, then
//! reports `Exit` or `Refused`. The session ends with one immutable `Lost`
//! marker and offline presence. An unfinished run joins that marker with its
//! own operation stream: it may have run, and it is never run again. This
//! avoids enumerating commands or growing the Actor checkpoint on disconnect.
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
    DeviceMessage, RECORD_VERSION, Run, RunCommand, RunEvent, TargetPaths, TargetStatus,
};
pub use runner::{
    Attachment, DeviceApprover, DeviceAuthority, DeviceLink, DeviceRunner, DeviceShell,
};
