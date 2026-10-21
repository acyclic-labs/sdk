//! Explicit metadata inputs for a replay's physical filesystem boundary.
use super::*;

/// Native timestamp facts, including fields the host cannot observe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeTimes {
    /// Creation (birth) time in nanoseconds since the Unix epoch, if known.
    pub created_ns: Option<i64>,
    /// Last content modification time in nanoseconds since the Unix epoch.
    pub modified_ns: Option<i64>,
    /// Last access time in nanoseconds since the Unix epoch.
    pub accessed_ns: Option<i64>,
    /// Last metadata (inode) change time in nanoseconds since the Unix epoch.
    pub changed_ns: Option<i64>,
}

/// Physical identity and kind remain host facts; a fixture supplies only time.
#[derive(Clone, Copy, Debug)]
pub struct NativeMetadataInput {
    /// Device holding the object, as the host reports it.
    pub device: u64,
    /// Object identity on that device (inode or file index).
    pub object: u64,
    /// Whether the object is a directory.
    pub directory: bool,
    /// The timestamps the host observed for the object.
    pub times: NativeTimes,
}

pub(super) type Provider = dyn Fn(NativeMetadataInput) -> NativeTimes + Send + Sync;

impl Schedule {
    /// Supplies explicit native timestamp inputs before dispatch. Physical
    /// identity, content and snapshot revalidation still use the original host.
    /// Without a provider, captures retain every original native timestamp.
    pub fn set_native_times_provider(
        &self,
        provider: impl Fn(NativeMetadataInput) -> NativeTimes + Send + Sync + 'static,
    ) {
        let mut state = self.0.state();
        assert!(
            state.trace.is_empty(),
            "metadata inputs must precede dispatch"
        );
        assert_eq!(state.active, 0, "cannot replace live metadata inputs");
        assert!(
            state.metadata.is_none(),
            "metadata inputs already installed"
        );
        state.metadata = Some(Arc::new(provider));
    }
}

/// Reads fixture input on the original task or its receiving native worker.
/// Callers must separately validate snapshots using unmodified physical facts.
pub fn capture_native_times(input: NativeMetadataInput) -> Option<NativeTimes> {
    let context = Context::simulation()?;
    let provider = context.schedule.0.state().metadata.clone()?;
    Some(provider(input))
}
