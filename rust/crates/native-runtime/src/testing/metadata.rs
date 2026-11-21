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
pub(super) type SourceIdentityProvider = dyn Fn([u8; 32]) -> [u8; 32] + Send + Sync;

impl Schedule {
    /// Supplies explicit native timestamp inputs before dispatch. Physical
    /// identity, content and snapshot revalidation still use the original host.
    /// Without a provider, captures retain every original native timestamp.
    ///
    /// # Panics
    ///
    /// Panics after dispatch or cancellation, while owned work is active, or
    /// if a timestamp provider is already installed.
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

    /// Supplies stable stand-ins for native file identities before dispatch.
    /// Only logical source keys use these stand-ins. Host snapshot validation
    /// and authorization continue to compare the original physical identities.
    ///
    /// # Panics
    ///
    /// Panics while owned work is active or if a provider is already installed.
    pub fn set_source_identity_provider(
        &self,
        provider: impl Fn([u8; 32]) -> [u8; 32] + Send + Sync + 'static,
    ) {
        let mut state = self.0.state();
        assert_eq!(state.active, 0, "cannot replace live source identities");
        assert!(
            state.source_identity.is_none(),
            "source identities already installed"
        );
        state.source_identity = Some(Arc::new(provider));
    }
}

/// Reads fixture input on the original task or its receiving native worker.
/// Callers must separately validate snapshots using unmodified physical facts.
pub fn capture_native_times(input: NativeMetadataInput) -> Option<NativeTimes> {
    let context = Context::simulation()?;
    let provider = context.schedule.0.state().metadata.clone()?;
    Some(provider(input))
}

/// Selects a replay's logical source identity, keeping native validation intact.
pub fn source_identity(original: [u8; 32]) -> [u8; 32] {
    let provider = Context::simulation()
        .and_then(|context| context.schedule.0.state().source_identity.clone());
    provider.map_or(original, |provider| provider(original))
}

/// Selects the identity a host-derived value (a root's file handle, a path's
/// digest) records in logical state. A seeded world numbers each distinct
/// original per domain in its own first-sight order and records a stand-in
/// derived from the domain and that number alone, so every process and every
/// incarnation of one world records the same identities. Outside a seeded
/// simulation the original is returned unchanged.
pub fn stable_identity<const N: usize>(domain: &'static str, original: [u8; N]) -> [u8; N] {
    let Some(context) = Context::simulation().filter(|context| context.schedule.is_seeded()) else {
        return original;
    };
    let ordinal = ordinal(&context.schedule, domain, &original);
    let mut hash = blake3::Hasher::new_derive_key(domain);
    hash.update(&ordinal.to_le_bytes());
    let mut stand_in = [0; N];
    hash.finalize_xof().fill(&mut stand_in);
    stand_in
}

/// The world's number for `original` in `domain`: distinct originals are
/// numbered in first-sight order, and an original seen again keeps its number.
fn ordinal(schedule: &Schedule, domain: &'static str, original: &[u8]) -> u64 {
    let mut state = schedule.0.state();
    let next = state
        .stand_ins
        .range((domain, Vec::new())..)
        .take_while(|((seen, _), _)| *seen == domain)
        .count() as u64;
    *state
        .stand_ins
        .entry((domain, original.to_vec()))
        .or_insert(next)
}

/// The timestamp a physical capture records. A seeded world without a times
/// provider records each distinct host timestamp as a stand-in numbered in its
/// first-sight order, so equal times stay equal, distinct times stay distinct,
/// and the record repeats in every process. A world with a provider, and any
/// code outside a seeded simulation, records the host's time.
pub fn stable_native_time(original_ns: i64) -> i64 {
    const BASE_NS: i64 = 1_700_000_000_000_000_000;
    match Context::simulation().filter(|context| {
        context.schedule.is_seeded() && context.schedule.0.state().metadata.is_none()
    }) {
        Some(context) => {
            let ordinal = ordinal(
                &context.schedule,
                "acyclic.testing.native-time/v1",
                &original_ns.to_le_bytes(),
            );
            BASE_NS.saturating_add(i64::try_from(ordinal).unwrap_or(i64::MAX))
        }
        None => original_ns,
    }
}
