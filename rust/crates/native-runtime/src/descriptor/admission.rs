//! Admission of work that holds several descriptors while it runs.
//!
//! The cache bounds descriptors that idle files keep; work such as a tree
//! materialization instead holds its directories and files open until it
//! finishes. Each such operation is admitted for the descriptors it may hold
//! at once, from one process-wide allowance, so the descriptors held by work
//! in flight stay bounded however many operations start together: excess
//! operations wait for admission instead of failing for want of descriptors.

use std::io;
use std::sync::OnceLock;
use tokio::sync::{Semaphore, SemaphorePermit};

/// Descriptors admitted to one operation, returned when it is dropped.
#[must_use = "admission lasts only while it is held"]
pub struct DescriptorAdmission {
    _permit: SemaphorePermit<'static>,
}

fn allowance() -> &'static Semaphore {
    static ALLOWANCE: OnceLock<Semaphore> = OnceLock::new();
    ALLOWANCE.get_or_init(|| Semaphore::new(admitted_descriptor_capacity()))
}

/// The environment variable that bounds the descriptors all admitted work
/// holds at once, read once as the process first admits work.
pub const ADMITTED_DESCRIPTORS_VARIABLE: &str = "ACYCLIC_ADMITTED_DESCRIPTORS";

/// The descriptors all admitted work may hold at once:
/// [`ADMITTED_DESCRIPTORS_VARIABLE`] when it is a positive integer, or
/// otherwise as many as the cache keeps by default, a quarter of the
/// process's descriptor limit, so that the two together hold at most half.
pub fn admitted_descriptor_capacity() -> usize {
    static CAPACITY: OnceLock<usize> = OnceLock::new();
    *CAPACITY.get_or_init(|| {
        std::env::var_os(ADMITTED_DESCRIPTORS_VARIABLE)
            .and_then(|value| value.to_str()?.parse::<usize>().ok())
            .filter(|capacity| *capacity > 0)
            .unwrap_or_else(super::default_capacity)
            .min(Semaphore::MAX_PERMITS)
    })
}

/// Waits until `descriptors` more may be held, then admits them until the
/// returned admission drops. An operation that needs more than the whole
/// allowance is admitted alone.
pub async fn admit_descriptors(descriptors: usize) -> io::Result<DescriptorAdmission> {
    let wanted = descriptors.clamp(1, admitted_descriptor_capacity());
    let wanted = u32::try_from(wanted).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let permit = allowance()
        .acquire_many(wanted)
        .await
        .map_err(|_| io::Error::other("descriptor allowance closed"))?;
    Ok(DescriptorAdmission { _permit: permit })
}
