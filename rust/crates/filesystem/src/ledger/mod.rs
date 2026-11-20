//! The native work ledger: one original demand ledger and every view, allowance,
//! window, reservation and allocation that charges it.
//!
//! This layer names no materializer, mount, host or watch module. A root ledger is
//! admitted only with [`crate::services::ServicesInitialized`].

mod accounting;
mod admissions;
mod allocation;
mod allowance;
mod buffers;
mod checkout_work;
mod cleanup;
mod context;
mod link;
mod reservation;
mod reservation_meter;
mod reservations;
mod runtime_meter;
#[cfg(test)]
#[cfg(not(loom))]
mod scope_tests;
mod views;
mod window;
mod work;
#[cfg(test)]
mod write_probe;

pub use accounting::DemandWork;
pub(crate) use admissions::{NativeAllocationAccounting, ViewReadCompletion};
pub use allocation::NativeAllocation;
pub(crate) use allocation::NativeResourceRetirement;
pub(crate) use allowance::{NativeResourceAllowance, NativeResourceCredit};
pub(crate) use cleanup::NativeCleanupWrite;
#[cfg(target_os = "macos")]
pub(crate) use context::native_demand_read_error;
pub(crate) use context::{CaptureTransientCustody, NativeDemandReadMeter, native_demand_error};
pub(crate) use link::{Link, WeakLink};
pub use reservation::NativeWorkReservation;
pub use views::NativeSourceIo;
pub(crate) use views::{NativeInvocationView, NativeSourceExecutionView};
pub(crate) use window::NativeWorkWindow;
pub use work::NativeMaterializationWork;
#[cfg(test)]
pub(crate) use write_probe::WriteProbe;
