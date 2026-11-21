//! Shared fixtures for filesystem cases that link the SDK library.

use acyclic_fs_work::performance::WorkBudget;

#[path = "../../filesystem/tests/support/allow_publication.rs"]
mod allow_publication;
#[path = "../../filesystem/tests/support/checkout_defaults.rs"]
mod checkout_defaults;
#[path = "../../filesystem/tests/support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../filesystem/tests/support/checkout_live_fixture.rs"]
mod checkout_live_fixture;
#[path = "../../filesystem/tests/support/checkout_manual.rs"]
mod checkout_manual;
#[path = "../../filesystem/tests/support/checkout_serialized.rs"]
mod checkout_serialized;
pub mod faults;
#[path = "../../filesystem/tests/support/volume_fixture.rs"]
mod fixtures;
#[path = "../../filesystem/tests/support/memory_checkout.rs"]
mod memory_checkout;
mod publication;
#[path = "../../filesystem/tests/support/volume_commit.rs"]
mod volume_commit;

pub use allow_publication::allow;
pub use checkout_fixture::{config, path, path_parts, writable_pinned};
pub use checkout_live_fixture::writable_live;
pub use checkout_manual::writable_manual;
pub use checkout_serialized::serialized_config;
pub use fixtures::{TestCheckout, TestResult, TestVolume, create, fixture, open_checkout};
pub use publication::{MemoryProviders, commit_head, fixture_with_providers};
pub use volume_commit::commit;

/// An unbounded work budget for cases testing semantics independently of budget enforcement.
pub const B: WorkBudget = WorkBudget::UNBOUNDED;
