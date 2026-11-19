//! Shared fixtures for filesystem cases that link the SDK library.

use acyclic_fs_work::performance::WorkBudget;

#[path = "../../filesystem/tests/support/allow_publication.rs"]
mod allow_publication;
#[path = "../../filesystem/tests/support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../filesystem/tests/support/checkout_live_fixture.rs"]
mod checkout_live_fixture;
#[path = "../../filesystem/tests/support/checkout_manual.rs"]
mod checkout_manual;
#[path = "../../filesystem/tests/support/checkout_serialized.rs"]
mod checkout_serialized;
#[path = "../../filesystem/tests/support/volume_fixture.rs"]
mod fixtures;
mod publication;

pub use allow_publication::allow;
pub use checkout_fixture::{config, path, path_parts, writable_pinned};
pub use checkout_live_fixture::writable_live;
pub use checkout_manual::writable_manual;
pub use checkout_serialized::serialized_config;
pub use fixtures::{TestCheckout, TestResult, TestVolume, create, fixture, open_checkout};
pub use publication::{MemoryProviders, commit_head, fixture_with_providers};

/// An unbounded work budget for cases testing semantics independently of budget enforcement.
pub const B: WorkBudget = WorkBudget::UNBOUNDED;
