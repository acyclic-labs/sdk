//! Public checkout contracts.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

use acyclic_fs_work::performance::WorkBudget;

#[macro_use]
#[path = "support/provider_receipt.rs"]
mod provider_receipt;
#[path = "support/allow_publication.rs"]
mod allow_publication;
use allow_publication::allow;
#[path = "support/checkout_defaults.rs"]
mod checkout_defaults;
#[path = "support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "support/checkout_serialized.rs"]
mod checkout_serialized;
#[path = "support/volume_fixture.rs"]
mod fixtures;
#[path = "support/memory_checkout.rs"]
mod memory_checkout;
#[path = "support/checkout_tracking.rs"]
mod tracking_fixture;
#[path = "support/volume_commit.rs"]
mod volume_commit;
#[path = "support/volume_read.rs"]
mod volume_read;
const B: WorkBudget = WorkBudget::UNBOUNDED;

#[path = "checkout_cancellation.rs"]
mod checkout_cancellation;
#[path = "checkout_content.rs"]
mod checkout_content;
#[path = "checkout_live.rs"]
mod checkout_live;
#[path = "checkout_merge.rs"]
mod checkout_merge;
#[path = "checkout_profiles.rs"]
mod checkout_profiles;
#[path = "checkout_publication.rs"]
mod checkout_publication;
#[path = "checkout_read.rs"]
mod checkout_read;
#[path = "checkout_tracking.rs"]
mod checkout_tracking;
#[path = "checkout_transactions.rs"]
mod checkout_transactions;
#[path = "measured_creation.rs"]
mod creation;
#[path = "local_commit_model.rs"]
mod local_commit_model;
#[path = "local_facade.rs"]
mod local_facade;
#[path = "memory_authority.rs"]
mod memory_authority;
#[path = "memory_objects.rs"]
mod memory_objects;
