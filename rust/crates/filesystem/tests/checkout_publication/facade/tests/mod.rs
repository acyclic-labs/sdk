use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/allow_publication.rs"]
mod allow_publication;
#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../../support/checkout_manual.rs"]
mod checkout_manual;
#[path = "../../../support/checkout_serialized.rs"]
mod checkout_serialized;
#[path = "../../../support/checkout_tracking.rs"]
mod checkout_tracking;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;
mod publication;
mod transactions;

use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
