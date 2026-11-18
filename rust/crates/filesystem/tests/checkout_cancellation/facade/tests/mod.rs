use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/allow_publication.rs"]
mod allow_publication;
#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
mod creation;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;

const B: WorkBudget = WorkBudget::UNBOUNDED;
