use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;
mod transactions;
#[path = "../../../support/volume_read.rs"]
mod volume_read;

const B: WorkBudget = WorkBudget::UNBOUNDED;
