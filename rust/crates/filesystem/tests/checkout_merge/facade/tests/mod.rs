use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/allow_publication.rs"]
mod allow_publication;
#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;
mod merge;

use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
