use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/allow_publication.rs"]
mod allow_publication;
#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;
mod generation_position;
mod paths;
mod read;
mod transfer;
#[path = "../../../support/volume_commit.rs"]
mod volume_commit;
use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
