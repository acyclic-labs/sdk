use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/allow_publication.rs"]
mod allow_publication;
#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
mod content;
mod detached;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;
mod identity;
#[path = "../../../support/volume_commit.rs"]
mod volume_commit;
#[path = "../../../support/volume_read.rs"]
mod volume_read;

use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
