use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/allow_publication.rs"]
mod allow_publication;
#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../../support/checkout_tracking.rs"]
mod checkout_tracking;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;
mod profiles;
#[path = "../../../support/volume_commit.rs"]
mod volume_commit;

use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
