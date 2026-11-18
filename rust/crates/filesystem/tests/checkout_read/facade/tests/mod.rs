use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/allow_publication.rs"]
mod allow_publication;
mod attributes;
#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../../support/checkout_tracking.rs"]
mod checkout_tracking;
#[path = "../../../support/checkout_writable_tracking.rs"]
mod checkout_writable_tracking;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;
mod generation_position;
mod paths;
mod read;
mod transfer;
#[path = "../../../support/volume_commit.rs"]
mod volume_commit;
#[path = "../../../support/volume_read.rs"]
mod volume_read;
use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
