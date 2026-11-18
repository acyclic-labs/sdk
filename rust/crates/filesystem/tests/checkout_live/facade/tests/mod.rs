use acyclic_fs_work::performance::WorkBudget;

#[path = "../../../support/allow_publication.rs"]
mod allow_publication;
#[path = "../../../support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "../../../support/checkout_serialized.rs"]
mod checkout_serialized;
#[path = "../../../support/volume_fixture.rs"]
mod fixtures;
mod live;
#[path = "../../../support/checkout_live_fixture.rs"]
mod live_fixture;
#[path = "../../../support/volume_commit.rs"]
mod volume_commit;
#[path = "../../../support/volume_read.rs"]
mod volume_read;

use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
