use acyclic_fs_work::performance::WorkBudget;

use crate::allow_publication;
use crate::checkout_fixture;
#[path = "../../../support/checkout_manual.rs"]
mod checkout_manual;
use crate::checkout_serialized;
use crate::fixtures;
use crate::tracking_fixture as checkout_tracking;
mod publication;
mod transactions;

use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
