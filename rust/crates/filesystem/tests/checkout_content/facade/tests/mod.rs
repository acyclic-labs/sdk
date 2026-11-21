use acyclic_fs_work::performance::WorkBudget;

use crate::allow_publication;
use crate::checkout_fixture;
mod content;
mod detached;
use crate::fixtures;
mod identity;
use crate::volume_commit;
use crate::volume_read;

use allow_publication::allow;

const B: WorkBudget = WorkBudget::UNBOUNDED;
