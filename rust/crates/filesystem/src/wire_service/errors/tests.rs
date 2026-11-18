use super::*;
use crate::kernel::*;
use crate::{CancellationError, WorkError};

#[path = "../../../tests/support/wire_errors.rs"]
mod fixtures;

use fixtures::{allocation, converted, io, object};

mod kernel_cases;

fn converted_decode(error: &CanonicalDecodeError) -> Code {
    Status::from(&converted(error.clone())).code()
}
