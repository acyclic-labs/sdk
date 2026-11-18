use super::*;
use crate::WorkError;
use crate::kernel::*;

#[path = "../../../tests/support/wire_errors.rs"]
mod fixtures;

use fixtures::{allocation, converted, io, object};

mod kernel_cases;

fn converted_decode(error: &CanonicalDecodeError) -> Code {
    Status::from(&converted(error.clone())).code()
}
