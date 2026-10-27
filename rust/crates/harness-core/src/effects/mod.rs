//! Validation of effect result schemas and result bytes.

use crate::{Error, Result, conversation::FileRef};
use serde_json::Value;

mod validation;
pub(crate) use validation::{validate_result_bytes, validate_schema_bytes};
