//! Local composition acceptance: inspect the first provider request.
#![cfg(not(target_arch = "wasm32"))]

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[path = "e2e/local_skills.rs"]
mod local_skills;
// The crate root already loads this file; loading it again would compile it twice.
use crate::local_workspace;
