//! Explicit bounded text tools using the original task's authenticated reader.

use super::Tool;
use super::schema::ProjectionMode;
use super::text::{SearchMatches, SearchOptions};
use crate::{Error, Result, conversation::FileRef};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Selects bounded literal search and its model projection.
pub fn search_file(
    options: SearchOptions,
    maximum_result_bytes: u64,
    mode: ProjectionMode,
) -> Result<Tool> {
    options.validate()?;
    finite_result_bound(maximum_result_bytes)?;
    super::files::FileTool::Search {
        options,
        maximum_result_bytes,
    }
    .assemble(mode)
}

fn finite_result_bound(maximum: u64) -> Result<()> {
    if maximum == 0 || maximum > crate::conversation::MAX_EXACT_JS_INTEGER {
        return Err(Error::Invalid(
            "text result bound must be positive and exactly representable".into(),
        ));
    }
    Ok(())
}

mod inputs;
pub use inputs::{SearchInput, SearchResult};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod records;
#[cfg(test)]
mod tests;
