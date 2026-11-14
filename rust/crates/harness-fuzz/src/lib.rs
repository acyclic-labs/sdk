//! Retained corpus checks, without building fuzz executables.

#[cfg(feature = "reader")]
#[path = "../support/reader_snapshot.rs"]
pub mod reader_snapshot;
#[cfg(test)]
#[cfg(feature = "client")]
#[path = "../ui_blocks_oracle.rs"]
mod ui_blocks;

#[cfg(test)]
#[cfg(feature = "native-answer")]
#[path = "../tests/native_answer.rs"]
mod native_answer;
