//! Retained corpus checks, without building fuzz executables.

#[cfg(feature = "reader")]
#[path = "../support/reader_snapshot.rs"]
pub mod reader_snapshot;

#[cfg(all(test, feature = "native-answer"))]
#[path = "../tests/native_answer.rs"]
mod native_answer;
