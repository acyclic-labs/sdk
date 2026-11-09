//! Retained corpus checks, without building fuzz executables.

#[cfg(all(test, feature = "client"))]
#[path = "../ui_blocks_oracle.rs"]
mod ui_blocks;

#[cfg(all(test, feature = "native-answer"))]
#[path = "../tests/native_answer.rs"]
mod native_answer;
