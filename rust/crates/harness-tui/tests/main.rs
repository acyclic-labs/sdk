//! Integration behaviour tests.
#![cfg(all(test, not(loom)))]

acyclic_test_support::counting_allocator!();

#[path = "cases/completion.rs"]
mod completion;

#[path = "cases/config.rs"]
mod config;

#[path = "cases/editor.rs"]
mod editor;

#[path = "cases/golden.rs"]
mod golden;

#[path = "cases/input.rs"]
mod input;

#[path = "cases/screen.rs"]
mod screen;
