//! Harness filesystem access and file tool integration tests.
#![cfg(not(loom))]

acyclic_test_support::counting_allocator!();

#[path = "../../harness/tests/support/content_context.rs"]
mod content_context;
mod filesystem_access;
mod portable_text_tools;
mod retained_content;
mod volume_composition;
