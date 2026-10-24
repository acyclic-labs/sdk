//! Integration behaviour tests.

#[path = "../tests/cases/completion.rs"]
mod completion;

#[path = "../tests/cases/config.rs"]
mod config;

#[path = "../tests/cases/editor.rs"]
mod editor;

#[path = "../tests/cases/input.rs"]
mod input;

#[path = "../tests/cases/screen.rs"]
mod screen;

#[path = "../tests/cases/terminal.rs"]
mod terminal;

#[path = "../tests/cases/terminal_allocations.rs"]
mod terminal_allocations;

#[path = "../tests/cases/terminal_conformance.rs"]
mod terminal_conformance;

#[path = "../tests/cases/terminal_decoder.rs"]
mod terminal_decoder;

#[path = "../tests/cases/terminal_frame_faults.rs"]
mod terminal_frame_faults;

#[path = "../tests/cases/terminal_regions.rs"]
mod terminal_regions;

#[path = "../tests/cases/terminal_signals.rs"]
mod terminal_signals;

#[path = "../tests/cases/transcript_search.rs"]
mod transcript_search;
