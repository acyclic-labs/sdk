//! Integration behaviour tests.

#[path = "cases/completion.rs"]
mod completion;

#[path = "cases/config.rs"]
mod config;

#[path = "cases/editor.rs"]
mod editor;

#[path = "cases/input.rs"]
mod input;

#[path = "cases/screen.rs"]
mod screen;

#[path = "cases/terminal.rs"]
mod terminal;

#[path = "cases/terminal_allocations.rs"]
mod terminal_allocations;

#[path = "cases/terminal_conformance.rs"]
mod terminal_conformance;

#[path = "cases/terminal_decoder.rs"]
mod terminal_decoder;

#[path = "cases/terminal_frame_faults.rs"]
mod terminal_frame_faults;

#[path = "cases/terminal_regions.rs"]
mod terminal_regions;

#[path = "cases/terminal_signals.rs"]
mod terminal_signals;

#[path = "cases/transcript_search.rs"]
mod transcript_search;
