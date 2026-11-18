//! Terminal projection of the shared TUI configuration.

mod diagnostics;
mod theme;

pub use acyclic_harness_tui_core::config::{
    ConfigFile, ConfigView, Diagnostic, DiagnosticKind, Scope, Token, fold, parse, parse_bytes,
};
pub use diagnostics::ConfigDiagnostics;
pub use theme::{ThemeRole, ThemeStyles, default_style};
