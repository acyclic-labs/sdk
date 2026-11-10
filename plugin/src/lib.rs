//! Shared agent frontend and native-host admission.

pub mod ingress;
pub mod input;
pub mod marketplace;
mod runner;

pub use runner::{
    CliCompletion, CliFailure, CliPhase, DiagnosticConfigurationError, FailureReport, main_from,
    main_with,
};

#[cfg(feature = "test-support")]
pub use runner::test_support;
