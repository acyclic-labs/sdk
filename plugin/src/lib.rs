//! Shared frontend embedded by the product executable and server compositions.
pub mod ingress;
mod runner;

pub use runner::{
    CliCompletion, CliFailure, CliPhase, DiagnosticConfigurationError, FailureReport, main_from,
    main_with,
};
