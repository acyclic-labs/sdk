//! Retained corpus and property checks, without building fuzz executables.
#![cfg(test)]

#[cfg(feature = "grpc-codegen")]
mod grpc_codegen;
#[cfg(feature = "local-hotpath-options")]
mod local_hotpath_options;
#[cfg(feature = "local-hotpath-quantiles")]
mod local_hotpath_quantiles;
#[cfg(feature = "observation")]
mod observation_slo;
