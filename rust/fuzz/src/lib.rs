//! Retained corpus and property checks, without building fuzz executables.

#[cfg(all(test, feature = "grpc-codegen"))]
#[path = "../tests/grpc_codegen.rs"]
mod grpc_codegen;
#[cfg(all(test, feature = "local-hotpath-options"))]
#[path = "../tests/local_hotpath_options.rs"]
mod local_hotpath_options;
#[cfg(all(test, feature = "local-hotpath-quantiles"))]
#[path = "../tests/local_hotpath_quantiles.rs"]
mod local_hotpath_quantiles;
#[cfg(all(test, feature = "observation"))]
#[path = "../tests/observation_slo.rs"]
mod observation_slo;
