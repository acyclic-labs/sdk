//! Output identities for Rust-owned public facade generation.
//!
//! Source hashing excludes these generated files while retaining their Rust
//! emitters and every handwritten runtime adapter as authored input.

use crate::csharp_typed_facades::CSHARP_TYPED_PATH;
use crate::facades::{
    DART_TYPED_PATH, FacadeLanguage, JAVA_CLIENTS_PATH, JAVA_PATH, JAVA_REQUESTS_PATH,
    JAVA_RESPONSES_PATH, KOTLIN_CLIENTS_PATH, KOTLIN_PATH, KOTLIN_REQUESTS_PATH,
    KOTLIN_RESPONSES_PATH, PHP_TYPED_PATH, RUBY_RBS_PATH, RUBY_SORBET_PATH, RUBY_TYPED_PATH,
    SCALA_CLIENTS_PATH, SCALA_PATH, SCALA_REQUESTS_PATH, SCALA_RESPONSES_PATH,
};
use crate::swift_cpp_typed_facades::{CPP_TYPED_PATH, SWIFT_TYPED_PATH};

/// Exact public facade output paths, derived from the emitters' path constants.
pub const GENERATED_FACADE_PATHS: &[&str] = &[
    FacadeLanguage::Python.output_path(),
    FacadeLanguage::Go.output_path(),
    FacadeLanguage::Ruby.output_path(),
    FacadeLanguage::Php.output_path(),
    FacadeLanguage::Dart.output_path(),
    FacadeLanguage::Java.output_path(),
    FacadeLanguage::Csharp.output_path(),
    JAVA_PATH,
    KOTLIN_PATH,
    SCALA_PATH,
    JAVA_REQUESTS_PATH,
    KOTLIN_REQUESTS_PATH,
    SCALA_REQUESTS_PATH,
    JAVA_CLIENTS_PATH,
    KOTLIN_CLIENTS_PATH,
    SCALA_CLIENTS_PATH,
    JAVA_RESPONSES_PATH,
    KOTLIN_RESPONSES_PATH,
    SCALA_RESPONSES_PATH,
    CSHARP_TYPED_PATH,
    SWIFT_TYPED_PATH,
    CPP_TYPED_PATH,
    RUBY_TYPED_PATH,
    RUBY_RBS_PATH,
    RUBY_SORBET_PATH,
    PHP_TYPED_PATH,
    DART_TYPED_PATH,
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn output_registry_covers_public_facades_without_duplicate_paths() {
        let paths = GENERATED_FACADE_PATHS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(paths.len(), GENERATED_FACADE_PATHS.len());
        for output in crate::generate_remote_facades() {
            assert!(
                paths.contains(output.path),
                "unregistered facade {}",
                output.path
            );
        }
        for output in crate::generate_portable_typed_facades() {
            assert!(
                paths.contains(output.path),
                "unregistered typed facade {}",
                output.path
            );
        }
        for (path, _) in crate::generate_jvm_semantic_types()
            .into_iter()
            .chain(crate::generate_jvm_typed_requests())
            .chain(crate::generate_jvm_typed_clients())
            .chain(crate::generate_jvm_typed_responses())
            .chain(crate::generate_swift_cpp_typed_facades())
            .chain(std::iter::once(crate::generate_csharp_typed_facade()))
        {
            assert!(paths.contains(path), "unregistered typed facade {path}");
        }
    }
}
