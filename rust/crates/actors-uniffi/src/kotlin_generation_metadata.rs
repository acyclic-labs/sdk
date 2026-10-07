//! Rust-owned release inputs for the qualified Kotlin/JVM package.
//!
//! These values are source authority for the candidate packaging recipe. The
//! generated Kotlin file and package manifests are derived artifacts; they do
//! not define the Cargo version, source cohort, or visibility policy.

/// The pinned UniFFI generator cohort used by this standalone crate.
pub const UNIFFI_VERSION: &str = "0.31.0";
/// The Cargo package version used as the generated Maven version.
pub const CARGO_PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// The Maven artifact suffix emitted for the Kotlin/JVM target.
pub const MAVEN_ARTIFACT_SUFFIX: &str = "kotlin";
/// The Kotlin target release used by the qualification consumer.
pub const JVM_TARGET: &str = "17";
/// The visibility policy applied by the maintained template patch.
pub const OPAQUE_CONSTRUCTOR_VISIBILITY: &str = "internal";
/// Standard JNA resource roots; no custom loader is generated.
pub const JNA_RESOURCE_PREFIXES: &[&str] = &["linux-x86-64", "win32-x86-64", "darwin-aarch64"];

/// Source-backed metadata consumed by a packaging/release driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KotlinGenerationMetadata {
    pub cargo_package_version: &'static str,
    pub uniffi_version: &'static str,
    pub maven_artifact_suffix: &'static str,
    pub jvm_target: &'static str,
    pub opaque_constructor_visibility: &'static str,
    pub jna_resource_prefixes: &'static [&'static str],
}

/// Return the canonical source-owned metadata for this crate.
pub const fn metadata() -> KotlinGenerationMetadata {
    KotlinGenerationMetadata {
        cargo_package_version: CARGO_PACKAGE_VERSION,
        uniffi_version: UNIFFI_VERSION,
        maven_artifact_suffix: MAVEN_ARTIFACT_SUFFIX,
        jvm_target: JVM_TARGET,
        opaque_constructor_visibility: OPAQUE_CONSTRUCTOR_VISIBILITY,
        jna_resource_prefixes: JNA_RESOURCE_PREFIXES,
    }
}
