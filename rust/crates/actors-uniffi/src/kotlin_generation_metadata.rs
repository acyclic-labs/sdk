//! Rust-owned release inputs for the qualified Kotlin/JVM package.
//!
//! These values are source authority for the candidate packaging recipe. The
//! generated Kotlin file and package manifests are derived artifacts; they do
//! not define the Cargo version, source cohort, or visibility policy.

/// The pinned UniFFI generator cohort used by this standalone crate.
pub const UNIFFI_VERSION: &str = "0.31.0";
/// The Rust Actors domain source cohort used by the generated package.
pub const DOMAIN_SOURCE_SHA256: &str = "79092881EC6B9434A3AC3AE6B810DCA46BB85A8E4C8C1E98191982AC5D82B4B2";
/// The immutable-record generator configuration cohort.
pub const UNIFFI_CONFIG_SHA256: &str = "950AB29CBC05E20643831B08A16FA5CABB99258FB4E0EACEA1FC78D54718B599";
/// The pinned Kotlin generator source patch.
pub const GENERATOR_PATCH_SHA256: &str = "275222CFE64FD25C3723F6619FA87929713D7950320D46777D9B732A3C41A474";
/// The Cargo package version used as the generated Maven version.
pub const CARGO_PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// The Maven artifact suffix emitted for the Kotlin/JVM target.
pub const MAVEN_ARTIFACT_SUFFIX: &str = "kotlin";
/// The Maven group selected by the Rust-owned packaging policy.
pub const MAVEN_GROUP_ID: &str = "dev.acyclic";
/// The Kotlin target release used by the qualification consumer.
pub const JVM_TARGET: &str = "17";
/// The pinned Kotlin compiler/runtime dependency.
pub const KOTLIN_VERSION: &str = "1.9.21";
/// The pinned coroutine runtime dependency.
pub const COROUTINES_VERSION: &str = "1.8.0";
/// The pinned JNA runtime dependency.
pub const JNA_VERSION: &str = "5.15.0";
/// The visibility policy applied by the maintained template patch.
pub const OPAQUE_CONSTRUCTOR_VISIBILITY: &str = "internal";
/// Standard JNA resource roots; no custom loader is generated.
pub const JNA_RESOURCE_PREFIXES: &[&str] = &["linux-x86-64", "win32-x86-64", "darwin-aarch64"];
/// Generated Kotlin source hash for the qualified 0.31.0 cohort.
pub const GENERATED_KOTLIN_SHA256: &str = "201C988FA9B679784EE12E6FFC5C72EE88D5AD001633D085FFEFD628E392AC58";
/// Linux x86_64 native resource hash.
pub const LINUX_NATIVE_SHA256: &str = "AA6427DD1C6836F164CEB83BF0D14C83E6CD8BC1598683654F7619DB5BF76A24";
/// Windows x86_64 native resource hash.
pub const WINDOWS_NATIVE_SHA256: &str = "A09452F273B201FA7E3D288F6C3B3FDFC4ABD979431392797D4C80D375D85241";
/// macOS arm64 native resource hash.
pub const MACOS_ARM64_NATIVE_SHA256: &str = "28D885561244BD2D682D1103B70AD8C1989AC341777F28499731C1140FF3B719";

/// Source-backed metadata consumed by a packaging/release driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KotlinGenerationMetadata {
    pub domain_source_sha256: &'static str,
    pub uniffi_config_sha256: &'static str,
    pub generator_patch_sha256: &'static str,
    pub cargo_package_version: &'static str,
    pub uniffi_version: &'static str,
    pub maven_group_id: &'static str,
    pub maven_artifact_suffix: &'static str,
    pub jvm_target: &'static str,
    pub kotlin_version: &'static str,
    pub coroutines_version: &'static str,
    pub jna_version: &'static str,
    pub opaque_constructor_visibility: &'static str,
    pub jna_resource_prefixes: &'static [&'static str],
    pub generated_kotlin_sha256: &'static str,
    pub linux_native_sha256: &'static str,
    pub windows_native_sha256: &'static str,
    pub macos_arm64_native_sha256: &'static str,
}

/// Return the canonical source-owned metadata for this crate.
pub const fn metadata() -> KotlinGenerationMetadata {
    KotlinGenerationMetadata {
        domain_source_sha256: DOMAIN_SOURCE_SHA256,
        uniffi_config_sha256: UNIFFI_CONFIG_SHA256,
        generator_patch_sha256: GENERATOR_PATCH_SHA256,
        cargo_package_version: CARGO_PACKAGE_VERSION,
        uniffi_version: UNIFFI_VERSION,
        maven_group_id: MAVEN_GROUP_ID,
        maven_artifact_suffix: MAVEN_ARTIFACT_SUFFIX,
        jvm_target: JVM_TARGET,
        kotlin_version: KOTLIN_VERSION,
        coroutines_version: COROUTINES_VERSION,
        jna_version: JNA_VERSION,
        opaque_constructor_visibility: OPAQUE_CONSTRUCTOR_VISIBILITY,
        jna_resource_prefixes: JNA_RESOURCE_PREFIXES,
        generated_kotlin_sha256: GENERATED_KOTLIN_SHA256,
        linux_native_sha256: LINUX_NATIVE_SHA256,
        windows_native_sha256: WINDOWS_NATIVE_SHA256,
        macos_arm64_native_sha256: MACOS_ARM64_NATIVE_SHA256,
    }
}
