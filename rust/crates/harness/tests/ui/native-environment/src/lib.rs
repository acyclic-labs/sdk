//! Compiler controls for the production process-environment boundary.

#[path = "../../../../../harness-core/src/native_environment.rs"]
pub mod real_seam;

/// Read operations rejected outside the production environment boundary.
#[cfg(feature = "negative")]
pub fn forbidden() {
    let _ = std::env::var_os("HOME");
    use std::env::var_os as renamed;
    let _ = renamed(r#"APPDATA"#);
    use std::env as process;
    let _ = process::var("CODEX_HOME");
    let _ = std::env::vars();
    let _ = std::env::vars_os();
    let _ = std::env::home_dir();
}

/// Compile-time environment reads rejected outside explicit Cargo metadata.
#[cfg(feature = "negative")]
pub fn forbidden_compile_time() {
    let _ = env!("HOME");
    let _ = option_env!("HOME");
    use std::option_env as renamed;
    let _ = renamed!(r"USERPROFILE",);
}

/// Explicit compile-time Cargo metadata admitted by the compiler policy.
#[expect(clippy::disallowed_macros, reason = "explicit Cargo metadata seam")]
pub mod build_metadata {
    /// Directory containing this crate's manifest.
    pub const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");
}
