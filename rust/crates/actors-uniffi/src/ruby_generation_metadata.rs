//! Rust-owned recipe metadata for the maintained UniFFI Ruby generator.
//!
//! This describes the generic async future boundary consumed by the SDK
//! generation runner. It deliberately contains no operation-specific Ruby
//! wrappers: UniFFI metadata remains the authority for the generated methods.

/// The maintained generator cohort used by the Ruby package.
pub const UNIFFI_VERSION: &str = "0.31.0";
/// The source patch applied to the maintained `uniffi_bindgen` crate.
pub const GENERATOR_PATCH_PATH: &str =
    "rust/crates/actors-uniffi/generator-patches/uniffi-0.31.0-ruby-async.patch";
pub const GENERATOR_PATCH_SHA256: &str =
    "CBDF2A090DA2995AD6BD37042F6D1EF0FDC44047ECC26379AE0048880DBC18BC";
/// Generic native future hooks emitted from UniFFI async metadata.
pub const ASYNC_FUTURE_HOOKS: &[&str] = &[
    "ffi_rust_future_poll",
    "ffi_rust_future_complete",
    "ffi_rust_future_cancel",
    "ffi_rust_future_free",
];
/// The current Actors async export surface used by the qualification runner.
pub const ASYNC_EXPORTS: &[&str] = &[
    "connect_actors",
    "connect_actors_with_ca",
    "ActorsClient.create_actor",
    "ActorsClient.update_actor",
    "ActorsClient.inspect_actor",
    "ActorsClient.inspect_actor_request",
    "ActorsClient.add_subscription",
    "ActorsClient.remove_subscription",
    "ActorsClient.resume_subscription",
    "ActorsClient.checkpoint_actor",
    "ActorsClient.invoke_actor",
];
/// Semantic checks required by the external Ruby qualification consumer.
pub const REQUIRED_CHECKS: &[&str] = &[
    "all-eight caller-supplied requests",
    "full-u64 cursor 9007199254740993",
    "typed service and contract errors",
    "pending cancellation and native future cleanup",
    "installed package consumer",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RubyGenerationMetadata {
    pub language: &'static str,
    pub uniffi_version: &'static str,
    pub generator_patch_path: &'static str,
    pub generator_patch_sha256: &'static str,
    pub async_future_hooks: &'static [&'static str],
    pub async_exports: &'static [&'static str],
    pub required_checks: &'static [&'static str],
}

pub const fn metadata() -> RubyGenerationMetadata {
    RubyGenerationMetadata {
        language: "ruby",
        uniffi_version: UNIFFI_VERSION,
        generator_patch_path: GENERATOR_PATCH_PATH,
        generator_patch_sha256: GENERATOR_PATCH_SHA256,
        async_future_hooks: ASYNC_FUTURE_HOOKS,
        async_exports: ASYNC_EXPORTS,
        required_checks: REQUIRED_CHECKS,
    }
}

fn json_array(values: &[&str]) -> String {
    let values = values
        .iter()
        .map(|value| format!("\"{value}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{values}]")
}

/// Emit the recipe consumed by the SDK language-package runner.
pub fn render_json() -> String {
    let value = metadata();
    format!(
        "{{\"schema\":\"acyclic.uniffi.language-metadata.v1\",\"language\":\"{}\",\"uniffiVersion\":\"{}\",\"generatorPatchPath\":\"{}\",\"generatorPatchSha256\":\"{}\",\"asyncFutureHooks\":{},\"asyncExports\":{},\"requiredChecks\":{}}}",
        value.language,
        value.uniffi_version,
        value.generator_patch_path,
        value.generator_patch_sha256,
        json_array(value.async_future_hooks),
        json_array(value.async_exports),
        json_array(value.required_checks),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipe_is_generic_and_complete() {
        let value = metadata();
        assert_eq!(value.language, "ruby");
        assert_eq!(value.async_future_hooks.len(), 4);
        assert_eq!(value.async_exports.len(), 11);
        assert!(render_json().contains("ffi_rust_future_cancel"));
        assert!(render_json().contains("9007199254740993"));
    }
}
