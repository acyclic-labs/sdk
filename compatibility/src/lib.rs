//! Build protobuf bindings from each published crate's own protocol sources.

// This workspace-provided input is tracked across every schema build script.
// Cargo-provided manifest variables alone do not invalidate reused outputs.
const _: Option<&str> = option_env!("ACYCLIC_BUILD_SOURCE_ROOT");

use std::path::PathBuf;

mod authorization;
mod compiler;
pub use compiler::Compiler;
mod descriptor;
mod method;
pub use descriptor::canonicalize;
pub use method::{
    authorization, product_authorization, typescript_authorization, write_authorization,
};

pub use tonic_prost_build::{Builder, Config};

/// Descriptor roots and retained wire identities, in original generation order.
pub const FAMILIES: &[(&str, &str, &[&str])] = &[
    ("auth", "auth-v1.bin", &["proto/auth/v1/auth.proto"]),
    ("actors", "actors-v1.bin", &["proto/actors/v1/actors.proto"]),
    (
        "filesystem-wire",
        "filesystem-v2.bin",
        &["proto/filesystem/v2/filesystem.proto"],
    ),
    (
        "inference",
        "inference-v1.bin",
        &["proto/inference/v1/inference.proto"],
    ),
    (
        "machines",
        "machines-v1.bin",
        &["proto/machines/v1/machines.proto"],
    ),
    (
        "objects",
        "objects-v1.bin",
        &["proto/objects/v1/objects.proto"],
    ),
    (
        "stream",
        "stream-v1.bin",
        &[
            "proto/stream/v1/stream.proto",
            "proto/stream/v1/session.proto",
        ],
    ),
    (
        "harness-core",
        "harness-v2.bin",
        &[
            "proto/harness/v2/harness.proto",
            "proto/protocol/v1/protocol.proto",
        ],
    ),
];

impl Compiler {
    /// Configure the compiler and the public Objects metadata map shape.
    #[must_use]
    pub fn config(&self) -> Config {
        let mut config = Config::new();
        config.protoc_executable(self.executable());
        config.btree_map([".acyclic.objects.v1.ObjectMetadata.user"]);
        config.protoc_arg("--retain_options");
        config
    }
}

/// Preserve the public service client/server shapes when transport is enabled.
#[must_use]
pub fn services(enabled: bool) -> Builder {
    tonic_prost_build::configure()
        .build_client(enabled)
        .build_server(enabled)
        .emit_rerun_if_changed(false)
}

/// Actors shares semantic wire aliases and exposes servers only on native hosts.
#[must_use]
pub fn actor_services() -> Builder {
    services(true)
        .build_transport(false)
        .server_mod_attribute(".", "#[cfg(not(target_arch = \"wasm32\"))]")
}

/// Import root for canonical identities shared by independently published services.
#[must_use]
pub fn shared_protocols() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto")
}

impl Compiler {
    /// Compile the caller's explicit source root and selected services.
    /// Build scripts pass their compile-time manifest root so Cargo tracks
    /// relocation as a compiler input, including with a shared target.
    ///
    /// # Errors
    /// Returns filesystem and protobuf compilation errors.
    pub fn compile(
        &self,
        manifest: &std::path::Path,
        mut config: Config,
        services: Builder,
        protos: &[&str],
        descriptor: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let include = self.includes().to_owned();

        let output =
            PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is unset")?).join(descriptor);
        config.file_descriptor_set_path(&output);
        let paths: Vec<_> = protos.iter().map(|path| manifest.join(path)).collect();
        services.compile_with_config(
            config,
            &paths,
            &[manifest.join("proto"), shared_protocols(), include],
        )?;
        std::fs::write(&output, canonicalize(&std::fs::read(&output)?, protos)?)?;
        println!(
            "cargo:rerun-if-changed={}",
            manifest.join("proto").display()
        );
        println!("cargo:rerun-if-changed={}", shared_protocols().display());
        println!("cargo:rerun-if-env-changed=PROTOC");
        println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");
        Ok(())
    }

    /// Regenerate a complete descriptor closure without building any SDK crate.
    /// `shared` is the import root containing the shared validation protocol.
    /// Both source roots are explicit so generators can run after relocation.
    ///
    /// # Errors
    /// Returns compiler, descriptor and filesystem errors.
    pub fn descriptor(
        &self,
        manifest: &std::path::Path,
        protos: &[&str],
        output: &std::path::Path,
        shared: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut config = self.config();
        config.file_descriptor_set_path(output);
        let paths: Vec<_> = protos.iter().map(|path| manifest.join(path)).collect();
        config.load_fds(
            &paths,
            &[
                manifest.join("proto"),
                shared.to_path_buf(),
                self.includes().to_owned(),
            ],
        )?;
        std::fs::write(output, canonicalize(&std::fs::read(output)?, protos)?)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

/// Shared golden-frame lookup for persisted and wire codec tests.
pub mod golden;
