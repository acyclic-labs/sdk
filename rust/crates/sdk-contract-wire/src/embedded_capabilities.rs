//! Rust-owned embedded binding capabilities.
//!
//! This registry records only binding surfaces that exist in the Rust source
//! tree.  An absent `(language, family)` pair is intentionally unsupported;
//! generators must not turn that absence into a generic `ffi-or-wasm` claim.
//! Remote-web WASM is recorded separately from local embedded execution.

use std::collections::BTreeMap;

use serde_json::{Value, json};

/// A language with a first-class embedded binding model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedLanguage {
    Rust,
    C,
    Python,
    TypeScript,
    CSharp,
}

impl EmbeddedLanguage {
    /// Stable manifest spelling used by docs and package tooling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::C => "c",
            Self::Python => "python",
            Self::TypeScript => "typescript",
            Self::CSharp => "csharp",
        }
    }
}

/// A product family that can expose embedded behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedFamily {
    Stream,
    Filesystem,
    Harness,
    Objects,
    Inference,
    Machines,
    Actors,
    Workers,
}

impl EmbeddedFamily {
    /// Stable manifest spelling used by docs and package tooling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Filesystem => "filesystem",
            Self::Harness => "harness",
            Self::Objects => "objects",
            Self::Inference => "inference",
            Self::Machines => "machines",
            Self::Actors => "actors",
            Self::Workers => "workers",
        }
    }
}

/// The boundary used by an embedded binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedBinding {
    /// The Rust crate itself, consumed in-process by Rust applications.
    RustNative,
    /// The stable C ABI emitted by the embedded Rust prototype.
    CAbi,
    /// A UniFFI probe which has not yet become a distributable package.
    UniFfiProbe,
    /// A WebAssembly package for local execution in a JS/WASM host.
    Wasm,
    /// A Rust N-API package and its JavaScript companion.
    Napi,
    /// A platform-specific native companion selected by a TypeScript package.
    TypeScriptNative,
    /// An installable .NET package backed by the embedded native library.
    DotnetNuget,
    /// A WASM package for the remote-web transport, not local embedding.
    RemoteWebWasm,
}

impl EmbeddedBinding {
    /// Stable manifest spelling used by docs and package tooling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RustNative => "rust-native",
            Self::CAbi => "c-abi",
            Self::UniFfiProbe => "uniffi-probe",
            Self::Wasm => "wasm",
            Self::Napi => "n-api",
            Self::TypeScriptNative => "typescript-native",
            Self::DotnetNuget => "dotnet-nuget",
            Self::RemoteWebWasm => "remote-web-wasm",
        }
    }

    /// Whether the binding executes product behavior in the local process.
    pub const fn is_local(self) -> bool {
        !matches!(self, Self::RemoteWebWasm)
    }
}

/// The strongest evidence currently recorded for a binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedCoverage {
    /// Rust source and its native implementation are present.
    RustImplementation,
    /// A native, C ABI, N-API, or UniFFI boundary is emitted by Rust.
    NativeBoundary,
    /// An installable WASM or language package surface is emitted by Rust.
    PackageSurface,
    /// An installed consumer executed against this source revision.
    InstalledConsumer,
}

impl EmbeddedCoverage {
    /// Stable manifest spelling used by docs and package tooling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RustImplementation => "rust-implementation",
            Self::NativeBoundary => "native-boundary",
            Self::PackageSurface => "package-surface",
            Self::InstalledConsumer => "installed-consumer",
        }
    }
}

/// Evidence level for one binding entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedQualification {
    /// The boundary has an executable consumer receipt for this snapshot.
    Verified,
    /// The source and package surface exist, but the complete consumer matrix
    /// is still outstanding.
    SurfaceOnly,
}

impl EmbeddedQualification {
    /// Stable manifest spelling used by docs and qualification tooling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::SurfaceOnly => "surface-only",
        }
    }
}

/// Rust source and qualification evidence for an embedded capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedEvidence {
    pub qualification: EmbeddedQualification,
    pub coverage: EmbeddedCoverage,
    pub source_evidence: &'static [&'static str],
    /// An optional checked-in receipt or fixture path. External machine
    /// receipts stay in the qualification workspace and are never implied by
    /// this registry unless a repository path is recorded here.
    pub receipt: Option<&'static str>,
}

/// The package or artifact identity exposed by an embedded binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedArtifact {
    pub kind: EmbeddedArtifactKind,
    pub identity: &'static str,
    pub installable: bool,
    /// Exact package identities emitted for platform-specific artifacts.
    /// An empty list means the artifact has no platform split.
    pub target_identities: &'static [&'static str],
}

/// Typed artifact categories prevent docs from treating a source crate as an
/// installable package or a remote WASM transport as local embedding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedArtifactKind {
    CargoCrate,
    NativeLibrary,
    UniFfiModule,
    WasmPackage,
    NapiPackage,
    TypeScriptNativePackage,
    DotnetNuget,
    RemoteWebWasmPackage,
}

impl EmbeddedArtifactKind {
    /// Stable manifest spelling used by docs and package tooling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CargoCrate => "cargo-crate",
            Self::NativeLibrary => "native-library",
            Self::UniFfiModule => "uniffi-module",
            Self::WasmPackage => "wasm-package",
            Self::NapiPackage => "n-api-package",
            Self::TypeScriptNativePackage => "typescript-native-package",
            Self::DotnetNuget => "dotnet-nuget",
            Self::RemoteWebWasmPackage => "remote-web-wasm-package",
        }
    }
}

/// One Rust-owned embedded binding capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedCapability {
    /// Consumer language or host ecosystem.
    pub language: EmbeddedLanguage,
    /// Contract family exposed by the binding.
    pub family: EmbeddedFamily,
    /// Native, WASM, or package boundary.
    pub binding: EmbeddedBinding,
    /// Crate, package, or generated artifact identity and installability.
    pub artifact: EmbeddedArtifact,
    /// Qualification, coverage, and source evidence for this exact surface.
    pub evidence: EmbeddedEvidence,
}

const STREAM_NATIVE_SOURCES: &[&str] = &[
    "rust/crates/sdk-embedded-prototype/Cargo.toml",
    "rust/crates/sdk-embedded-prototype/Cargo.lock",
    "rust/crates/sdk-embedded-prototype/build.rs",
    "rust/crates/sdk-embedded-prototype/src/lib.rs",
    "rust/crates/sdk-embedded-prototype/tests/c_consumer.c",
    "rust/crates/sdk-embedded-prototype/tests/python_consumer.py",
    "scripts/build-stream-native-package.mjs",
    "rust/crates/sdk-stream-native/npm",
];
const STREAM_PACKAGE_SOURCES: &[&str] = &[
    "rust/crates/stream-wasm/Cargo.toml",
    "typescript/packages/stream/package.json",
    "typescript/packages/stream/scripts/build-wasm.mjs",
    "scripts/build-stream-native-package.mjs",
    "rust/crates/sdk-stream-native/npm",
];
const FILESYSTEM_SOURCES: &[&str] = &[
    "rust/crates/sdk-embedded-filesystem/Cargo.toml",
    "rust/crates/filesystem-wasm/Cargo.toml",
    "rust/crates/filesystem-napi/Cargo.toml",
    "rust/crates/filesystem-napi/build.rs",
    "rust/crates/filesystem-napi/src/lib.rs",
    "rust/crates/sdk-filesystem-native/npm",
    "scripts/build-filesystem-wasm.mjs",
    "scripts/check-filesystem-napi.mjs",
    "typescript/packages/filesystem/src/native.ts",
    "typescript/packages/filesystem/package.json",
];
const HARNESS_SOURCES: &[&str] = &[
    "rust/crates/sdk-embedded-filesystem/Cargo.toml",
    "rust/crates/harness/src/wasm.rs",
    "scripts/build-harness-wasm.mjs",
    "typescript/packages/harness/package.json",
];
const OBJECTS_SOURCES: &[&str] = &[
    "rust/crates/objects-wasm/Cargo.toml",
    "typescript/packages/objects/package.json",
];
const INFERENCE_SOURCES: &[&str] = &[
    "rust/crates/inference-wasm/Cargo.toml",
    "typescript/packages/inference/package.json",
];
const MACHINES_SOURCES: &[&str] = &[
    "rust/crates/machines-wasm/Cargo.toml",
    "typescript/packages/machines/package.json",
];
const REMOTE_WEB_SOURCES: &[&str] = &[
    "typescript/packages/actors/package.json",
    "typescript/packages/workers/package.json",
    "scripts/build-remote-web-wasm.mjs",
];

const NO_TARGET_IDENTITIES: &[&str] = &[];
const STREAM_NATIVE_TARGET_IDENTITIES: &[&str] = &[
    "@acyclic-labs/stream-darwin-arm64",
    "@acyclic-labs/stream-darwin-x64",
    "@acyclic-labs/stream-linux-arm64-gnu",
    "@acyclic-labs/stream-linux-arm64-musl",
    "@acyclic-labs/stream-linux-x64-gnu",
    "@acyclic-labs/stream-linux-x64-musl",
    "@acyclic-labs/stream-win32-arm64",
    "@acyclic-labs/stream-win32-x64",
];
const FILESYSTEM_NAPI_TARGET_IDENTITIES: &[&str] = &[
    "@acyclic-labs/fs-darwin-arm64",
    "@acyclic-labs/fs-darwin-x64",
    "@acyclic-labs/fs-linux-arm64",
    "@acyclic-labs/fs-linux-x64",
    "@acyclic-labs/fs-win32-arm64",
    "@acyclic-labs/fs-win32-x64",
];

/// The current, deliberately conservative embedded capability registry.
///
/// The list is a projection of Rust-owned source and package declarations. It
/// does not imply that every entry has completed the full release matrix.
pub const EMBEDDED_CAPABILITIES: &[EmbeddedCapability] = &[
    EmbeddedCapability {
        language: EmbeddedLanguage::Rust,
        family: EmbeddedFamily::Stream,
        binding: EmbeddedBinding::RustNative,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::CargoCrate,
            identity: "rust/crates/sdk-embedded-prototype",
            installable: false,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::RustImplementation,
            source_evidence: STREAM_NATIVE_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::C,
        family: EmbeddedFamily::Stream,
        binding: EmbeddedBinding::CAbi,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::NativeLibrary,
            identity: "acyclic_embedded_prototype.h + native library",
            installable: false,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::NativeBoundary,
            source_evidence: STREAM_NATIVE_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::Python,
        family: EmbeddedFamily::Stream,
        binding: EmbeddedBinding::CAbi,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::NativeLibrary,
            identity: "acyclic_embedded_prototype shared library (ctypes C ABI)",
            installable: false,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::NativeBoundary,
            source_evidence: STREAM_NATIVE_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Stream,
        binding: EmbeddedBinding::Wasm,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::WasmPackage,
            identity: "@acyclic-labs/stream",
            installable: true,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: STREAM_PACKAGE_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Stream,
        binding: EmbeddedBinding::TypeScriptNative,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::TypeScriptNativePackage,
            identity: "@acyclic-labs/stream",
            installable: true,
            target_identities: STREAM_NATIVE_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: STREAM_PACKAGE_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::Rust,
        family: EmbeddedFamily::Filesystem,
        binding: EmbeddedBinding::RustNative,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::CargoCrate,
            identity: "rust/crates/sdk-embedded-filesystem",
            installable: false,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::RustImplementation,
            source_evidence: FILESYSTEM_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Filesystem,
        binding: EmbeddedBinding::Wasm,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::WasmPackage,
            identity: "@acyclic-labs/fs",
            installable: true,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: FILESYSTEM_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Filesystem,
        binding: EmbeddedBinding::Napi,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::NapiPackage,
            identity: "@acyclic-labs/fs",
            installable: true,
            target_identities: FILESYSTEM_NAPI_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::NativeBoundary,
            source_evidence: FILESYSTEM_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::Rust,
        family: EmbeddedFamily::Harness,
        binding: EmbeddedBinding::RustNative,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::CargoCrate,
            identity: "rust/crates/sdk-embedded-filesystem (harness feature)",
            installable: false,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::RustImplementation,
            source_evidence: HARNESS_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Harness,
        binding: EmbeddedBinding::Wasm,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::WasmPackage,
            identity: "@acyclic-labs/harness",
            installable: true,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: HARNESS_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Objects,
        binding: EmbeddedBinding::Wasm,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::WasmPackage,
            identity: "@acyclic-labs/objects",
            installable: true,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: OBJECTS_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Inference,
        binding: EmbeddedBinding::Wasm,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::WasmPackage,
            identity: "@acyclic-labs/inference",
            installable: true,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: INFERENCE_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Machines,
        binding: EmbeddedBinding::Wasm,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::WasmPackage,
            identity: "@acyclic-labs/machines",
            installable: true,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: MACHINES_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Actors,
        binding: EmbeddedBinding::RemoteWebWasm,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::RemoteWebWasmPackage,
            identity: "@acyclic-labs/actors remote-web WASM",
            installable: true,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: REMOTE_WEB_SOURCES,
            receipt: None,
        },
    },
    EmbeddedCapability {
        language: EmbeddedLanguage::TypeScript,
        family: EmbeddedFamily::Workers,
        binding: EmbeddedBinding::RemoteWebWasm,
        artifact: EmbeddedArtifact {
            kind: EmbeddedArtifactKind::RemoteWebWasmPackage,
            identity: "@acyclic-labs/workers remote-web WASM",
            installable: true,
            target_identities: NO_TARGET_IDENTITIES,
        },
        evidence: EmbeddedEvidence {
            qualification: EmbeddedQualification::SurfaceOnly,
            coverage: EmbeddedCoverage::PackageSurface,
            source_evidence: REMOTE_WEB_SOURCES,
            receipt: None,
        },
    },
];

/// Return all Rust-owned capability entries.
pub const fn embedded_capabilities() -> &'static [EmbeddedCapability] {
    EMBEDDED_CAPABILITIES
}

/// Find one exact language/family capability when that pair has one binding.
///
/// Ambiguous pairs return `None`; callers that need to select among multiple
/// Rust-declared bindings must use [`embedded_capabilities_for`] or
/// [`embedded_capability_with_binding`].  This prevents the old first-entry
/// behavior from silently hiding a valid alternative.
pub fn embedded_capability(language: &str, family: &str) -> Option<&'static EmbeddedCapability> {
    let mut matches = embedded_capabilities_for(language, family).into_iter();
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

/// Return every Rust-declared binding for an exact language/family pair.
pub fn embedded_capabilities_for(
    language: &str,
    family: &str,
) -> Vec<&'static EmbeddedCapability> {
    EMBEDDED_CAPABILITIES
        .iter()
        .filter(|entry| entry.language.as_str() == language && entry.family.as_str() == family)
        .collect()
}

/// Find one explicitly selected Rust-declared binding for a language/family pair.
pub fn embedded_capability_with_binding(
    language: &str,
    family: &str,
    binding: EmbeddedBinding,
) -> Option<&'static EmbeddedCapability> {
    EMBEDDED_CAPABILITIES.iter().find(|entry| {
        entry.language.as_str() == language
            && entry.family.as_str() == family
            && entry.binding == binding
    })
}

/// Project the registry into the deterministic JSON consumed by docs and
/// package qualification tooling.
pub fn embedded_capabilities_json() -> Value {
    json!({
        "schema": "acyclic.sdk.embedded-capabilities.v1",
        "authority": "rust",
        "capabilities": EMBEDDED_CAPABILITIES.iter().map(|entry| json!({
            "language": entry.language.as_str(),
            "family": entry.family.as_str(),
            "binding": entry.binding.as_str(),
            "artifact": {
                "kind": entry.artifact.kind.as_str(),
                "identity": entry.artifact.identity,
                "installable": entry.artifact.installable,
                "target_identities": entry.artifact.target_identities,
            },
            "installable": entry.artifact.installable,
            "qualification": entry.evidence.qualification.as_str(),
            "coverage": entry.evidence.coverage.as_str(),
            "source_evidence": entry.evidence.source_evidence,
            "receipt": entry.evidence.receipt,
        })).collect::<Vec<_>>(),
    })
}

/// Project the same typed registry into the family-oriented table consumed by
/// generated reference pages.  The website can render this table directly:
/// every row retains its Rust language, boundary, artifact identity,
/// installability, coverage tier, and qualification evidence.  No family or
/// package row is authored in a presentation language.
pub fn embedded_family_table_json() -> Value {
    let mut families = BTreeMap::<&str, Vec<Value>>::new();
    for entry in EMBEDDED_CAPABILITIES {
        families
            .entry(entry.family.as_str())
            .or_default()
            .push(json!({
                "language": entry.language.as_str(),
                "binding": entry.binding.as_str(),
                "local": entry.binding.is_local(),
                "artifact": {
                    "kind": entry.artifact.kind.as_str(),
                    "identity": entry.artifact.identity,
                    "installable": entry.artifact.installable,
                    "targetIdentities": entry.artifact.target_identities,
                },
                "qualification": entry.evidence.qualification.as_str(),
                "coverage": entry.evidence.coverage.as_str(),
                "sourceEvidence": entry.evidence.source_evidence,
                "receipt": entry.evidence.receipt,
            }));
    }

    json!({
        "schema": "acyclic.sdk.embedded-family-table.v1",
        "authority": "rust",
        "families": families.into_iter().map(|(family, capabilities)| {
            json!({
                "family": family,
                "capabilities": capabilities,
            })
        }).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_no_generic_unqualified_binding() {
        assert!(EMBEDDED_CAPABILITIES.iter().all(|entry| {
            !entry.artifact.identity.contains("ffi-or-wasm")
                && !entry.artifact.identity.contains("unqualified")
                && !entry.evidence.source_evidence.is_empty()
        }));
    }

    #[test]
    fn remote_web_is_distinct_from_local_wasm() {
        let actors = embedded_capability("typescript", "actors").unwrap();
        assert_eq!(actors.binding, EmbeddedBinding::RemoteWebWasm);
        assert!(embedded_capability("typescript", "stream").is_none());
        let stream = embedded_capabilities_for("typescript", "stream");
        assert_eq!(stream.len(), 2);
        assert_eq!(
            stream.first().map(|entry| entry.binding),
            Some(EmbeddedBinding::Wasm)
        );
        assert_eq!(
            stream.get(1).map(|entry| entry.binding),
            Some(EmbeddedBinding::TypeScriptNative)
        );
        assert_eq!(
            embedded_capability_with_binding("typescript", "stream", EmbeddedBinding::Wasm)
                .unwrap()
                .binding,
            EmbeddedBinding::Wasm
        );
        assert!(!actors.binding.is_local());
        assert!(stream.iter().all(|entry| entry.binding.is_local()));
    }

    #[test]
    fn absent_binding_is_explicitly_unsupported() {
        assert!(embedded_capability("swift", "stream").is_none());
        assert!(embedded_capability("cpp", "filesystem").is_none());
    }

    #[test]
    fn json_projection_is_rust_authoritative() {
        let manifest = embedded_capabilities_json();
        assert_eq!(manifest["schema"], "acyclic.sdk.embedded-capabilities.v1");
        assert_eq!(manifest["authority"], "rust");
        assert_eq!(
            manifest["capabilities"].as_array().unwrap().len(),
            EMBEDDED_CAPABILITIES.len()
        );
        assert_eq!(manifest["capabilities"][0]["language"], "rust");
        assert_eq!(
            manifest["capabilities"][0]["artifact"]["kind"],
            "cargo-crate"
        );
        assert_eq!(
            manifest["capabilities"][0]["coverage"],
            "rust-implementation"
        );
        assert_eq!(
            manifest["capabilities"][4]["artifact"]["target_identities"]
                .as_array()
                .unwrap()
                .len(),
            8
        );
    }

    #[test]
    fn family_table_is_grouped_and_preserves_package_evidence() {
        let table = embedded_family_table_json();
        assert_eq!(table["schema"], "acyclic.sdk.embedded-family-table.v1");
        assert_eq!(table["authority"], "rust");
        let families = table["families"].as_array().unwrap();
        assert_eq!(families.len(), 8);
        assert_eq!(families[0]["family"], "actors");
        let stream = families
            .iter()
            .find(|family| family["family"] == "stream")
            .unwrap();
        let typescript = stream["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["language"] == "typescript")
            .collect::<Vec<_>>();
        assert_eq!(typescript.len(), 2);
        assert!(typescript.iter().any(|row| row["binding"] == "wasm"));
        assert!(
            typescript
                .iter()
                .any(|row| row["binding"] == "typescript-native")
        );
        assert!(
            stream["capabilities"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| {
                    row["artifact"]["identity"] == "@acyclic-labs/stream"
                        && row["artifact"]["installable"] == true
                        && row["coverage"] == "package-surface"
                })
        );
        let actors = families
            .iter()
            .find(|family| family["family"] == "actors")
            .unwrap();
        assert!(
            actors["capabilities"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["local"] == false)
        );
    }

    #[test]
    fn typed_entries_keep_artifact_and_evidence_consistent() {
        for entry in EMBEDDED_CAPABILITIES {
            assert!(!entry.artifact.identity.is_empty());
            assert!(!entry.evidence.source_evidence.is_empty());
            assert_eq!(
                entry.binding == EmbeddedBinding::RemoteWebWasm,
                !entry.binding.is_local()
            );
            if entry.evidence.qualification == EmbeddedQualification::Verified {
                assert_eq!(
                    entry.evidence.coverage,
                    EmbeddedCoverage::InstalledConsumer,
                    "verified entries require an installed consumer receipt"
                );
                assert!(
                    entry.evidence.receipt.is_some(),
                    "verified entries require an exact checked-in receipt"
                );
            } else {
                assert_ne!(
                    entry.evidence.coverage,
                    EmbeddedCoverage::InstalledConsumer,
                    "installed consumer coverage requires verified receipt"
                );
            }
        }

        let stream_native = EMBEDDED_CAPABILITIES
            .iter()
            .find(|entry| entry.binding == EmbeddedBinding::TypeScriptNative)
            .unwrap();
        assert_eq!(stream_native.artifact.target_identities.len(), 8);
        let filesystem_napi = EMBEDDED_CAPABILITIES
            .iter()
            .find(|entry| entry.binding == EmbeddedBinding::Napi)
            .unwrap();
        assert_eq!(filesystem_napi.artifact.target_identities.len(), 6);
        assert!(!EMBEDDED_CAPABILITIES
            .iter()
            .any(|entry| entry.language == EmbeddedLanguage::CSharp));
    }
}
