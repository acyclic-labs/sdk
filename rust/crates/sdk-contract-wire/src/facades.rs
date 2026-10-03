//! Rust-owned remote facade output for the portable Ruby, PHP, and Dart SDKs.
//!
//! The generated files contain the transport matrix and its source binding.
//! Runtime adapters remain target-language code: a generated facade must pass
//! installed and endpoint availability through the Rust policy before invoking
//! a wire adapter.

use sha2::{Digest, Sha256};

use crate::{
    family_registry::FAMILY_VIEWS,
    transport::{ClientRuntime, TransportKind, TransportOption},
};

/// Cancellation semantics that a generated facade must preserve per RPC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationKind {
    /// A unary operation has no modeled cancellation operation.
    None,
    /// The target runtime can cancel the in-flight RPC call.
    Call,
    /// The contract exposes an explicit `Cancel` RPC.
    Operation,
}

impl CancellationKind {
    const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Call => "call",
            Self::Operation => "operation",
        }
    }
}

/// Rust-owned operation metadata emitted into portable language facades.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FacadeOperationPolicy {
    /// Stable fully-qualified RPC identity from the Rust model.
    pub rpc: &'static str,
    /// Whether the request is client streaming.
    pub client_streaming: bool,
    /// Whether the response is server streaming.
    pub server_streaming: bool,
    /// Whether the shared bearer credential is required.
    pub bearer_auth: bool,
    /// Cancellation behavior for this operation.
    pub cancellation: CancellationKind,
    /// Rust-owned capability requirements.
    pub capabilities: &'static [&'static str],
    /// Rust-owned error identities.
    pub errors: &'static [&'static str],
    /// Rust-owned request validation identities.
    pub validations: &'static [&'static str],
}

/// Selection safety policy emitted into every portable facade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FacadeSelectionPolicy {
    /// Selection never probes a mutating operation.
    pub probe: &'static str,
    /// A failed call never triggers a transport fallback.
    pub post_failure_fallback: &'static str,
    /// A failed operation is never replayed by transport selection.
    pub replay: &'static str,
}

/// Shared Rust-owned selection safety policy.
pub const FACADE_SELECTION_POLICY: FacadeSelectionPolicy = FacadeSelectionPolicy {
    probe: "none",
    post_failure_fallback: "none",
    replay: "none",
};

/// Target language for a generated policy facade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FacadeLanguage {
    /// Ruby source consumed by the gem adapter.
    Ruby,
    /// PHP source consumed by the Composer adapter.
    Php,
    /// Dart source consumed by the pub package.
    Dart,
}

impl FacadeLanguage {
    /// Stable target name used in generated paths and provenance.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ruby => "ruby",
            Self::Php => "php",
            Self::Dart => "dart",
        }
    }

    /// Generated source path relative to the Rust exporter root.
    pub const fn output_path(self) -> &'static str {
        match self {
            Self::Ruby => "generated/facades/ruby/remote_policy.generated.rb",
            Self::Php => "generated/facades/php/RemotePolicy.generated.php",
            Self::Dart => "generated/facades/dart/remote_policy.generated.dart",
        }
    }
}

/// Generated facade source and its Rust model binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FacadeOutput {
    /// Target language selected for this output.
    pub language: FacadeLanguage,
    /// Relative generated output path.
    pub path: &'static str,
    /// SHA-256 binding over the Rust policy source files.
    pub source_binding: String,
    /// Target-language source text.
    pub source: String,
}

/// Generate all portable policy facade outputs from the Rust family registry.
pub fn generate_remote_facades() -> Vec<FacadeOutput> {
    [
        FacadeLanguage::Ruby,
        FacadeLanguage::Php,
        FacadeLanguage::Dart,
    ]
    .into_iter()
    .map(generate_remote_facade)
    .collect()
}

/// Generate one portable policy facade from the Rust family registry.
pub fn generate_remote_facade(language: FacadeLanguage) -> FacadeOutput {
    let source_binding = rust_policy_source_binding();
    let source = match language {
        FacadeLanguage::Ruby => render_ruby(&source_binding),
        FacadeLanguage::Php => render_php(&source_binding),
        FacadeLanguage::Dart => render_dart(&source_binding),
    };
    FacadeOutput {
        language,
        path: language.output_path(),
        source_binding,
        source,
    }
}

/// Return operation policies for every family in the unified Rust registry.
pub fn all_facade_operations() -> Vec<FacadeOperationPolicy> {
    FAMILY_VIEWS.iter().flat_map(facade_operations).collect()
}

/// Return descriptor-linked operation policies for one Rust-owned family.
pub fn facade_operations(
    family: &'static crate::family_registry::FamilyView,
) -> Vec<FacadeOperationPolicy> {
    family
        .operation_policies
        .iter()
        .map(|policy| {
            let (client_streaming, server_streaming) = method_streaming(family, policy.rpc)
                .unwrap_or_else(|| {
                    panic!(
                        "Rust operation policy has no descriptor method: {}",
                        policy.rpc
                    )
                });
            let cancellation = if policy.rpc.ends_with("/Cancel") {
                CancellationKind::Operation
            } else if client_streaming || server_streaming {
                CancellationKind::Call
            } else {
                CancellationKind::None
            };
            FacadeOperationPolicy {
                rpc: policy.rpc,
                client_streaming,
                server_streaming,
                bearer_auth: true,
                cancellation,
                capabilities: policy.capabilities,
                errors: policy.errors,
                validations: policy.validations,
            }
        })
        .collect()
}

fn method_streaming(
    family: &'static crate::family_registry::FamilyView,
    rpc: &str,
) -> Option<(bool, bool)> {
    let (service_path, method_name) = rpc.rsplit_once('/')?;
    match family.model {
        crate::family_registry::FamilyModel::ContractSpec(spec) => spec
            .services
            .iter()
            .find(|service| format!("{}.{}", spec.package, service.name) == service_path)
            .and_then(|service| {
                service
                    .methods
                    .iter()
                    .find(|method| method.name == method_name)
            })
            .map(|method| (method.client_streaming, method.server_streaming)),
        crate::family_registry::FamilyModel::Filesystem(model) => {
            descriptor_method_streaming(&model.file_descriptor(), service_path, method_name)
        }
        crate::family_registry::FamilyModel::Harness(model) => {
            descriptor_method_streaming(&model.file_descriptor(), service_path, method_name)
        }
    }
}

fn descriptor_method_streaming(
    file: &prost_types::FileDescriptorProto,
    service_path: &str,
    method_name: &str,
) -> Option<(bool, bool)> {
    let package = file.package.as_deref().unwrap_or_default();
    file.service
        .iter()
        .find(|service| {
            format!(
                "{}.{}",
                package,
                service.name.as_deref().unwrap_or_default()
            ) == service_path
        })
        .and_then(|service| {
            service
                .method
                .iter()
                .find(|method| method.name.as_deref() == Some(method_name))
        })
        .map(|method| {
            (
                method.client_streaming.unwrap_or(false),
                method.server_streaming.unwrap_or(false),
            )
        })
}

fn rust_policy_source_binding() -> String {
    let mut digest = Sha256::new();
    for source in [
        include_bytes!("facades.rs").as_slice(),
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("transport.rs").as_slice(),
        include_bytes!("family_registry.rs").as_slice(),
        include_bytes!("credential.rs").as_slice(),
        include_bytes!("workers.rs").as_slice(),
        include_bytes!("objects.rs").as_slice(),
        include_bytes!("stream.rs").as_slice(),
        include_bytes!("inference.rs").as_slice(),
        include_bytes!("machines.rs").as_slice(),
        include_bytes!("filesystem.rs").as_slice(),
        include_bytes!("harness.rs").as_slice(),
        include_bytes!("protocol.rs").as_slice(),
    ] {
        digest.update(source);
    }
    format!("{:X}", digest.finalize())
}

fn options(
    runtime: ClientRuntime,
) -> impl Iterator<Item = (&'static str, &'static [TransportOption])> {
    FAMILY_VIEWS
        .iter()
        .map(move |family| (family.name, family.transport.for_runtime(runtime).options))
}

fn transport_name(kind: TransportKind) -> &'static str {
    match kind {
        TransportKind::Grpc => "grpc",
        TransportKind::GrpcWeb => "grpc_web",
        TransportKind::HttpJson => "http_json",
    }
}

fn render_ruby(binding: &str) -> String {
    let mut output = format!(
        "# Generated by acyclic-sdk-contract-wire; do not edit.\n# rust_policy_source_binding: {binding}\n\nmodule Acyclic\n  module Remote\n    module GeneratedPolicy\n      SOURCE_BINDING = {binding:?}.freeze\n      OPTIONS = {{\n"
    );
    for (runtime_name, runtime) in [
        ("native", ClientRuntime::Native),
        ("browser", ClientRuntime::Browser),
    ] {
        output.push_str(&format!("        {runtime_name}: {{\n"));
        for (family, family_options) in options(runtime) {
            output.push_str(&format!("          {family:?} => ["));
            for (index, option) in family_options.iter().enumerate() {
                if index != 0 {
                    output.push_str(", ");
                }
                output.push_str(&format!(
                    "[{:?}, {}, {}]",
                    transport_name(option.kind),
                    option.streaming,
                    option.bearer_auth
                ));
            }
            output.push_str("],\n");
        }
        output.push_str("        },\n");
    }
    output.push_str("      }.freeze\n\n");
    output.push_str(&render_ruby_operations());
    output.push_str(
        r#"      ALL = { "grpc" => true, "grpc_web" => true, "http_json" => true }.freeze

      module_function

      def select(family:, runtime: :native, streaming: false, bearer_auth: true,
                 installed: nil, endpoint: nil, override: nil)
        family_options = OPTIONS.fetch(runtime.to_sym) { raise ArgumentError, "unknown client runtime: #{runtime}" }
                                   .fetch(family.to_s) { raise ArgumentError, "unknown transport family or runtime: #{family}/#{runtime}" }
        installed ||= ALL
        endpoint ||= ALL
        compatible = family_options.select do |kind, supports_streaming, supports_bearer|
          installed.fetch(kind, false) && endpoint.fetch(kind, false) &&
            (!streaming || supports_streaming) && (!bearer_auth || supports_bearer)
        end
        if override
          selected = compatible.find { |kind, _streaming, _bearer| kind == override.to_s }
          return selected.first if selected
          raise ArgumentError, "unsupported transport override #{override} for #{family}/#{runtime}"
        end
        raise ArgumentError, "no compatible transport for #{family}/#{runtime}" if compatible.empty?

        compatible.first.first
      end

      def validate_bearer(token)
        value = String(token)
        raise ArgumentError, "invalid bearer credential" if value.strip.empty? || value.match?(/[\r\n]/)

        value
      end
    end
  end
end
"#,
    );
    output
}

fn render_ruby_operations() -> String {
    let mut output = format!(
        "      SELECTION_POLICY = {{ probe: {:?}, post_failure_fallback: {:?}, replay: {:?} }}.freeze\n      OPERATIONS = {{\n",
        FACADE_SELECTION_POLICY.probe,
        FACADE_SELECTION_POLICY.post_failure_fallback,
        FACADE_SELECTION_POLICY.replay,
    );
    for family in FAMILY_VIEWS {
        output.push_str(&format!("        {:?} => {{\n", family.name));
        for operation in facade_operations(family) {
            output.push_str(&format!(
                "          {:?} => {{ client_streaming: {}, server_streaming: {}, bearer_auth: {}, cancellation: {:?}, capabilities: {:?}, errors: {:?}, validations: {:?} }},\n",
                operation.rpc,
                operation.client_streaming,
                operation.server_streaming,
                operation.bearer_auth,
                operation.cancellation.name(),
                operation.capabilities,
                operation.errors,
                operation.validations,
            ));
        }
        output.push_str("        },\n");
    }
    output.push_str("      }.freeze\n\n");
    output
}

fn render_php(binding: &str) -> String {
    let mut output = format!(
        "<?php\n\n// Generated by acyclic-sdk-contract-wire; do not edit.\n// rust_policy_source_binding: {binding}\n\nnamespace Acyclic\\Runtime;\n\nfinal class GeneratedRemotePolicy\n{{\n    public const SOURCE_BINDING = {binding:?};\n    public const OPTIONS = [\n"
    );
    for (runtime_name, runtime) in [
        ("native", ClientRuntime::Native),
        ("browser", ClientRuntime::Browser),
    ] {
        output.push_str(&format!("        '{runtime_name}' => [\n"));
        for (family, family_options) in options(runtime) {
            output.push_str(&format!("            '{family}' => ["));
            for (index, option) in family_options.iter().enumerate() {
                if index != 0 {
                    output.push_str(", ");
                }
                output.push_str(&format!(
                    "['kind' => '{}', 'streaming' => {}, 'bearer_auth' => {}]",
                    transport_name(option.kind),
                    option.streaming,
                    option.bearer_auth
                ));
            }
            output.push_str("],\n");
        }
        output.push_str("        ],\n");
    }
    output.push_str("    ];\n\n");
    output.push_str(&render_php_operations());
    output.push_str(
        r#"    private const ALL = ['grpc' => true, 'grpc_web' => true, 'http_json' => true];

    public static function select(
        string $family,
        bool $streaming = false,
        string $runtime = 'native',
        bool $bearerAuth = true,
        ?array $installed = null,
        ?array $endpoint = null,
        ?string $override = null,
    ): string {
        $familyOptions = self::OPTIONS[$runtime][$family] ?? throw new \InvalidArgumentException("unknown transport family or runtime: {$family}/{$runtime}");
        $installed ??= self::ALL;
        $endpoint ??= self::ALL;
        $compatible = array_values(array_filter(
            $familyOptions,
            static fn (array $option): bool => ($installed[$option['kind']] ?? false)
                && ($endpoint[$option['kind']] ?? false)
                && (!$streaming || $option['streaming'])
                && (!$bearerAuth || $option['bearer_auth']),
        ));
        if ($override !== null) {
            foreach ($compatible as $option) {
                if ($option['kind'] === $override) {
                    return $override;
                }
            }
            throw new \InvalidArgumentException("unsupported transport override {$override} for {$family}/{$runtime}");
        }
        if ($compatible === []) {
            throw new \InvalidArgumentException("no compatible transport for {$family}/{$runtime}");
        }
        return $compatible[0]['kind'];
    }

    public static function validateBearer(string $token): string
    {
        if (trim($token) === '' || preg_match('/[\r\n]/', $token) === 1) {
            throw new \InvalidArgumentException('invalid bearer credential');
        }
        return $token;
    }
}
"#,
    );
    output
}

fn render_php_operations() -> String {
    let mut output = format!(
        "    public const SELECTION_POLICY = ['probe' => {:?}, 'post_failure_fallback' => {:?}, 'replay' => {:?}];\n    public const OPERATIONS = [\n",
        FACADE_SELECTION_POLICY.probe,
        FACADE_SELECTION_POLICY.post_failure_fallback,
        FACADE_SELECTION_POLICY.replay,
    );
    for family in FAMILY_VIEWS {
        output.push_str(&format!("        '{}' => [\n", family.name));
        for operation in facade_operations(family) {
            output.push_str(&format!(
                "            '{}' => ['client_streaming' => {}, 'server_streaming' => {}, 'bearer_auth' => {}, 'cancellation' => '{}', 'capabilities' => {}, 'errors' => {}, 'validations' => {}],\n",
                operation.rpc,
                operation.client_streaming,
                operation.server_streaming,
                operation.bearer_auth,
                operation.cancellation.name(),
                php_strings(operation.capabilities),
                php_strings(operation.errors),
                php_strings(operation.validations),
            ));
        }
        output.push_str("        ],\n");
    }
    output.push_str("    ];\n\n");
    output
}

fn php_strings(values: &[&str]) -> String {
    let values = values
        .iter()
        .map(|value| format!("{:?}", value))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

fn render_dart(binding: &str) -> String {
    let mut output = format!(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// rust_policy_source_binding: {binding}\n\nconst generatedRemotePolicySourceBinding = {binding:?};\n\nenum GeneratedRemoteTransport {{ grpc, grpcWeb, httpJson }}\nenum GeneratedClientRuntime {{ native, browser }}\n\nconst generatedRemotePolicyOptions = <GeneratedClientRuntime, Map<String, List<(GeneratedRemoteTransport, bool, bool)>>>{{\n"
    );
    for (runtime_name, runtime) in [
        ("native", ClientRuntime::Native),
        ("browser", ClientRuntime::Browser),
    ] {
        output.push_str(&format!("  GeneratedClientRuntime.{runtime_name}: {{\n"));
        for (family, family_options) in options(runtime) {
            output.push_str(&format!("    '{family}': ["));
            for (index, option) in family_options.iter().enumerate() {
                if index != 0 {
                    output.push_str(", ");
                }
                output.push_str(&format!(
                    "(GeneratedRemoteTransport.{}, {}, {})",
                    dart_transport_name(option.kind),
                    option.streaming,
                    option.bearer_auth
                ));
            }
            output.push_str("],\n");
        }
        output.push_str("  },\n");
    }
    output.push_str("};\n\n");
    output.push_str(&render_dart_operations());
    output.push_str(
        r#"const generatedRemoteTransportAvailability = <GeneratedRemoteTransport, bool>{
  GeneratedRemoteTransport.grpc: true,
  GeneratedRemoteTransport.grpcWeb: true,
  GeneratedRemoteTransport.httpJson: true,
};

class GeneratedRemotePolicy {
  static GeneratedRemoteTransport select({
    required String family,
    GeneratedClientRuntime runtime = GeneratedClientRuntime.native,
    bool streaming = false,
    bool bearerAuth = true,
    Map<GeneratedRemoteTransport, bool>? installed,
    Map<GeneratedRemoteTransport, bool>? endpoint,
    GeneratedRemoteTransport? transportOverride,
  }) {
    final familyOptions = generatedRemotePolicyOptions[runtime]?[family];
    if (familyOptions == null) {
      throw ArgumentError('unknown transport family or runtime: $family/$runtime');
    }
    final installedAvailability = installed ?? generatedRemoteTransportAvailability;
    final endpointAvailability = endpoint ?? generatedRemoteTransportAvailability;
    final compatible = familyOptions.where((option) =>
        (installedAvailability[option.$1] ?? false) &&
        (endpointAvailability[option.$1] ?? false) &&
        (!streaming || option.$2) &&
        (!bearerAuth || option.$3)).toList();
    if (transportOverride != null) {
      if (compatible.any((option) => option.$1 == transportOverride)) return transportOverride;
      throw ArgumentError('unsupported transport override for $family/$runtime');
    }
    if (compatible.isEmpty) throw ArgumentError('no compatible transport for $family/$runtime');
    return compatible.first.$1;
  }

  static String validateBearer(String token) {
    if (token.trim().isEmpty || token.contains(RegExp(r'[\r\n]'))) {
      throw ArgumentError('invalid bearer credential');
    }
    return token;
  }
}
"#,
    );
    output
}

fn dart_transport_name(kind: TransportKind) -> &'static str {
    match kind {
        TransportKind::Grpc => "grpc",
        TransportKind::GrpcWeb => "grpcWeb",
        TransportKind::HttpJson => "httpJson",
    }
}

fn render_dart_operations() -> String {
    let mut output = format!(
        "const generatedRemoteSelectionPolicy = <String, String>{{'probe': {:?}, 'postFailureFallback': {:?}, 'replay': {:?}}};\n\nconst generatedRemoteOperations = <String, Map<String, Map<String, Object?>>>{{\n",
        FACADE_SELECTION_POLICY.probe,
        FACADE_SELECTION_POLICY.post_failure_fallback,
        FACADE_SELECTION_POLICY.replay,
    );
    for family in FAMILY_VIEWS {
        output.push_str(&format!("  {:?}: {{\n", family.name));
        for operation in facade_operations(family) {
            output.push_str(&format!(
                "    {:?}: <String, Object?>{{'clientStreaming': {}, 'serverStreaming': {}, 'bearerAuth': {}, 'cancellation': {:?}, 'capabilities': <String>{:?}, 'errors': <String>{:?}, 'validations': <String>{:?}}},\n",
                operation.rpc,
                operation.client_streaming,
                operation.server_streaming,
                operation.bearer_auth,
                operation.cancellation.name(),
                operation.capabilities,
                operation.errors,
                operation.validations,
            ));
        }
        output.push_str("  },\n");
    }
    output.push_str("};\n\n");
    output
}

#[cfg(test)]
mod tests {
    use super::{
        CancellationKind, FACADE_SELECTION_POLICY, FacadeLanguage, all_facade_operations,
        facade_operations, generate_remote_facade, generate_remote_facades,
    };
    use crate::family_registry::FAMILY_VIEWS;

    #[test]
    fn every_target_is_emitted_from_the_registry() {
        let outputs = generate_remote_facades();
        assert_eq!(outputs.len(), 3);
        for output in outputs {
            assert!(
                output
                    .source
                    .contains("Generated by acyclic-sdk-contract-wire")
            );
            assert!(output.source.contains(&output.source_binding));
            assert!(
                output.source.contains("OPERATIONS")
                    || output.source.contains("generatedRemoteOperations")
            );
            assert!(output.source.contains(FACADE_SELECTION_POLICY.probe));
            assert!(
                output
                    .source
                    .contains(FACADE_SELECTION_POLICY.post_failure_fallback)
            );
            for family in FAMILY_VIEWS {
                assert!(
                    output.source.contains(&format!("'{}'", family.name))
                        || output.source.contains(&format!("\"{}\"", family.name)),
                    "{} missing {}",
                    output.language.name(),
                    family.name
                );
            }
        }
    }

    #[test]
    fn generated_policy_has_the_rust_transport_kinds() {
        for language in [
            FacadeLanguage::Ruby,
            FacadeLanguage::Php,
            FacadeLanguage::Dart,
        ] {
            let output = generate_remote_facade(language);
            let expected = match language {
                FacadeLanguage::Dart => ["grpc", "httpJson", "grpcWeb"],
                FacadeLanguage::Ruby | FacadeLanguage::Php => ["grpc", "http_json", "grpc_web"],
            };
            for kind in expected {
                assert!(
                    output.source.contains(kind),
                    "{} missing {kind}",
                    language.name()
                );
            }
        }
    }

    #[test]
    fn generated_transport_snapshots_match_every_rust_option() {
        for language in [
            FacadeLanguage::Ruby,
            FacadeLanguage::Php,
            FacadeLanguage::Dart,
        ] {
            let output = generate_remote_facade(language);
            for family in FAMILY_VIEWS {
                for option in family.transport.native.options {
                    let name = match (language, option.kind) {
                        (FacadeLanguage::Dart, crate::TransportKind::Grpc) => "grpc",
                        (FacadeLanguage::Dart, crate::TransportKind::GrpcWeb) => "grpcWeb",
                        (FacadeLanguage::Dart, crate::TransportKind::HttpJson) => "httpJson",
                        (_, crate::TransportKind::Grpc) => "grpc",
                        (_, crate::TransportKind::GrpcWeb) => "grpc_web",
                        (_, crate::TransportKind::HttpJson) => "http_json",
                    };
                    assert!(
                        output.source.contains(name),
                        "{} omitted {} {} transport",
                        language.name(),
                        family.name,
                        name
                    );
                }
                for option in family.transport.browser.options {
                    let name = match (language, option.kind) {
                        (FacadeLanguage::Dart, crate::TransportKind::Grpc) => "grpc",
                        (FacadeLanguage::Dart, crate::TransportKind::GrpcWeb) => "grpcWeb",
                        (FacadeLanguage::Dart, crate::TransportKind::HttpJson) => "httpJson",
                        (_, crate::TransportKind::Grpc) => "grpc",
                        (_, crate::TransportKind::GrpcWeb) => "grpc_web",
                        (_, crate::TransportKind::HttpJson) => "http_json",
                    };
                    assert!(
                        output.source.contains(name),
                        "{} omitted {} browser {} transport",
                        language.name(),
                        family.name,
                        name
                    );
                }
            }
        }
    }

    #[test]
    fn operation_metadata_is_descriptor_linked_and_bearer_authenticated() {
        let mut count = 0;
        for family in FAMILY_VIEWS {
            let operations = facade_operations(family);
            assert_eq!(
                operations.len(),
                family.operation_policies.len(),
                "{} operation count",
                family.name
            );
            for operation in operations {
                count += 1;
                assert!(
                    operation.bearer_auth,
                    "{} missing bearer policy",
                    operation.rpc
                );
                if operation.cancellation == CancellationKind::Call {
                    assert!(
                        operation.client_streaming || operation.server_streaming,
                        "{} has call cancellation without streaming",
                        operation.rpc
                    );
                }
                if operation.cancellation == CancellationKind::Operation {
                    assert!(
                        operation.rpc.ends_with("/Cancel"),
                        "{} has operation cancellation without Cancel RPC",
                        operation.rpc
                    );
                }
            }
        }
        assert!(count > 0);
        assert_eq!(all_facade_operations().len(), count);
    }

    #[test]
    fn generated_sources_bind_streaming_cancel_and_fallback_policy() {
        for language in [
            FacadeLanguage::Ruby,
            FacadeLanguage::Php,
            FacadeLanguage::Dart,
        ] {
            let output = generate_remote_facade(language);
            assert!(
                output.source.contains("server_streaming")
                    || output.source.contains("serverStreaming")
            );
            assert!(output.source.contains("cancellation"));
            assert!(
                output.source.contains("post_failure_fallback")
                    || output.source.contains("postFailureFallback")
            );
            assert!(output.source.contains("replay"));
            assert!(
                output
                    .source
                    .contains("acyclic.stream.v2.StreamService/Read")
            );
        }
    }
}
