//! Rust-owned remote facade output for the portable SDKs.
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
    /// Java source consumed by the JVM transport package.
    Java,
    /// C# source consumed by the .NET transport package.
    Csharp,
}

impl FacadeLanguage {
    /// Stable target name used in generated paths and provenance.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ruby => "ruby",
            Self::Php => "php",
            Self::Dart => "dart",
            Self::Java => "java",
            Self::Csharp => "csharp",
        }
    }

    /// Generated source path relative to the Rust exporter root.
    pub const fn output_path(self) -> &'static str {
        match self {
            Self::Ruby => "ruby/lib/acyclic_sdk/generated_remote_policy.rb",
            Self::Php => "php/src/Acyclic/Runtime/GeneratedRemotePolicy.php",
            Self::Dart => "dart/lib/src/generated_remote_policy.dart",
            Self::Java => "jvm/src/main/java/dev/acyclic/transport/GeneratedRemotePolicy.java",
            Self::Csharp => "dotnet/GeneratedRemotePolicy.cs",
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
        FacadeLanguage::Java,
        FacadeLanguage::Csharp,
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
        FacadeLanguage::Java => render_java(&source_binding),
        FacadeLanguage::Csharp => render_csharp(&source_binding),
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

fn render_java(binding: &str) -> String {
    let mut output = format!(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// rust_policy_source_binding: {binding}\n\npackage dev.acyclic.transport;\n\nimport java.util.ArrayList;\nimport java.util.Collections;\nimport java.util.LinkedHashMap;\nimport java.util.List;\nimport java.util.Map;\n\npublic final class GeneratedRemotePolicy {{\n  private GeneratedRemotePolicy() {{}}\n\n  public enum Runtime {{ NATIVE, BROWSER }}\n  public enum Transport {{ GRPC, GRPC_WEB, HTTP_JSON }}\n  public record Option(Transport transport, boolean streaming, boolean bearerAuth) {{}}\n  public record Operation(boolean clientStreaming, boolean serverStreaming, boolean bearerAuth, String cancellation) {{}}\n  public record Availability(boolean grpc, boolean grpcWeb, boolean httpJson) {{\n    public static final Availability ALL = new Availability(true, true, true);\n    public static final Availability GRPC_ONLY = new Availability(true, false, false);\n    boolean supports(Transport transport) {{\n      return switch (transport) {{\n        case GRPC -> grpc;\n        case GRPC_WEB -> grpcWeb;\n        case HTTP_JSON -> httpJson;\n      }};\n    }}\n  }}\n  public record Defaults(String endpoint, String bearerToken) {{\n    public static Defaults fromEnvironment() {{\n      return new Defaults(firstNonBlank(System.getProperty(\"acyclic.endpoint\"),\n          System.getenv(\"ACYCLIC_ENDPOINT\"), \"http://127.0.0.1:8080\"),\n          firstNonBlank(System.getProperty(\"acyclic.bearerToken\"),\n          System.getenv(\"ACYCLIC_BEARER_TOKEN\"), \"\"));\n    }}\n  }}\n\n  public static final String SOURCE_BINDING = {binding:?};\n  public static final String PROBE = {probe:?};\n  public static final String POST_FAILURE_FALLBACK = {fallback:?};\n  public static final String REPLAY = {replay:?};\n  public static final Map<Runtime, Map<String, List<Option>>> OPTIONS = Map.of(\n      Runtime.NATIVE, buildOptions(ClientRuntime.NATIVE),\n      Runtime.BROWSER, buildOptions(ClientRuntime.BROWSER));\n  public static final Map<String, Map<String, Operation>> OPERATIONS = buildOperations();\n\n",
        probe = FACADE_SELECTION_POLICY.probe,
        fallback = FACADE_SELECTION_POLICY.post_failure_fallback,
        replay = FACADE_SELECTION_POLICY.replay,
    );
    output.push_str("  private enum ClientRuntime { NATIVE, BROWSER }\n\n");
    output.push_str("  private static Map<String, List<Option>> buildOptions(ClientRuntime runtime) {\n    Map<String, List<Option>> result = new LinkedHashMap<>();\n    switch (runtime) {\n      case NATIVE -> {\n");
    render_java_options(&mut output, ClientRuntime::Native, 8);
    output.push_str("      }\n      case BROWSER -> {\n");
    render_java_options(&mut output, ClientRuntime::Browser, 8);
    output.push_str("      }\n    }\n    return Collections.unmodifiableMap(result);\n  }\n\n");
    output.push_str("  private static Map<String, Map<String, Operation>> buildOperations() {\n    Map<String, Map<String, Operation>> result = new LinkedHashMap<>();\n");
    for family in FAMILY_VIEWS {
        output.push_str(&format!(
            "    Map<String, Operation> {} = new LinkedHashMap<>();\n",
            java_identifier(family.name)
        ));
        for operation in facade_operations(family) {
            output.push_str(&format!(
                "    {}.put({:?}, new Operation({}, {}, {}, {:?}));\n",
                java_identifier(family.name),
                operation.rpc,
                operation.client_streaming,
                operation.server_streaming,
                operation.bearer_auth,
                operation.cancellation.name(),
            ));
        }
        output.push_str(&format!(
            "    result.put({:?}, Collections.unmodifiableMap({}));\n",
            family.name,
            java_identifier(family.name)
        ));
    }
    output.push_str("    return Collections.unmodifiableMap(result);\n  }\n\n");
    output.push_str(
        r#"  public static Transport select(
      String family,
      Runtime runtime,
      boolean streaming,
      boolean bearerAuth,
      Availability installed,
      Availability endpoint,
      Transport override) {
    List<Option> familyOptions = OPTIONS.get(runtime).get(family);
    if (familyOptions == null) throw new IllegalArgumentException("unknown transport family or runtime: " + family + "/" + runtime);
    for (Option option : familyOptions) {
      if (option.transport() == override && compatible(option, streaming, bearerAuth, installed, endpoint)) return option.transport();
    }
    if (override != null) throw new IllegalArgumentException("unsupported transport override for " + family + "/" + runtime);
    for (Option option : familyOptions) {
      if (compatible(option, streaming, bearerAuth, installed, endpoint)) return option.transport();
    }
    throw new IllegalArgumentException("no compatible transport for " + family + "/" + runtime);
  }

  private static boolean compatible(Option option, boolean streaming, boolean bearerAuth,
                                    Availability installed, Availability endpoint) {
    return installed.supports(option.transport()) && endpoint.supports(option.transport())
        && (!streaming || option.streaming()) && (!bearerAuth || option.bearerAuth());
  }

  public static String validateBearer(String token) {
    if (token == null || token.isBlank() || token.indexOf('\r') >= 0 || token.indexOf('\n') >= 0) {
      throw new IllegalArgumentException("invalid bearer credential");
    }
    return token;
  }

  private static String firstNonBlank(String first, String second, String fallback) {
    if (first != null && !first.isBlank()) return first;
    if (second != null && !second.isBlank()) return second;
    return fallback;
  }
}
"#,
    );
    output
}

fn render_java_options(output: &mut String, runtime: ClientRuntime, indent: usize) {
    let prefix = " ".repeat(indent);
    for (family, family_options) in options(runtime) {
        output.push_str(&format!("{prefix}result.put({family:?}, List.of("));
        for (index, option) in family_options.iter().enumerate() {
            if index != 0 {
                output.push_str(", ");
            }
            output.push_str(&format!(
                "new Option(Transport.{}, {}, {})",
                java_transport_name(option.kind),
                option.streaming,
                option.bearer_auth
            ));
        }
        output.push_str("));\n");
    }
}

fn java_transport_name(kind: TransportKind) -> &'static str {
    match kind {
        TransportKind::Grpc => "GRPC",
        TransportKind::GrpcWeb => "GRPC_WEB",
        TransportKind::HttpJson => "HTTP_JSON",
    }
}

fn java_identifier(value: &str) -> String {
    value.replace(['.', '/', '-'], "_")
}

fn render_csharp(binding: &str) -> String {
    let mut output = format!(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// rust_policy_source_binding: {binding}\n#pragma warning disable CS1591\n\nusing System;\nusing System.Collections.Generic;\n\nnamespace Acyclic.Sdk.Transport;\n\npublic static class GeneratedRemotePolicy\n{{\n    public enum Runtime {{ Native, Browser }}\n    public enum Transport {{ Grpc, GrpcWeb, HttpJson }}\n    public readonly record struct Option(Transport Transport, bool Streaming, bool BearerAuth);\n    public readonly record struct Operation(bool ClientStreaming, bool ServerStreaming, bool BearerAuth, string Cancellation);\n    public readonly record struct Availability(bool Grpc, bool GrpcWeb, bool HttpJson)\n    {{\n        public static readonly Availability All = new(true, true, true);\n        public static readonly Availability GrpcOnly = new(true, false, false);\n        public bool Supports(Transport transport) => transport switch\n        {{\n            Transport.Grpc => Grpc,\n            Transport.GrpcWeb => GrpcWeb,\n            Transport.HttpJson => HttpJson,\n            _ => false\n        }};\n    }}\n    public readonly record struct Defaults(string Endpoint, string BearerToken)\n    {{\n        public static Defaults FromEnvironment() => new(\n            FirstNonBlank(Environment.GetEnvironmentVariable(\"ACYCLIC_ENDPOINT\"), \"http://127.0.0.1:8080\"),\n            Environment.GetEnvironmentVariable(\"ACYCLIC_BEARER_TOKEN\") ?? string.Empty);\n    }}\n\n    public const string SourceBinding = {binding:?};\n    public const string Probe = {probe:?};\n    public const string PostFailureFallback = {fallback:?};\n    public const string Replay = {replay:?};\n    public static readonly IReadOnlyDictionary<Runtime, IReadOnlyDictionary<string, IReadOnlyList<Option>>> Options =\n        new Dictionary<Runtime, IReadOnlyDictionary<string, IReadOnlyList<Option>>>\n        {{\n            [Runtime.Native] = BuildOptions(Runtime.Native),\n            [Runtime.Browser] = BuildOptions(Runtime.Browser),\n        }};\n    public static readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, Operation>> Operations = BuildOperations();\n\n",
        probe = FACADE_SELECTION_POLICY.probe,
        fallback = FACADE_SELECTION_POLICY.post_failure_fallback,
        replay = FACADE_SELECTION_POLICY.replay,
    );
    output.push_str("    private static IReadOnlyDictionary<string, IReadOnlyList<Option>> BuildOptions(Runtime runtime)\n    {\n        var result = new Dictionary<string, IReadOnlyList<Option>>(StringComparer.Ordinal);\n        switch (runtime)\n        {\n            case Runtime.Native:\n");
    render_csharp_options(&mut output, ClientRuntime::Native, 16);
    output.push_str("                break;\n            case Runtime.Browser:\n");
    render_csharp_options(&mut output, ClientRuntime::Browser, 16);
    output.push_str("                break;\n        }\n        return result;\n    }\n\n    private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, Operation>> BuildOperations()\n    {\n        var result = new Dictionary<string, IReadOnlyDictionary<string, Operation>>(StringComparer.Ordinal);\n");
    for family in FAMILY_VIEWS {
        output.push_str(&format!(
            "        var {} = new Dictionary<string, Operation>(StringComparer.Ordinal);\n",
            csharp_identifier(family.name)
        ));
        for operation in facade_operations(family) {
            output.push_str(&format!(
                "        {}[{:#?}] = new({}, {}, {}, {:#?});\n",
                csharp_identifier(family.name),
                operation.rpc,
                operation.client_streaming,
                operation.server_streaming,
                operation.bearer_auth,
                operation.cancellation.name(),
            ));
        }
        output.push_str(&format!(
            "        result[{:#?}] = {}.AsReadOnly();\n",
            family.name,
            csharp_identifier(family.name)
        ));
    }
    output.push_str("        return result;\n    }\n\n");
    output.push_str(
        r#"    public static Transport Select(
        string family,
        Runtime runtime,
        bool streaming,
        bool bearerAuth,
        Availability installed,
        Availability endpoint,
        Transport? overrideTransport = null)
    {
        if (!Options.TryGetValue(runtime, out var byFamily) || !byFamily.TryGetValue(family, out var familyOptions))
            throw new ArgumentException($"unknown transport family or runtime: {family}/{runtime}");
        foreach (var option in familyOptions)
        {
            if (overrideTransport == option.Transport && Compatible(option, streaming, bearerAuth, installed, endpoint))
                return option.Transport;
        }
        if (overrideTransport is not null)
            throw new ArgumentException($"unsupported transport override for {family}/{runtime}");
        foreach (var option in familyOptions)
        {
            if (Compatible(option, streaming, bearerAuth, installed, endpoint)) return option.Transport;
        }
        throw new ArgumentException($"no compatible transport for {family}/{runtime}");
    }

    private static bool Compatible(Option option, bool streaming, bool bearerAuth,
        Availability installed, Availability endpoint) => installed.Supports(option.Transport)
        && endpoint.Supports(option.Transport)
        && (!streaming || option.Streaming)
        && (!bearerAuth || option.BearerAuth);

    public static string ValidateBearer(string token)
    {
        if (string.IsNullOrWhiteSpace(token) || token.Contains('\r') || token.Contains('\n'))
            throw new ArgumentException("invalid bearer credential", nameof(token));
        return token;
    }

    private static string FirstNonBlank(string? value, string fallback) =>
        string.IsNullOrWhiteSpace(value) ? fallback : value;
}
"#,
    );
    output
}

fn render_csharp_options(output: &mut String, runtime: ClientRuntime, indent: usize) {
    let prefix = " ".repeat(indent);
    for (family, family_options) in options(runtime) {
        output.push_str(&format!("{prefix}result[{family:#?}] = new Option[] {{"));
        for (index, option) in family_options.iter().enumerate() {
            if index != 0 {
                output.push_str(", ");
            }
            output.push_str(&format!(
                "new(Transport.{}, {}, {})",
                csharp_transport_name(option.kind),
                option.streaming,
                option.bearer_auth
            ));
        }
        output.push_str("};\n");
    }
}

fn csharp_transport_name(kind: TransportKind) -> &'static str {
    match kind {
        TransportKind::Grpc => "Grpc",
        TransportKind::GrpcWeb => "GrpcWeb",
        TransportKind::HttpJson => "HttpJson",
    }
}

fn csharp_identifier(value: &str) -> String {
    value.replace(['.', '/', '-'], "_")
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
        assert_eq!(outputs.len(), 5);
        for output in outputs {
            assert!(
                output
                    .source
                    .contains("Generated by acyclic-sdk-contract-wire")
            );
            assert!(output.source.contains(&output.source_binding));
            assert!(
                output.source.contains("OPERATIONS")
                    || output.source.contains("Operations")
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
            FacadeLanguage::Java,
            FacadeLanguage::Csharp,
        ] {
            let output = generate_remote_facade(language);
            let expected = match language {
                FacadeLanguage::Dart => ["grpc", "httpJson", "grpcWeb"],
                FacadeLanguage::Ruby | FacadeLanguage::Php => ["grpc", "http_json", "grpc_web"],
                FacadeLanguage::Java => ["GRPC", "HTTP_JSON", "GRPC_WEB"],
                FacadeLanguage::Csharp => ["Grpc", "HttpJson", "GrpcWeb"],
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
            FacadeLanguage::Java,
            FacadeLanguage::Csharp,
        ] {
            let output = generate_remote_facade(language);
            for family in FAMILY_VIEWS {
                for option in family.transport.native.options {
                    let name = match (language, option.kind) {
                        (FacadeLanguage::Dart, crate::TransportKind::Grpc) => "grpc",
                        (FacadeLanguage::Dart, crate::TransportKind::GrpcWeb) => "grpcWeb",
                        (FacadeLanguage::Dart, crate::TransportKind::HttpJson) => "httpJson",
                        (FacadeLanguage::Java, crate::TransportKind::Grpc) => "GRPC",
                        (FacadeLanguage::Java, crate::TransportKind::GrpcWeb) => "GRPC_WEB",
                        (FacadeLanguage::Java, crate::TransportKind::HttpJson) => "HTTP_JSON",
                        (FacadeLanguage::Csharp, crate::TransportKind::Grpc) => "Grpc",
                        (FacadeLanguage::Csharp, crate::TransportKind::GrpcWeb) => "GrpcWeb",
                        (FacadeLanguage::Csharp, crate::TransportKind::HttpJson) => "HttpJson",
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
                        (FacadeLanguage::Java, crate::TransportKind::Grpc) => "GRPC",
                        (FacadeLanguage::Java, crate::TransportKind::GrpcWeb) => "GRPC_WEB",
                        (FacadeLanguage::Java, crate::TransportKind::HttpJson) => "HTTP_JSON",
                        (FacadeLanguage::Csharp, crate::TransportKind::Grpc) => "Grpc",
                        (FacadeLanguage::Csharp, crate::TransportKind::GrpcWeb) => "GrpcWeb",
                        (FacadeLanguage::Csharp, crate::TransportKind::HttpJson) => "HttpJson",
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
            FacadeLanguage::Java,
            FacadeLanguage::Csharp,
        ] {
            let output = generate_remote_facade(language);
            assert!(
                output.source.contains("server_streaming")
                    || output.source.contains("serverStreaming")
                    || output.source.contains("ServerStreaming")
            );
            assert!(
                output.source.contains("cancellation")
                    || output.source.contains("Cancellation")
            );
            assert!(
                output.source.contains("post_failure_fallback")
                    || output.source.contains("POST_FAILURE_FALLBACK")
                    || output.source.contains("PostFailureFallback")
                    || output.source.contains("postFailureFallback")
            );
            assert!(
                output.source.contains("replay")
                    || output.source.contains("REPLAY")
                    || output.source.contains("Replay")
            );
            assert!(
                output
                    .source
                    .contains("acyclic.stream.v2.StreamService/Read")
            );
        }
    }
}
