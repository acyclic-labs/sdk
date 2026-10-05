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
    type_policy::{
        resolved_request_fields, resolved_response_fields, resolved_rpc_methods,
        ResolvedRequestField, ResolvedValidationConstraint, SemanticRule,
    },
};

#[path = "facades_python_go.rs"]
mod python_go;
#[path = "typed_facades.rs"]
mod typed_facades;

pub use typed_facades::{
    generate_jvm_semantic_types, generate_jvm_typed_clients, generate_jvm_typed_requests,
    generate_jvm_typed_responses,
    JAVA_CLIENTS_PATH, JAVA_PATH, JAVA_REQUESTS_PATH, JAVA_RESPONSES_PATH,
    KOTLIN_CLIENTS_PATH, KOTLIN_PATH, KOTLIN_REQUESTS_PATH, KOTLIN_RESPONSES_PATH,
    SCALA_CLIENTS_PATH, SCALA_PATH, SCALA_REQUESTS_PATH, SCALA_RESPONSES_PATH,
};
pub use crate::csharp_typed_facades::{generate_csharp_typed_facade, CSHARP_TYPED_PATH};

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
    /// Python source consumed by the pip package.
    Python,
    /// Go source consumed by the Go module.
    Go,
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
            Self::Python => "python",
            Self::Go => "go",
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
            Self::Python => "python/src/acyclic_sdk/remote.py",
            Self::Go => "go/client.go",
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
        FacadeLanguage::Python,
        FacadeLanguage::Go,
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
        FacadeLanguage::Python => python_go::render_python(&source_binding),
        FacadeLanguage::Go => python_go::render_go(&source_binding),
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

/// Generate checker fixtures for the Rust-owned Python and Go refinements.
///
/// These files are emitted beside the installed facade sources so release
/// qualification exercises the exact constructors shipped to consumers.
pub fn generate_type_policy_qualification_tests() -> Vec<(&'static str, String)> {
    let mut outputs = vec![
        (
            "python/tests/generated_type_policy_test.py",
            python_go::render_python_type_policy_test(),
        ),
        (
            "go/type_policy_generated_test.go",
            python_go::render_go_type_policy_test(),
        ),
    ];
    outputs.extend(crate::csharp_typed_facades::generate_csharp_type_policy_tests());
    outputs.push((
        "jvm/src/test/java/dev/acyclic/transport/RustSemanticTypesTest.java",
        render_java_semantic_type_test(),
    ));
    outputs
}

fn render_java_semantic_type_test() -> String {
    "".to_owned()
        + "// Generated by acyclic-sdk-contract-wire; do not edit.\n"
        + "package dev.acyclic.transport;\n\n"
        + "import static org.junit.jupiter.api.Assertions.*;\n"
        + "import com.google.protobuf.ByteString;\n"
        + "import java.util.Optional;\n"
        + "import org.junit.jupiter.api.Test;\n\n"
        + "class RustSemanticTypesTest {\n"
        + "  @Test void nominalStringsCannotBeConfusedWithWireStrings() {\n"
        + "    var id = RustSemanticTypes.ActorId.of(\"actor-1\");\n"
        + "    assertEquals(\"actor-1\", id.toWire());\n"
        + "    assertThrows(IllegalArgumentException.class, () -> RustSemanticTypes.ActorId.of(\"\"));\n"
        + "  }\n"
        + "  @Test void fixedDigestAndBoundedLimitAreValidated() {\n"
        + "    var digest = RustSemanticTypes.Sha256Digest.of(ByteString.copyFrom(new byte[32]));\n"
        + "    assertEquals(32, digest.toWire().size());\n"
        + "    assertThrows(IllegalArgumentException.class, () -> RustSemanticTypes.Sha256Digest.of(ByteString.copyFrom(new byte[31])));\n"
        + "    assertEquals(1000, RustSemanticTypes.PageLimit.of(1000).value());\n"
        + "    assertThrows(IllegalArgumentException.class, () -> RustSemanticTypes.PageLimit.of(1001));\n"
        + "  }\n"
        + "  @Test void presenceAndUnknownUnionRemainExplicit() {\n"
        + "    assertEquals(Optional.empty(), RustSemanticTypes.present(\"x\", false));\n"
        + "    RustSemanticTypes.WireChoice unknown = new RustSemanticTypes.Unknown(99, ByteString.EMPTY);\n"
        + "    assertEquals(99, ((RustSemanticTypes.Unknown) unknown).tag());\n"
        + "  }\n"
        + "  @Test void publicRequestFactoriesConvertNominalValuesToWireFields() {\n"
        + "    var invoke = RustTypedRequests.actorsInvokeActor(RustSemanticTypes.ActorId.of(\"actor-1\"), RustSemanticTypes.MethodName.of(\"GET\"));\n"
        + "    assertEquals(\"actor-1\", invoke.getActorId());\n"
        + "    assertEquals(\"GET\", invoke.getMethod());\n"
        + "    var read = RustTypedRequests.streamRead(RustSemanticTypes.StreamPageLimit.of(5));\n"
        + "    assertEquals(5, read.getLimit());\n"
        + "  }\n"
        + "}\n"
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
                // Keep operation metadata aligned with the Rust-owned native
                // adapter. Machines is native gRPC with mTLS and deliberately
                // does not require the shared bearer credential.
                bearer_auth: family
                    .transport
                    .native
                    .options
                    .first()
                    .map(|option| option.bearer_auth)
                    .unwrap_or(true),
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
        include_bytes!("facades_python_go.rs").as_slice(),
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
        include_bytes!("type_policy.rs").as_slice(),
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
    output.push_str(&render_ruby_shapes());
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
    output.push_str(&render_php_shapes());
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
                "    {}.put({:?}, new Operation({}, {}, {}, {:?}, {}, {}, {}));\n",
                java_identifier(family.name),
                operation.rpc,
                operation.client_streaming,
                operation.server_streaming,
                operation.bearer_auth,
                operation.cancellation.name(),
                java_strings(operation.capabilities),
                java_strings(operation.errors),
                java_strings(operation.validations),
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

  public static boolean requiresBearer(String family, Runtime runtime) {
    List<Option> familyOptions = OPTIONS.get(runtime).get(family);
    if (familyOptions == null || familyOptions.isEmpty()) {
      throw new IllegalArgumentException("unknown transport family or runtime: " + family + "/" + runtime);
    }
    return familyOptions.get(0).bearerAuth();
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
    output = output.replace(
        "public record Operation(boolean clientStreaming, boolean serverStreaming, boolean bearerAuth, String cancellation) {}",
        "public record Operation(boolean clientStreaming, boolean serverStreaming, boolean bearerAuth,\n                          String cancellation, List<String> capabilities, List<String> errors,\n                          List<String> validations) {}",
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

fn java_strings(values: &[&str]) -> String {
    let values = values
        .iter()
        .map(|value| format!("{:?}", value))
        .collect::<Vec<_>>()
        .join(", ");
    format!("List.of({values})")
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
                "        {}[{:#?}] = new({}, {}, {}, {:#?}, {}, {}, {});\n",
                csharp_identifier(family.name),
                operation.rpc,
                operation.client_streaming,
                operation.server_streaming,
                operation.bearer_auth,
                operation.cancellation.name(),
                csharp_strings(operation.capabilities),
                csharp_strings(operation.errors),
                csharp_strings(operation.validations),
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

    public static bool RequiresBearer(string family, Runtime runtime)
    {
        if (!Options.TryGetValue(runtime, out var byFamily) ||
            !byFamily.TryGetValue(family, out var familyOptions) || familyOptions.Count == 0)
            throw new ArgumentException($"unknown transport family or runtime: {family}/{runtime}");
        return familyOptions[0].BearerAuth;
    }

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
    output = output.replace(
        "public readonly record struct Operation(bool ClientStreaming, bool ServerStreaming, bool BearerAuth, string Cancellation);",
        "public readonly record struct Operation(\n        bool ClientStreaming,\n        bool ServerStreaming,\n        bool BearerAuth,\n        string Cancellation,\n        IReadOnlyList<string> Capabilities,\n        IReadOnlyList<string> Errors,\n        IReadOnlyList<string> Validations);",
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

fn csharp_strings(values: &[&str]) -> String {
    if values.is_empty() {
        return "Array.Empty<string>()".to_owned();
    }
    let values = values
        .iter()
        .map(|value| format!("{value:#?}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("new[] {{ {values} }}")
}

fn render_dart(binding: &str) -> String {
    let mut output = format!(
        "// Generated by acyclic-sdk-contract-wire; do not edit.\n// rust_policy_source_binding: {binding}\n\nimport 'dart:convert';\n\nconst generatedRemotePolicySourceBinding = {binding:?};\n\nenum GeneratedRemoteTransport {{ grpc, grpcWeb, httpJson }}\nenum GeneratedClientRuntime {{ native, browser }}\n\nconst generatedRemotePolicyOptions = <GeneratedClientRuntime, Map<String, List<(GeneratedRemoteTransport, bool, bool)>>>{{\n"
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
    output.push_str(&render_dart_shapes());
    output.push_str(
        r#"const generatedRemoteTransportAvailability = <GeneratedRemoteTransport, bool>{
  GeneratedRemoteTransport.grpc: true,
  GeneratedRemoteTransport.grpcWeb: true,
  GeneratedRemoteTransport.httpJson: true,
};

class GeneratedRemotePolicy {
  static Map<String, Object?> requestShape(String rpc, [String? family]) {
    final shape = generatedRemoteRpcShapes[_shapeKey(rpc, family)]?['request'];
    if (shape is Map<String, Object?>) return shape;
    throw ArgumentError('unknown RPC shape: $rpc');
  }

  static Map<String, Object?> responseShape(String rpc, [String? family]) {
    final shape = generatedRemoteRpcShapes[_shapeKey(rpc, family)]?['response'];
    if (shape is Map<String, Object?>) return shape;
    throw ArgumentError('unknown RPC shape: $rpc');
  }

  static String _shapeKey(String rpc, String? family) {
    final direct = generatedRemoteRpcShapes[rpc];
    if (direct != null && (family == null || direct['family'] == family)) return rpc;
    for (final entry in generatedRemoteRpcShapes.entries) {
      if (entry.key.toLowerCase().endsWith('/${rpc.toLowerCase()}') && (family == null || entry.value['family'] == family)) return entry.key;
    }
    return rpc;
  }

  static Object? constructRequest(String rpc, [Object? values, String? family]) {
    return validateRequest(rpc, values, family);
  }

  static Object? validateRequest(String rpc, Object? request, [String? family]) {
    return _validateShape(requestShape(rpc, family), request);
  }

  static Object? validateResponse(String rpc, Object? response, [String? family]) {
    return _validateShape(responseShape(rpc, family), response);
  }

  static Object? preserveUnknown(Object? value) => value;

  static Object? _validateShape(Map<String, Object?> shape, Object? value) {
    if (value == null) return value;
    _validateNested(shape['fields'], shape['message'], value, <int>{}, 0);
    final fields = shape['fields'];
    if (fields is! List) return value;
    for (final rawField in fields) {
      if (rawField is! Map || rawField['required'] != true) continue;
      final name = rawField['field'];
      if (name is String && !_fieldPresent(value, rawField)) {
        throw ArgumentError('required field missing: $name');
      }
    }
    return value;
  }

  static Map<Object?, Object?>? _proto3Json(Object value) {
    if (value is Map) return value.cast<Object?, Object?>();
    try {
      final json = (value as dynamic).toProto3Json();
      return json is Map ? json.cast<Object?, Object?>() : null;
    } catch (_) {
      return null;
    }
  }

  static Object? _fieldValue(Object value, Map rawField) {
    final number = rawField['number'];
    try {
      if (number is int) return (value as dynamic).getField(number);
    } catch (_) {}
    final json = _proto3Json(value);
    return json?[rawField['jsonName']];
  }

  static bool _fieldPresent(Object value, Map rawField) {
    if (value is Map) return value.containsKey(rawField['field']);
    final number = rawField['number'];
    try {
      if (number is int && (value as dynamic).hasField(number) == true) return true;
    } catch (_) {}
    final json = _proto3Json(value);
    if (json != null) return json.containsKey(rawField['jsonName']);
    return rawField['required'] == true && _fieldValue(value, rawField) != null;
  }

  static void _validateNested(Object? rawFields, Object? rawMessage, Object value, Set<int> seen, [int depth = 0]) {
    if (rawFields is! List || rawMessage is! String || depth >= 64) return;
    final identity = identityHashCode(value);
    if (seen.contains(identity)) return;
    seen.add(identity);
    for (final rawField in rawFields) {
      if (rawField is! Map) continue;
      final path = rawField['path'];
      if (path is! String || !path.startsWith('$rawMessage.') || path.split('.').length != rawMessage.split('.').length + 1) continue;
      final name = rawField['field'];
      if (name is! String) continue;
      final map = value is Map ? value : null;
      var present = map?.containsKey(name) ?? _fieldPresent(value, rawField);
      var fieldValue = map?[name] ?? (map == null ? _fieldValue(value, rawField) : null);
      if (rawField['required'] == true && !present && fieldValue != null) present = true;
      if (rawField['required'] == true && !present) throw ArgumentError('required field missing: $name');
      if (!present) continue;
      for (final constraint in (rawField['constraints'] is List ? rawField['constraints'] as List : const [])) {
        if (constraint == 'non_empty' && fieldValue is String && fieldValue.isEmpty) throw ArgumentError('invalid field: $name');
        if (constraint == 'utf8' && fieldValue is List<int>) {
          try {
            utf8.decode(fieldValue);
          } on FormatException {
            throw ArgumentError('invalid field: $name');
          }
        }
        if (constraint == 'non_negative' && fieldValue is num && fieldValue < 0) throw ArgumentError('invalid field: $name');
        if (constraint == 'strictly_positive' && fieldValue is num && fieldValue <= 0) throw ArgumentError('invalid field: $name');
        if (constraint is String && constraint.startsWith('operation_capability:')) {
          final capability = constraint.substring('operation_capability:'.length);
          final capabilities = fieldValue is Map
              ? fieldValue['capabilities']
              : (fieldValue == null ? null : _proto3Json(fieldValue)?['capabilities']);
          if (capabilities is! List || !capabilities.contains(capability)) throw ArgumentError('missing operation capability $capability for $name');
        }
        if (constraint is String && constraint.startsWith('ordered_parts:')) {
          final limits = constraint.substring('ordered_parts:'.length).split(':').map(int.parse).toList();
          if (fieldValue is! List || fieldValue.length > limits[0]) throw ArgumentError('invalid ordered parts for $name');
          var previous = 0;
          for (final part in fieldValue) {
            final number = part is Map
                ? (part['part_number'] ?? part['partNumber'])
                : (_proto3Json(part)?['partNumber'] ?? _proto3Json(part)?['part_number']);
            if (number is! int || number <= previous || number > limits[1]) throw ArgumentError('invalid ordered parts for $name');
            previous = number;
          }
        }
        if (constraint is String && constraint.startsWith('max_record_bytes:')) {
          final maximum = int.parse(constraint.substring('max_record_bytes:'.length));
          if (fieldValue is! List) throw ArgumentError('record is not serializable for $name');
          for (final record in fieldValue) {
            if (record is! List<int>) throw ArgumentError('record is not serializable for $name');
            if (record.length > maximum) throw ArgumentError('record exceeds Rust byte limit for $name');
          }
        }
        if (constraint is String && constraint.startsWith('max_command_bytes:')) {
          final maximum = int.parse(constraint.substring('max_command_bytes:'.length));
          if (fieldValue is! List) throw ArgumentError('command is not serializable for $name');
          for (final command in fieldValue) {
            int? bytes;
            if (command is List<int>) {
              bytes = command.length;
            } else {
              try {
                final encoded = (command as dynamic).writeToBuffer();
                if (encoded is List<int>) bytes = encoded.length;
              } catch (_) {}
            }
            if (bytes == null) throw ArgumentError('command is not serializable for $name');
            if (bytes > maximum) throw ArgumentError('command exceeds Rust byte limit for $name');
          }
        }
        if (constraint == 'atomic_precondition') {
          var arms = 0;
          if (fieldValue is Map) {
            arms = (fieldValue.containsKey('if_absent') || fieldValue.containsKey('ifAbsent') ? 1 : 0)
                + (fieldValue.containsKey('if_match') || fieldValue.containsKey('ifMatch') ? 1 : 0);
          } else {
            final json = fieldValue == null ? null : _proto3Json(fieldValue);
            arms = json == null ? 0 : (json.containsKey('ifAbsent') || json.containsKey('if_absent') ? 1 : 0)
                + (json.containsKey('ifMatch') || json.containsKey('if_match') ? 1 : 0);
            try {
              final which = (fieldValue as dynamic).whichOneof('condition');
              if (which != null) arms = 1;
            } catch (_) {}
          }
          if (arms != 1) throw ArgumentError('preconditions must select exactly one condition');
        }
        if (constraint is String && constraint.startsWith('cross_field:') && constraint.endsWith('.required')) {
          if (fieldValue == null || (fieldValue is String && fieldValue.isEmpty) || (fieldValue is List && fieldValue.isEmpty)) {
            throw ArgumentError('required cross-field value missing: $name');
          }
        }
        // bucket_must_be_empty is provider state and remains metadata for
        // server admission; a client cannot invent a bucket snapshot.
      }
      final typeName = (rawField['typeName'] ?? '').toString().replaceFirst(RegExp(r'^\.'), '');
      if (typeName.isEmpty) continue;
      if (rawField['repeated'] == true && fieldValue is List) {
        for (final item in fieldValue) {
          _validateNested(rawFields, typeName, item, <int>{...seen}, depth + 1);
        }
      } else if (fieldValue != null) {
        _validateNested(rawFields, typeName, fieldValue, <int>{...seen}, depth + 1);
      }
    }
  }

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

fn resolved_shape_models() -> (
    Vec<crate::type_policy::ResolvedRpcMethod>,
    Vec<ResolvedRequestField>,
    Vec<ResolvedRequestField>,
) {
    (
        resolved_rpc_methods().expect("Rust RPC identities must resolve before facade generation"),
        resolved_request_fields().expect("Rust request shapes must resolve before facade generation"),
        resolved_response_fields().expect("Rust response shapes must resolve before facade generation"),
    )
}

fn shape_optional_i32(value: Option<i32>, null_literal: &str) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| null_literal.to_owned())
}

fn shape_optional_string(value: Option<&str>, null_literal: &str) -> String {
    value
        .map(|value| format!("{value:?}"))
        .unwrap_or_else(|| null_literal.to_owned())
}

fn shape_presence(field: &ResolvedRequestField) -> bool {
    field.proto3_optional || field.oneof_index.is_some() || field.label == Some(2)
}

fn shape_repeated(field: &ResolvedRequestField) -> bool {
    field.label == Some(3)
}

fn shape_required(field: &ResolvedRequestField) -> bool {
    field.label == Some(2)
}

fn shape_constraint_name(constraint: &ResolvedValidationConstraint) -> String {
    match constraint {
        ResolvedValidationConstraint::Operation(rule) => match rule {
            crate::type_policy::OperationRule::Capability(value) => {
                format!("operation_capability:{value}")
            }
            crate::type_policy::OperationRule::BucketMustBeEmpty => {
                "bucket_must_be_empty".to_owned()
            }
            crate::type_policy::OperationRule::OrderedParts {
                max_items,
                max_part_number,
            } => format!("ordered_parts:{max_items}:{max_part_number}"),
            crate::type_policy::OperationRule::AtomicPrecondition => {
                "atomic_precondition".to_owned()
            }
            crate::type_policy::OperationRule::MaxRecordBytes(value) => {
                format!("max_record_bytes:{value}")
            }
            crate::type_policy::OperationRule::MaxCommandBytes(value) => {
                format!("max_command_bytes:{value}")
            }
            crate::type_policy::OperationRule::Policy(value) => {
                format!("rust_policy:{value}")
            }
        },
        ResolvedValidationConstraint::CrossField(value) => format!("cross_field:{value}"),
        ResolvedValidationConstraint::Unresolved(value) => format!("unresolved:{value}"),
        ResolvedValidationConstraint::Rule(rule) => match rule {
            SemanticRule::NonEmpty => "non_empty".to_owned(),
            SemanticRule::Utf8 => "utf8".to_owned(),
            SemanticRule::NonNegative => "non_negative".to_owned(),
            SemanticRule::StrictlyPositive => "strictly_positive".to_owned(),
            SemanticRule::FixedLength(value) => format!("fixed_length:{value}"),
            SemanticRule::MaxBytes(value) => format!("max_bytes:{value}"),
            SemanticRule::MaxItems(value) => format!("max_items:{value}"),
            SemanticRule::BoundedInteger { min, max } => format!("bounded_integer:{min}:{max}"),
            SemanticRule::Sha256Digest => "sha256_digest".to_owned(),
            SemanticRule::Immutable => "immutable".to_owned(),
            SemanticRule::Monotonic => "monotonic".to_owned(),
            SemanticRule::CanonicalResourceName => "canonical_resource_name".to_owned(),
            SemanticRule::ExactOneof => "exact_oneof".to_owned(),
            SemanticRule::ExplicitPresence => "explicit_presence".to_owned(),
            SemanticRule::PreserveUnknownEnum => "preserve_unknown_enum".to_owned(),
            SemanticRule::PreserveUnknownOneof => "preserve_unknown_oneof".to_owned(),
        },
    }
}

fn shape_constraints(field: &ResolvedRequestField) -> Vec<String> {
    field
        .validation_constraints
        .iter()
        .map(shape_constraint_name)
        .collect()
}

fn ruby_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn ruby_shape_field(field: &ResolvedRequestField) -> String {
    let constraints = ruby_string_list(&shape_constraints(field));
    format!(
        "{{ \"path\" => {:?}, \"field\" => {:?}, \"number\" => {}, \"json_name\" => {:?}, \"type_name\" => {}, \"wire_type\" => {}, \"label\" => {}, \"oneof_index\" => {}, \"presence\" => {}, \"required\" => {}, \"repeated\" => {}, \"proto3_optional\" => {}, \"semantic_type\" => {}, \"validation\" => {:?}, \"constraints\" => [{}], \"preserve_unknown_enum\" => {}, \"preserve_unknown_oneof\" => {} }}",
        format!("{}.{}", field.message_path, field.field),
        field.field,
        field.number,
        field.json_name,
        shape_optional_string(field.type_name.as_deref(), "nil"),
        shape_optional_i32(field.wire_type, "nil"),
        shape_optional_i32(field.label, "nil"),
        shape_optional_i32(field.oneof_index, "nil"),
        shape_presence(field),
        shape_required(field),
        shape_repeated(field),
        field.proto3_optional,
        shape_optional_string(field.semantic_type.as_deref(), "nil"),
        field.validation_rules,
        constraints,
        field.wire_type == Some(14),
        field.oneof_index.is_some(),
    )
}

fn render_ruby_shape_fields(output: &mut String, fields: &[ResolvedRequestField], rpc: &str) {
    for field in fields.iter().filter(|field| field.rpc == rpc) {
        output.push_str("\n            ");
        output.push_str(&ruby_shape_field(field));
        output.push(',');
    }
}

fn render_ruby_shapes() -> String {
    let (methods, requests, responses) = resolved_shape_models();
    let mut output = String::from("      SHAPES = {\n");
    for method in methods {
        output.push_str(&format!(
            "        {:?} => {{ \"family\" => {:?}, \"service\" => {:?}, \"method\" => {:?}, \"request\" => {{ \"message\" => {:?}, \"fields\" => [",
            method.rpc, method.family, method.service, method.method, method.input_message
        ));
        render_ruby_shape_fields(&mut output, &requests, &method.rpc);
        output.push_str(&format!(
            "] }}, \"response\" => {{ \"message\" => {:?}, \"fields\" => [",
            method.output_message
        ));
        render_ruby_shape_fields(&mut output, &responses, &method.rpc);
        output.push_str(&format!(
            "] }}, \"client_streaming\" => {}, \"server_streaming\" => {} }},\n",
            method.client_streaming, method.server_streaming
        ));
    }
    output.push_str("      }.freeze\n\n      def request_shape(rpc, family: nil)\n        SHAPES.fetch(shape_key(rpc, family)).fetch(\"request\")\n      end\n\n      def response_shape(rpc, family: nil)\n        SHAPES.fetch(shape_key(rpc, family)).fetch(\"response\")\n      end\n\n      def shape_key(rpc, family = nil)\n        key = rpc.to_s\n        return key if SHAPES.key?(key) && (family.nil? || SHAPES.fetch(key).fetch(\"family\") == family.to_s)\n        SHAPES.keys.find { |candidate| candidate.downcase.end_with?(\"/#{key.downcase}\") && (family.nil? || SHAPES.fetch(candidate).fetch(\"family\") == family.to_s) } || key\n      end\n\n      def construct_request(rpc, values = {}, family: nil)\n        validate_request(rpc, values, family: family)\n      end\n\n      def validate_request(rpc, request, family: nil)\n        validate_shape!(request_shape(rpc, family: family), request)\n      end\n\n      def validate_response(rpc, response, family: nil)\n        shape = response_shape(rpc, family: family)\n        if response.is_a?(Array)\n          response.each { |item| validate_shape!(shape, item) }\n          response\n        else\n          validate_shape!(shape, response)\n        end\n      end\n\n      def preserve_unknown(value)\n        value\n      end\n\n      def validate_shape!(shape, value)\n        return value unless value\n        shape.fetch(\"fields\").each do |field|\n          next unless field[\"required\"]\n          field_name = field.fetch(\"field\")\n          present = if value.is_a?(Hash)\n            value.key?(field_name) || value.key?(field_name.to_sym)\n          elsif value.respond_to?(field_name)\n            !value.public_send(field_name).nil?\n          else\n            false\n          end\n          raise ArgumentError, \"required field missing: #{field_name}\" unless present\n        end\n        value\n      end\n\n      module_function :request_shape, :response_shape, :shape_key, :construct_request, :validate_request, :validate_response, :preserve_unknown, :validate_shape!\n\n");
    output.push_str(r###"      def validate_nested!(fields, message, value, seen, depth = 0)
        return value unless value
        return value if depth >= 64 || seen[value.object_id]
        seen[value.object_id] = true
          fields.select { |field| field["path"].start_with?("#{message}.") && field["path"].count(".") == message.count(".") + 1 }.each do |field|
            field_name = field.fetch("field")
          present, field_value = if value.is_a?(Hash)
            key = value.key?(field_name) ? field_name : field_name.to_sym
            [value.key?(key), value[key]]
          elsif value.respond_to?(field_name) && begin
            value.method(field_name).arity <= 0
          rescue NameError, ArgumentError
            false
          end
            candidate = value.public_send(field_name)
            present = if value.respond_to?(:to_h) && value.to_h.is_a?(Hash)
              wire = value.to_h
              wire.key?(field_name.to_sym) || wire.key?(field_name)
            else
              !candidate.nil?
            end
            [present, candidate]
          elsif value.respond_to?(:to_h) && value.to_h.is_a?(Hash)
            wire = value.to_h
            key = wire.key?(field_name.to_sym) ? field_name.to_sym : field_name
            [wire.key?(key), wire[key]]
          else
            [false, nil]
          end
          # A required proto3 scalar has no wire-level presence bit.  Its
          # generated getter still exposes the Rust contract's default, so
          # validate that default (for example, 0 must fail a positive rule)
          # instead of treating it as an absent optional field.
          if field["required"] && !present && !field_value.nil?
            present = true
          end
          raise ArgumentError, "required field missing: #{field_name}" if field["required"] && !present
          next unless present
          field.fetch("constraints", []).each do |constraint|
            case constraint
            when "non_empty"
              raise ArgumentError, "invalid #{field_name}" if field_value.respond_to?(:empty?) && field_value.empty?
            when "utf8"
              raise ArgumentError, "invalid #{field_name}" if field_value.is_a?(String) && !field_value.valid_encoding?
            when "non_negative"
              raise ArgumentError, "invalid #{field_name}" if field_value.is_a?(Numeric) && field_value.negative?
            when "strictly_positive"
              raise ArgumentError, "invalid #{field_name}" if field_value.is_a?(Numeric) && field_value <= 0
            when /^operation_capability:(.+)$/
              capability = Regexp.last_match(1)
              capabilities = if field_value.is_a?(Hash)
                field_value["capabilities"] || field_value[:capabilities]
              elsif field_value.respond_to?(:capabilities)
                field_value.public_send(:capabilities)
              end
              unless capabilities.respond_to?(:include?) && capabilities.include?(capability)
                raise ArgumentError, "missing operation capability #{capability} for #{field_name}"
              end
            when /^ordered_parts:(\d+):(\d+)$/
              maximum_items = Regexp.last_match(1).to_i
              maximum_part_number = Regexp.last_match(2).to_i
              unless (field_value.is_a?(Array) || (field_value.respond_to?(:each) && !field_value.is_a?(Hash) && !field_value.is_a?(String))) && field_value.length <= maximum_items
                raise ArgumentError, "invalid ordered parts for #{field_name}"
              end
              previous = 0
              field_value.each do |part|
                part_number = if part.is_a?(Hash)
                  part["part_number"] || part[:part_number]
                elsif part.respond_to?(:part_number)
                  part.public_send(:part_number)
                end
                unless part_number.is_a?(Integer) && part_number.positive? && part_number <= maximum_part_number && part_number > previous
                  raise ArgumentError, "invalid ordered parts for #{field_name}"
                end
                previous = part_number
              end
            when /^max_record_bytes:(\d+)$/
              maximum = Regexp.last_match(1).to_i
              Array(field_value).each do |record|
                bytes = record.respond_to?(:bytesize) ? record.bytesize : nil
                raise ArgumentError, "record is not serializable for #{field_name}" unless bytes.is_a?(Integer)
                raise ArgumentError, "record exceeds Rust byte limit for #{field_name}" if bytes > maximum
              end
            when /^max_command_bytes:(\d+)$/
              maximum = Regexp.last_match(1).to_i
              Array(field_value).each do |command|
                bytes = if command.respond_to?(:to_proto)
                  command.to_proto.bytesize
                elsif command.respond_to?(:serialize_to_string)
                  command.serialize_to_string.bytesize
                end
                raise ArgumentError, "command is not serializable for #{field_name}" unless bytes.is_a?(Integer)
                raise ArgumentError, "command exceeds Rust byte limit for #{field_name}" if bytes > maximum
              end
            when "atomic_precondition"
              arms = if field_value.is_a?(Hash)
                %w[if_absent if_match].count { |arm| field_value.key?(arm) || field_value.key?(arm.to_sym) }
              elsif field_value.respond_to?(:which_oneof)
                field_value.which_oneof(:condition) ? 1 : 0
              else
                0
              end
              raise ArgumentError, "preconditions must select exactly one condition" unless arms == 1
            when /^cross_field:(.+)\.required$/
              raise ArgumentError, "required cross-field value missing for #{field_name}" if field_value.nil? || (field_value.respond_to?(:empty?) && field_value.empty?)
            when /^cross_field:(.+)$/
              # Rust retains multi-field and provider rules in SHAPES; a
              # client never guesses those rules from partial local state.
            when "bucket_must_be_empty"
              # Emptiness is provider state and remains a Rust-owned server
              # admission check, never a fabricated client snapshot.\n            when /^fixed_length:(\d+)$/
              raise ArgumentError, "invalid #{field_name}" if field_value.respond_to?(:bytesize) && field_value.bytesize != Regexp.last_match(1).to_i
            when /^max_bytes:(\d+)$/
              raise ArgumentError, "invalid #{field_name}" if field_value.respond_to?(:bytesize) && field_value.bytesize > Regexp.last_match(1).to_i
            when /^max_items:(\d+)$/
              raise ArgumentError, "invalid #{field_name}" if field_value.respond_to?(:length) && field_value.length > Regexp.last_match(1).to_i
            when /^bounded_integer:(-?\d+):(-?\d+)$/
              minimum = Regexp.last_match(1).to_i
              maximum = Regexp.last_match(2).to_i
              raise ArgumentError, "invalid #{field_name}" if field_value.is_a?(Numeric) && (field_value < minimum || field_value > maximum)
            when "sha256_digest"
              raise ArgumentError, "invalid #{field_name}" if field_value.respond_to?(:bytesize) && field_value.bytesize != 32
            end
          end
          type_name = field["type_name"].to_s.sub(/^\./, "")
          next if type_name.empty?
          if field["repeated"] && (field_value.is_a?(Array) || (field_value.respond_to?(:each) && !field_value.is_a?(Hash) && !field_value.is_a?(String)))
            field_value.each { |item| validate_nested!(fields, type_name, item, seen.dup, depth + 1) }
          else
            validate_nested!(fields, type_name, field_value, seen.dup, depth + 1)
          end
        end
        value
      end

      def validate_shape!(shape, value)
        return value unless value
        validate_nested!(shape.fetch("fields"), shape.fetch("message"), value, {}, 0)
        value
      end

      module_function :validate_nested!, :validate_shape!
      "###);
    output
}

fn php_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn php_shape_field(field: &ResolvedRequestField) -> String {
    let constraints = php_string_list(&shape_constraints(field));
    format!(
        "['path' => {:?}, 'field' => {:?}, 'number' => {}, 'json_name' => {:?}, 'type_name' => {}, 'wire_type' => {}, 'label' => {}, 'oneof_index' => {}, 'presence' => {}, 'required' => {}, 'repeated' => {}, 'proto3_optional' => {}, 'semantic_type' => {}, 'validation' => {}, 'constraints' => [{}], 'preserve_unknown_enum' => {}, 'preserve_unknown_oneof' => {}]",
        format!("{}.{}", field.message_path, field.field),
        field.field,
        field.number,
        field.json_name,
        shape_optional_string(field.type_name.as_deref(), "null"),
        shape_optional_i32(field.wire_type, "null"),
        shape_optional_i32(field.label, "null"),
        shape_optional_i32(field.oneof_index, "null"),
        shape_presence(field),
        shape_required(field),
        shape_repeated(field),
        field.proto3_optional,
        shape_optional_string(field.semantic_type.as_deref(), "null"),
        format!("[{}]", php_string_list(&field.validation_rules)),
        constraints,
        field.wire_type == Some(14),
        field.oneof_index.is_some(),
    )
}

fn render_php_shape_fields(output: &mut String, fields: &[ResolvedRequestField], rpc: &str) {
    for field in fields.iter().filter(|field| field.rpc == rpc) {
        output.push_str("\n                ");
        output.push_str(&php_shape_field(field));
        output.push(',');
    }
}

fn render_php_shapes() -> String {
    let (methods, requests, responses) = resolved_shape_models();
    let mut output = String::from("    public const SHAPES = [\n");
    for method in methods {
        output.push_str(&format!(
            "        {:?} => ['family' => {:?}, 'service' => {:?}, 'method' => {:?}, 'request' => ['message' => {:?}, 'fields' => [",
            method.rpc, method.family, method.service, method.method, method.input_message
        ));
        render_php_shape_fields(&mut output, &requests, &method.rpc);
        output.push_str(&format!(
            "]], 'response' => ['message' => {:?}, 'fields' => [",
            method.output_message
        ));
        render_php_shape_fields(&mut output, &responses, &method.rpc);
        output.push_str(&format!(
            "]], 'client_streaming' => {}, 'server_streaming' => {}],\n",
            method.client_streaming, method.server_streaming
        ));
    }
    output.push_str("    ];\n\n    public static function requestShape(string $rpc, ?string $family = null): array\n    {\n        return self::SHAPES[self::shapeKey($rpc, $family)]['request'] ?? throw new \\InvalidArgumentException(\"unknown RPC shape: {$rpc}\");\n    }\n\n    public static function responseShape(string $rpc, ?string $family = null): array\n    {\n        return self::SHAPES[self::shapeKey($rpc, $family)]['response'] ?? throw new \\InvalidArgumentException(\"unknown RPC shape: {$rpc}\");\n    }\n\n    private static function shapeKey(string $rpc, ?string $family): string\n    {\n        if (isset(self::SHAPES[$rpc]) && ($family === null || self::SHAPES[$rpc]['family'] === $family)) return $rpc;\n        foreach (self::SHAPES as $candidate => $shape) {\n            if (str_ends_with(strtolower($candidate), '/' . strtolower($rpc)) && ($family === null || $shape['family'] === $family)) return $candidate;\n        }\n        return $rpc;\n    }\n\n    public static function constructRequest(string $rpc, mixed $values = [], ?string $family = null): mixed\n    {\n        return self::validateRequest($rpc, $values, $family);\n    }\n\n    public static function validateRequest(string $rpc, mixed $request, ?string $family = null): mixed\n    {\n        return self::validateShape(self::requestShape($rpc, $family), $request);\n    }\n\n    public static function validateResponse(string $rpc, mixed $response, ?string $family = null): mixed\n    {\n        return self::validateShape(self::responseShape($rpc, $family), $response);\n    }\n\n    public static function preserveUnknown(mixed $value): mixed\n    {\n        return $value;\n    }\n\n    private static function validateShape(array $shape, mixed $value): mixed\n    {\n        if ($value === null) return $value;\n        self::validateNested($shape['fields'], $shape['message'], $value, [], 0);\n        foreach ($shape['fields'] as $field) {\n            if (!($field['required'] ?? false)) continue;\n            $name = $field['field'];\n            $getter = 'get' . str_replace(' ', '', ucwords(str_replace('_', ' ', $name)));\n            $present = is_array($value) ? array_key_exists($name, $value) : (method_exists($value, $getter) && $value->{$getter}() !== null);\n            if (!$present) throw new \\InvalidArgumentException(\"required field missing: {$name}\");\n        }\n        return $value;\n    }\n\n");
    output.push_str(r###"    private static function validateNested(array $fields, string $message, mixed $value, array $seen, int $depth = 0): mixed
    {
        if ($value === null || $depth >= 64) return $value;
        if (is_object($value)) {
            $identity = spl_object_id($value);
            if (isset($seen[$identity])) return $value;
            $seen[$identity] = true;
        }
        foreach ($fields as $field) {
            $path = (string) ($field['path'] ?? '');
            if (!str_starts_with($path, $message . '.') || substr_count($path, '.') !== substr_count($message, '.') + 1) continue;
            $name = (string) $field['field'];
            if (is_array($value)) {
                $present = array_key_exists($name, $value);
                $fieldValue = $value[$name] ?? null;
            } else {
                $getter = 'get' . str_replace(' ', '', ucwords(str_replace('_', ' ', $name)));
                $present = method_exists($value, $getter);
                $fieldValue = $present ? $value->{$getter}() : null;
                if ($present) {
                    $has = 'has' . str_replace(' ', '', ucwords(str_replace('_', ' ', $name)));
                    if (method_exists($value, $has)) {
                        $present = (bool) $value->{$has}();
                    } elseif (method_exists($value, 'serializeToJsonString')) {
                        $json = json_decode($value->serializeToJsonString(), true);
                        $present = is_array($json) && array_key_exists((string) ($field['json_name'] ?? $name), $json);
                    } elseif (($field['required'] ?? false) === true) {
                        // Required proto3 scalars have no presence bit.  Keep
                        // their getter default so Rust semantic checks run.
                        $present = true;
                    } elseif (is_scalar($fieldValue) || $fieldValue === null) {
                        // Ordinary proto3 scalar defaults are absent unless
                        // the schema explicitly grants presence.
                        $present = false;
                    }
                }
            }
            if (($field['required'] ?? false) && !$present && $fieldValue !== null) $present = true;
            if (($field['required'] ?? false) && !$present) throw new \InvalidArgumentException("required field missing: {$name}");
            if (!$present) continue;
            foreach (($field['constraints'] ?? []) as $constraint) {
                if ($constraint === 'non_empty' && is_string($fieldValue) && $fieldValue === '') throw new \InvalidArgumentException("invalid field: {$name}");
                if ($constraint === 'utf8' && is_string($fieldValue) && preg_match('//u', $fieldValue) !== 1) throw new \InvalidArgumentException("invalid field: {$name}");
                if ($constraint === 'non_negative' && is_numeric($fieldValue) && $fieldValue < 0) throw new \InvalidArgumentException("invalid field: {$name}");
                if ($constraint === 'strictly_positive' && is_numeric($fieldValue) && $fieldValue <= 0) throw new \InvalidArgumentException("invalid field: {$name}");
                if (str_starts_with($constraint, 'operation_capability:')) {
                    $capability = substr($constraint, strlen('operation_capability:'));
                    $capabilities = is_array($fieldValue) ? ($fieldValue['capabilities'] ?? null) : (method_exists($fieldValue, 'getCapabilities') ? $fieldValue->getCapabilities() : null);
                    if ($capabilities instanceof \Traversable) $capabilities = iterator_to_array($capabilities);
                    if (!is_array($capabilities) || !in_array($capability, $capabilities, true)) throw new \InvalidArgumentException("missing operation capability {$capability} for {$name}");
                }
                if (str_starts_with($constraint, 'ordered_parts:')) {
                    $limits = array_map('intval', explode(':', substr($constraint, strlen('ordered_parts:'))));
                    $previous = 0;
                    if ($fieldValue instanceof \Traversable) $fieldValue = iterator_to_array($fieldValue);
                    if (!is_array($fieldValue) || count($fieldValue) > ($limits[0] ?? 0)) throw new \InvalidArgumentException("invalid ordered parts for {$name}");
                    foreach ($fieldValue as $part) {
                        $number = is_array($part) ? ($part['part_number'] ?? null) : (method_exists($part, 'getPartNumber') ? $part->getPartNumber() : null);
                        if (!is_int($number) || $number <= $previous || $number > ($limits[1] ?? 0)) throw new \InvalidArgumentException("invalid ordered parts for {$name}");
                        $previous = $number;
                    }
                }
                if (str_starts_with($constraint, 'max_record_bytes:')) {
                    $maximum = (int) substr($constraint, strlen('max_record_bytes:'));
                    if ($fieldValue instanceof \Traversable) $fieldValue = iterator_to_array($fieldValue);
                    if (!is_array($fieldValue)) throw new \InvalidArgumentException("record is not serializable for {$name}");
                    foreach ($fieldValue as $record) {
                        if (!is_string($record)) throw new \InvalidArgumentException("record is not serializable for {$name}");
                        if (strlen($record) > $maximum) throw new \InvalidArgumentException("record exceeds Rust byte limit for {$name}");
                    }
                }
                if (str_starts_with($constraint, 'max_command_bytes:')) {
                    $maximum = (int) substr($constraint, strlen('max_command_bytes:'));
                    if ($fieldValue instanceof \Traversable) $fieldValue = iterator_to_array($fieldValue);
                    if (!is_array($fieldValue)) throw new \InvalidArgumentException("command is not serializable for {$name}");
                    foreach ($fieldValue as $command) {
                        if (!is_object($command) || !method_exists($command, 'serializeToString')) throw new \InvalidArgumentException("command is not serializable for {$name}");
                        if (strlen($command->serializeToString()) > $maximum) throw new \InvalidArgumentException("command exceeds Rust byte limit for {$name}");
                    }
                }
                if ($constraint === 'atomic_precondition') {
                    $arms = 0;
                    if (is_array($fieldValue)) {
                        $arms = (int) array_key_exists('if_absent', $fieldValue) + (int) array_key_exists('if_match', $fieldValue)
                            + (int) array_key_exists('ifAbsent', $fieldValue) + (int) array_key_exists('ifMatch', $fieldValue);
                    } elseif (is_object($fieldValue) && method_exists($fieldValue, 'serializeToJsonString')) {
                        $json = json_decode($fieldValue->serializeToJsonString(), true);
                        $arms = is_array($json) ? (int) array_key_exists('ifAbsent', $json) + (int) array_key_exists('ifMatch', $json) : 0;
                    }
                    if ($arms !== 1) throw new \InvalidArgumentException("preconditions must select exactly one condition");
                }
                if (str_starts_with($constraint, 'cross_field:') && str_ends_with($constraint, '.required')) {
                    if ($fieldValue === null || (is_string($fieldValue) && $fieldValue === '') || (is_array($fieldValue) && count($fieldValue) === 0)) {
                        throw new \InvalidArgumentException("required cross-field value missing: {$name}");
                    }
                }
                // Bucket emptiness is provider state.  The structured Rust
                // rule remains in SHAPES for server admission and is not
                // approximated with a client-side snapshot.
            }
            $typeName = ltrim((string) ($field['type_name'] ?? ''), '.');
            if ($typeName === '') continue;
            if (($field['repeated'] ?? false) && is_array($fieldValue)) {
                foreach ($fieldValue as $item) self::validateNested($fields, $typeName, $item, $seen, $depth + 1);
            } else {
                self::validateNested($fields, $typeName, $fieldValue, $seen, $depth + 1);
            }
        }
        return $value;
    }

"###);
    output
}

fn dart_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn dart_shape_field(field: &ResolvedRequestField) -> String {
    let constraints = dart_string_list(&shape_constraints(field));
    format!(
        "<String, Object?>{{'path': {:?}, 'field': {:?}, 'number': {}, 'jsonName': {:?}, 'typeName': {}, 'wireType': {}, 'label': {}, 'oneofIndex': {}, 'presence': {}, 'required': {}, 'repeated': {}, 'proto3Optional': {}, 'semanticType': {}, 'validation': <String>[{}], 'constraints': <String>[{}], 'preserveUnknownEnum': {}, 'preserveUnknownOneof': {}}}",
        format!("{}.{}", field.message_path, field.field),
        field.field,
        field.number,
        field.json_name,
        shape_optional_string(field.type_name.as_deref(), "null"),
        shape_optional_i32(field.wire_type, "null"),
        shape_optional_i32(field.label, "null"),
        shape_optional_i32(field.oneof_index, "null"),
        shape_presence(field),
        shape_required(field),
        shape_repeated(field),
        field.proto3_optional,
        shape_optional_string(field.semantic_type.as_deref(), "null"),
        dart_string_list(&field.validation_rules),
        constraints,
        field.wire_type == Some(14),
        field.oneof_index.is_some(),
    )
}

fn render_dart_shape_fields(output: &mut String, fields: &[ResolvedRequestField], rpc: &str) {
    for field in fields.iter().filter(|field| field.rpc == rpc) {
        output.push_str("\n        ");
        output.push_str(&dart_shape_field(field));
        output.push(',');
    }
}

fn render_dart_shapes() -> String {
    let (methods, requests, responses) = resolved_shape_models();
    let mut output = String::from("const generatedRemoteRpcShapes = <String, Map<String, Object?>>{\n");
    for method in methods {
        output.push_str(&format!(
            "  {:?}: <String, Object?>{{'family': {:?}, 'service': {:?}, 'method': {:?}, 'request': <String, Object?>{{'message': {:?}, 'fields': <Map<String, Object?>>[",
            method.rpc, method.family, method.service, method.method, method.input_message
        ));
        render_dart_shape_fields(&mut output, &requests, &method.rpc);
        output.push_str(&format!(
            "]}}, 'response': <String, Object?>{{'message': {:?}, 'fields': <Map<String, Object?>>[",
            method.output_message
        ));
        render_dart_shape_fields(&mut output, &responses, &method.rpc);
        output.push_str(&format!(
            "]}}, 'clientStreaming': {}, 'serverStreaming': {}}},\n",
            method.client_streaming, method.server_streaming
        ));
    }
    output.push_str("};\n\n");
    output
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
        facade_operations, generate_jvm_semantic_types, generate_jvm_typed_clients,
        generate_jvm_typed_requests,
        generate_jvm_typed_responses,
        generate_remote_facade,
        generate_remote_facades,
        resolved_shape_models,
    };
    use crate::family_registry::FAMILY_VIEWS;

    #[test]
    fn every_target_is_emitted_from_the_registry() {
        let outputs = generate_remote_facades();
        assert_eq!(outputs.len(), 7);
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
    fn jvm_typed_requests_bind_every_rust_owned_request_field() {
        let outputs = generate_jvm_typed_requests();
        assert_eq!(outputs.len(), 3);
        for (path, source) in outputs {
            assert!(source.contains("RustTypedRequests"), "{path} missing facade");
            for binding in crate::type_policy::PUBLIC_FIELD_BINDINGS {
                if binding.direction == crate::type_policy::PublicFieldDirection::Request {
                    assert!(source.contains(&binding.field.replace('_', "")) || source.contains(binding.field), "{path} missing {}", binding.field);
                    let semantic = crate::type_policy::semantic_type(binding.semantic_type).expect("binding semantic type");
                    assert!(source.contains(semantic.rust_name), "{path} missing semantic binding {}", semantic.rust_name);
                }
            }
        }
    }

    #[test]
    fn jvm_typed_responses_are_lossless_and_semantic() {
        let outputs = generate_jvm_typed_responses();
        assert_eq!(outputs.len(), 3);
        for (path, source) in outputs {
            assert!(source.contains("RustTypedResponses"), "{path} missing facade");
            assert!(source.contains("fromWire"), "{path} missing wire decoder");
            assert!(source.contains("WireChoice"), "{path} missing open union");
            assert!(source.contains("preserveUnknown"), "{path} drops unknown union arms");
        }
    }


    #[test]
    fn jvm_typed_clients_bind_every_rust_owned_request_field_and_rpc() {
        let outputs = generate_jvm_typed_clients();
        assert_eq!(outputs.len(), 3);
        for (path, source) in outputs {
            assert!(source.contains("RustTypedClients"), "{path} missing client facade");
            assert!(source.contains("stub."), "{path} does not invoke a generated stub");
            for binding in crate::type_policy::PUBLIC_FIELD_BINDINGS {
                if binding.direction == crate::type_policy::PublicFieldDirection::Request {
                    let semantic = crate::type_policy::semantic_type(binding.semantic_type)
                        .expect("binding semantic type");
                    assert!(
                        source.contains(semantic.rust_name),
                        "{path} missing semantic client binding {}",
                        semantic.rust_name
                    );
                    assert!(
                        source.contains(binding.field),
                        "{path} missing client parameter {}",
                        binding.field
                    );
                    let rpc = binding.rpc().expect("request RPC");
                    let rpc = rpc[..1].to_ascii_lowercase() + &rpc[1..];
                    assert!(
                        source.contains(&rpc),
                        "{path} missing client RPC for {}",
                        binding.field
                    );
                }
            }
        }
    }

    #[test]
    fn generated_policy_has_the_rust_transport_kinds() {
        for language in [
            FacadeLanguage::Python,
            FacadeLanguage::Go,
            FacadeLanguage::Ruby,
            FacadeLanguage::Php,
            FacadeLanguage::Dart,
            FacadeLanguage::Java,
            FacadeLanguage::Csharp,
        ] {
            let output = generate_remote_facade(language);
            let expected = match language {
                FacadeLanguage::Python | FacadeLanguage::Go => ["grpc", "grpc_web", "http_json"],
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
            FacadeLanguage::Python,
            FacadeLanguage::Go,
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
                        (FacadeLanguage::Python | FacadeLanguage::Go, crate::TransportKind::Grpc) => "grpc",
                        (FacadeLanguage::Python | FacadeLanguage::Go, crate::TransportKind::GrpcWeb) => "grpc_web",
                        (FacadeLanguage::Python | FacadeLanguage::Go, crate::TransportKind::HttpJson) => "http_json",
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
                        (FacadeLanguage::Python | FacadeLanguage::Go, crate::TransportKind::Grpc) => "grpc",
                        (FacadeLanguage::Python | FacadeLanguage::Go, crate::TransportKind::GrpcWeb) => "grpc_web",
                        (FacadeLanguage::Python | FacadeLanguage::Go, crate::TransportKind::HttpJson) => "http_json",
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
    fn operation_metadata_is_descriptor_linked_and_uses_family_auth_policy() {
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
                if family.name == "machines" {
                    assert!(
                        !operation.bearer_auth,
                        "Machines must use native mTLS metadata"
                    );
                } else {
                    assert!(
                        operation.bearer_auth,
                        "{} missing bearer policy",
                        operation.rpc
                    );
                }
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
            FacadeLanguage::Python,
            FacadeLanguage::Go,
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
                output.source.contains("cancellation") || output.source.contains("Cancellation")
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

    #[test]
    fn portable_facades_emit_every_rust_resolved_request_and_response_shape() {
        let (methods, requests, responses) = resolved_shape_models();
        assert!(!methods.is_empty());
        assert!(methods.iter().any(|method| {
            requests.iter().all(|field| field.rpc != method.rpc)
                || responses.iter().all(|field| field.rpc != method.rpc)
        }), "the source model must retain empty request/response methods");
        for language in [FacadeLanguage::Ruby, FacadeLanguage::Php, FacadeLanguage::Dart] {
            let source = generate_remote_facade(language).source;
            assert!(source.contains("SHAPES") || source.contains("generatedRemoteRpcShapes"));
            assert!(source.contains("validate_request") || source.contains("validateRequest"));
            assert!(source.contains("construct_request") || source.contains("constructRequest"));
            assert!(source.contains("preserve_unknown") || source.contains("preserveUnknown"));
            assert!(source.contains("presence"));
            assert!(source.contains("preserve_unknown_enum") || source.contains("preserveUnknownEnum"));
            assert!(source.contains("preserve_unknown_oneof") || source.contains("preserveUnknownOneof"));
            for method in &methods {
                assert!(
                    source.contains(&format!("{:?}", method.rpc)),
                    "{} omitted {}",
                    language.name(),
                    method.rpc
                );
            }
        }
    }

    #[test]
    fn python_and_go_machine_ids_cross_the_wire_as_nominal_values() {
        let python = generate_remote_facade(FacadeLanguage::Python).source;
        assert!(python.contains("machines_pb2.MachineId(value=machine_id(self.machine_id))"));
        assert!(python.contains("machines_pb2.MachineId(value="));
        let go = generate_remote_facade(FacadeLanguage::Go).source;
        assert!(go.contains("&machinesv1.MachineId{Value: value[:]}"));
    }
}






