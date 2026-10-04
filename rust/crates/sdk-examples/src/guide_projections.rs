use crate::{
    CapabilityStatus, Language, RenderedSnippet, ScenarioMetadata, ValidationLevel,
    ValidationReceipt, ValidationStatus, filesystem_scenarios, harness_scenarios,
    inference_scenarios, machines_scenarios, objects_scenarios, workers_scenarios,
};

/// Whether a projection exercises a native Rust facade or the remote SDK.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuideProjectionMode {
    /// The Rust-owned embedded provider or native facade.
    Embedded,
    /// The generated remote client and its transport.
    Remote,
}

/// A Rust-owned guide scenario projected onto one generated language package.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuideProjection {
    pub scenario_id: &'static str,
    pub family: &'static str,
    pub source: &'static str,
    pub operation: &'static str,
    pub language: Language,
    pub mode: GuideProjectionMode,
    pub capability: CapabilityStatus,
    pub package: GuidePackageSpec,
    pub code: String,
}

/// Installable artifact identity used to compile a projection against the
/// package produced by the current generation revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuidePackageSpec {
    pub package_manager: &'static str,
    pub package_name: &'static str,
    pub artifact_path: &'static str,
}

/// Typed request identity consumed by package and documentation validators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuideRemoteRequestSpec {
    pub scenario_id: &'static str,
    pub family: &'static str,
    pub operation: &'static str,
    pub service: &'static str,
    pub method: &'static str,
    pub request: &'static str,
}

/// The six guide families with a generated remote or native facade.
pub const GUIDE_PROJECTION_SCENARIOS: [(&str, &str, &str); 6] = [
    (
        filesystem_scenarios::SCENARIO_ID,
        "filesystem",
        filesystem_scenarios::SOURCE,
    ),
    (
        harness_scenarios::SCENARIO_ID,
        "harness",
        harness_scenarios::SOURCE,
    ),
    (
        inference_scenarios::SCENARIO_ID,
        "inference",
        inference_scenarios::SOURCE,
    ),
    (
        machines_scenarios::SCENARIO_ID,
        "machines",
        machines_scenarios::SOURCE,
    ),
    (
        objects_scenarios::SCENARIO_ID,
        "objects",
        objects_scenarios::SOURCE,
    ),
    (
        workers_scenarios::SCENARIO_ID,
        "workers",
        workers_scenarios::SOURCE,
    ),
];

/// One typed request manifest for each remote guide scenario.
pub const GUIDE_REMOTE_REQUESTS: [GuideRemoteRequestSpec; 6] = [
    GuideRemoteRequestSpec {
        scenario_id: filesystem_scenarios::SCENARIO_ID,
        family: "filesystem",
        operation: "acyclic.filesystem.v2.FilesystemService/Handshake",
        service: "FilesystemService",
        method: "Handshake",
        request: "HandshakeRequest",
    },
    GuideRemoteRequestSpec {
        scenario_id: harness_scenarios::SCENARIO_ID,
        family: "harness",
        operation: "acyclic.harness.v2.HarnessService/Submit",
        service: "HarnessService",
        method: "Submit",
        request: "CommandEnvelope",
    },
    GuideRemoteRequestSpec {
        scenario_id: inference_scenarios::SCENARIO_ID,
        family: "inference",
        operation: "acyclic.inference.v1.RunsService/Watch",
        service: "RunsService",
        method: "Watch",
        request: "WatchRunRequest",
    },
    GuideRemoteRequestSpec {
        scenario_id: machines_scenarios::SCENARIO_ID,
        family: "machines",
        operation: "acyclic.machines.v1.MachinesService/Create",
        service: "MachinesService",
        method: "Create",
        request: "CreateMachineRequest",
    },
    GuideRemoteRequestSpec {
        scenario_id: objects_scenarios::SCENARIO_ID,
        family: "objects",
        operation: "acyclic.objects.v2.ObjectsService/PutObject",
        service: "ObjectsService",
        method: "PutObject",
        request: "PutObjectRequest",
    },
    GuideRemoteRequestSpec {
        scenario_id: workers_scenarios::SCENARIO_ID,
        family: "workers",
        operation: "acyclic.workers.v1.WorkersService/PublishVersion",
        service: "WorkersService",
        method: "PublishVersion",
        request: "PublishVersionRequest",
    },
];

fn rust_body(scenario_id: &str) -> Option<String> {
    Some(match scenario_id {
        filesystem_scenarios::SCENARIO_ID => filesystem_scenarios::QUICKSTART_SNIPPET.to_owned(),
        harness_scenarios::SCENARIO_ID => harness_scenarios::QUICKSTART_SNIPPET.to_owned(),
        inference_scenarios::SCENARIO_ID => inference_scenarios::rust_snippet().to_owned(),
        machines_scenarios::SCENARIO_ID => machines_scenarios::rust_snippet().to_owned(),
        objects_scenarios::SCENARIO_ID => objects_scenarios::rust_snippet().to_owned(),
        workers_scenarios::SCENARIO_ID => workers_scenarios::rust_snippet(),
        _ => return None,
    })
}

fn package_module(family: &str) -> Option<(&'static str, &'static str)> {
    Some(match family {
        "filesystem" => ("filesystem", "v2"),
        "harness" => ("harness", "v2"),
        "inference" => ("inference", "v1"),
        "machines" => ("machines", "v1"),
        "objects" => ("objects", "v2"),
        "workers" => ("workers", "v1"),
        _ => return None,
    })
}

fn remote_operation(
    family: &str,
) -> Option<(&'static str, &'static str, &'static str, &'static str)> {
    Some(match family {
        "filesystem" => (
            "FilesystemService",
            "Handshake",
            "HandshakeRequest",
            "acyclic.filesystem.v2.FilesystemService/Handshake",
        ),
        "harness" => (
            "HarnessService",
            "Submit",
            "CommandEnvelope",
            "acyclic.harness.v2.HarnessService/Submit",
        ),
        "inference" => (
            "RunsService",
            "Watch",
            "WatchRunRequest",
            "acyclic.inference.v1.RunsService/Watch",
        ),
        "machines" => (
            "MachinesService",
            "Create",
            "CreateMachineRequest",
            "acyclic.machines.v1.MachinesService/Create",
        ),
        "objects" => (
            "ObjectsService",
            "PutObject",
            "PutObjectRequest",
            "acyclic.objects.v2.ObjectsService/PutObject",
        ),
        "workers" => (
            "WorkersService",
            "PublishVersion",
            "PublishVersionRequest",
            "acyclic.workers.v1.WorkersService/PublishVersion",
        ),
        _ => return None,
    })
}

fn package_type(module: &str) -> Option<&'static str> {
    Some(match module {
        "filesystem" => "Filesystem",
        "harness" => "Harness",
        "inference" => "Inference",
        "machines" => "Machines",
        "objects" => "Objects",
        "workers" => "Workers",
        _ => return None,
    })
}

fn package_spec(family: &str, language: Language) -> Option<GuidePackageSpec> {
    let package = match language {
        Language::Rust => GuidePackageSpec {
            package_manager: "cargo",
            package_name: "acyclic-sdk-bundle",
            artifact_path: "qualification/packages/*-sdk-package.tgz",
        },
        Language::Python => GuidePackageSpec {
            package_manager: "pip",
            package_name: "acyclic-sdk-transport",
            artifact_path: "python/dist/*.whl",
        },
        Language::TypeScript => GuidePackageSpec {
            package_manager: "npm",
            package_name: match family {
                "filesystem" => "@acyclic-labs/fs",
                "harness" => "@acyclic-labs/harness",
                "inference" => "@acyclic-labs/inference",
                "machines" => "@acyclic-labs/machines",
                "objects" => "@acyclic-labs/objects",
                "workers" => "@acyclic-labs/workers",
                _ => return None,
            },
            artifact_path: match family {
                "filesystem" => "typescript/packages/filesystem/package.json",
                "harness" => "typescript/packages/harness/package.json",
                "inference" => "typescript/packages/inference/package.json",
                "machines" => "typescript/packages/machines/package.json",
                "objects" => "typescript/packages/objects/package.json",
                "workers" => "typescript/packages/workers/package.json",
                _ => return None,
            },
        },
        Language::Go => GuidePackageSpec {
            package_manager: "go",
            package_name: "github.com/acyclic-labs/sdk/go",
            artifact_path: "go/go.mod",
        },
        Language::Java => GuidePackageSpec {
            package_manager: "maven",
            package_name: "dev.acyclic:acyclic-sdk-jvm-transport",
            artifact_path: "jvm/target/acyclic-sdk-jvm-transport-*.jar",
        },
        Language::CSharp => GuidePackageSpec {
            package_manager: "nuget",
            package_name: "Acyclic.Sdk.Transport",
            artifact_path: "dotnet/bin/**/Acyclic.Sdk.Transport.dll",
        },
        Language::Ruby => GuidePackageSpec {
            package_manager: "bundler",
            package_name: "acyclic-sdk",
            artifact_path: "ruby/acyclic-sdk.gemspec",
        },
        Language::Dart => GuidePackageSpec {
            package_manager: "pub",
            package_name: "acyclic_sdk",
            artifact_path: "dart/pubspec.yaml",
        },
        Language::Php => GuidePackageSpec {
            package_manager: "composer",
            package_name: "acyclic/sdk-transport",
            artifact_path: "php/composer.json",
        },
    };
    Some(package)
}

fn version_type(version: &str) -> Option<&'static str> {
    Some(match version {
        "v1" => "V1",
        "v2" => "V2",
        _ => return None,
    })
}

fn snake_case(value: &str) -> String {
    value
        .chars()
        .enumerate()
        .map(|(index, ch)| {
            if ch.is_ascii_uppercase() && index > 0 {
                format!("_{}", ch.to_ascii_lowercase())
            } else {
                ch.to_ascii_lowercase().to_string()
            }
        })
        .collect()
}

fn lower_camel(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

/// Projects the Rust scenario as an actual generated remote SDK call.
///
/// The request values are the same stable identities used by the Rust fixture.
/// Embedded behavior remains in the Rust projection, which calls the native
/// provider APIs in the source scenario.
pub fn project(scenario_id: &'static str, language: Language) -> Option<GuideProjection> {
    let (_, family, source) = GUIDE_PROJECTION_SCENARIOS
        .iter()
        .find(|(id, _, _)| *id == scenario_id)?;
    let family = *family;
    let source = *source;
    let (module, version) = package_module(family)?;
    let (service, method, request, operation) = remote_operation(family)?;
    let package_type = package_type(module)?;
    let version_type = version_type(version)?;
    let package = package_spec(family, language)?;
    let method_camel = lower_camel(method);
    let method_snake = snake_case(method);
    let ts_package = if family == "filesystem" { "fs" } else { module };
    let ts_call = if family == "objects" {
        "const response = await client.putObject((async function* () { yield { body: new TextEncoder().encode(\"hello\") }; })());"
    } else {
        "const response = await client.METHOD({});"
    }
    .replace("METHOD", &method_camel);
    let code = match language {
        Language::Rust => rust_body(scenario_id)?,
        Language::Python => format!(
            r#"# Rust scenario: {scenario_id}
import os
import grpc
from acyclic_sdk.generated.{module}.{version} import {module}_pb2, {module}_pb2_grpc

channel = grpc.insecure_channel(os.environ["FIXTURE_GRPC_ADDRESS"])
client = {module}_pb2_grpc.{service}Stub(channel)
request = {module}_pb2.{request}()
response = client.{method}(request)
print(response)"#,
        ),
        Language::TypeScript => format!(
            r#"// Rust scenario: {scenario_id}
import {{ createClient }} from "@connectrpc/connect";
import {{ createGrpcTransport }} from "@connectrpc/connect-node";
import {{ {service} }} from "@acyclic-labs/{ts_package}/proto";

const transport = createGrpcTransport({{ baseUrl: process.env.FIXTURE_GRPC_ADDRESS! }});
const client = createClient({service}, transport);
{ts_call}
console.log(response);"#,
        ),
        Language::Go => {
            let call = if family == "objects" {
                format!(
                    r#"stream, err := client.PutObject(ctx)
    if err != nil {{ panic(err) }}
    if err := stream.Send(&generated.PutObjectRequest{{Frame: &generated.PutObjectRequest_Body{{Body: []byte("hello")}}}}); err != nil {{ panic(err) }}
    response, err := stream.CloseAndRecv()"#
                )
            } else {
                format!(
                    "response, err := client.{method}(ctx, &generated.{request}{{}})",
                    method = method,
                    request = request
                )
            };
            format!(
                r#"// Rust scenario: {scenario_id}
package main

import (
    "context"
    "fmt"
    "os"
    generated "github.com/acyclic-labs/sdk/go/gen/{module}/{version}"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {{
    endpoint := os.Getenv("FIXTURE_GRPC_ADDRESS")
    conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {{ panic(err) }}
    defer conn.Close()
    ctx := context.Background()
    client := generated.New{service}Client(conn)
    {call}
    if err != nil {{ panic(err) }}
    fmt.Println(response)
}}"#,
                call = call
            )
        }
        Language::Java => format!(
            r#"// Rust scenario: {scenario_id}
import io.grpc.ManagedChannelBuilder;
import acyclic.{module}.{version}.{package_type};
import acyclic.{module}.{version}.{package_type}ServiceGrpc;

public final class GuideSnippet {{
  public static void main(String[] args) {{
    var endpoint = System.getenv("FIXTURE_GRPC_ADDRESS");
    var channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
    try {{
      var request = {package_type}.{request}.newBuilder().build();
      var response = {package_type}ServiceGrpc.newBlockingStub(channel).{method_camel}(request);
      System.out.println(response);
    }} finally {{
      channel.shutdownNow();
    }}
  }}
}}"#,
        ),
        Language::CSharp => format!(
            r#"// Rust scenario: {scenario_id}
using Grpc.Net.Client;
using Acyclic.{package_type}.{version_type};

var endpoint = Environment.GetEnvironmentVariable("FIXTURE_GRPC_ADDRESS")
    ?? throw new InvalidOperationException("FIXTURE_GRPC_ADDRESS is required");
using var channel = GrpcChannel.ForAddress(endpoint);
var client = new {service}.{service}Client(channel);
var response = await client.{method}Async(new {request}());
Console.WriteLine(response);"#,
        ),
        Language::Ruby => format!(
            r#"# Rust scenario: {scenario_id}
require "acyclic_sdk"

endpoint = ENV.fetch("FIXTURE_GRPC_ADDRESS")
client = Acyclic::{package_type}::{version_type}::{service}::Stub.new(endpoint, :this_channel_is_insecure)
request = Acyclic::{package_type}::{version_type}::{request}.new
response = client.{method_snake}(request)
puts response"#,
        ),
        Language::Dart => format!(
            r#"// Rust scenario: {scenario_id}
import 'dart:io';
import 'package:grpc/grpc.dart';
import 'package:acyclic_sdk/src/generated/{module}/{version}/{module}.pb.dart' as generated;
import 'package:acyclic_sdk/src/generated/{module}/{version}/{module}.pbgrpc.dart' as rpc;

Future<void> main() async {{
  final endpoint = Platform.environment['FIXTURE_GRPC_ADDRESS']!;
  final parts = endpoint.split(':');
  final channel = ClientChannel(parts[0], port: int.parse(parts[1]),
      options: const ChannelOptions(credentials: ChannelCredentials.insecure()));
  try {{
    final client = rpc.{service}Client(channel);
    final response = await client.{method_camel}(generated.{request}());
    print(response);
  }} finally {{
    await channel.shutdown();
  }}
}}"#,
        ),
        Language::Php => format!(
            r#"<?php
// Rust scenario: {scenario_id}
require dirname(__DIR__) . '/vendor/autoload.php';

$endpoint = getenv('FIXTURE_GRPC_ADDRESS');
$client = new \Acyclic\{package_type}\{version}\{service}Client(
    $endpoint,
    ['credentials' => \Grpc\ChannelCredentials::createInsecure()]
);
$request = new \Acyclic\{package_type}\{version}\{request}();
[$response, $status] = $client->{method}($request)->wait();
if ($status->code !== \Grpc\STATUS_OK) throw new RuntimeException($status->details);
echo $response->serializeToJsonString(), PHP_EOL;"#,
        ),
    };
    Some(GuideProjection {
        scenario_id,
        family,
        source,
        operation,
        language,
        mode: if matches!(language, Language::Rust) {
            GuideProjectionMode::Embedded
        } else {
            GuideProjectionMode::Remote
        },
        capability: CapabilityStatus::Supported,
        package,
        code,
    })
}

/// Returns all six guide scenarios for every published language target.
pub fn all() -> Vec<GuideProjection> {
    GUIDE_PROJECTION_SCENARIOS
        .iter()
        .flat_map(|(scenario_id, _, _)| {
            Language::ALL
                .into_iter()
                .filter_map(|language| project(scenario_id, language))
        })
        .collect()
}

/// Converts a projection into the common generated-snippet metadata shape.
pub fn rendered(projection: GuideProjection) -> RenderedSnippet {
    RenderedSnippet {
        metadata: ScenarioMetadata {
            id: projection.scenario_id,
            family: projection.family,
            title: projection.operation,
            language: projection.language,
            source: projection.source,
            validation: ValidationReceipt {
                level: ValidationLevel::Rendered,
                status: ValidationStatus::NotRun,
                evidence: match projection.mode {
                    GuideProjectionMode::Embedded => {
                        "Rust native provider or facade scenario source"
                    }
                    GuideProjectionMode::Remote => {
                        "generated remote client call bound to the matching package artifact"
                    }
                },
            },
        },
        capability: projection.capability,
        code: projection.code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_guide_family_has_all_language_projections() {
        let projections = all();
        assert_eq!(
            projections.len(),
            GUIDE_PROJECTION_SCENARIOS.len() * Language::ALL.len()
        );
        for (scenario_id, _, source) in GUIDE_PROJECTION_SCENARIOS {
            for language in Language::ALL {
                let projection = project(scenario_id, language).expect("guide projection");
                assert_eq!(projection.source, source);
                assert!(!projection.package.package_name.is_empty());
                assert!(!projection.package.artifact_path.is_empty());
                if language == Language::Rust {
                    assert_eq!(projection.mode, GuideProjectionMode::Embedded);
                    assert!(!projection.code.is_empty());
                } else {
                    assert_eq!(projection.mode, GuideProjectionMode::Remote);
                    assert!(
                        projection.code.contains("client")
                            || projection.code.contains("Client")
                            || projection.code.contains("Stub")
                    );
                    assert!(projection.code.contains("Request"));
                }
            }
        }
    }

    #[test]
    fn rust_projection_is_the_scenario_source() {
        for (scenario_id, _, _) in GUIDE_PROJECTION_SCENARIOS {
            let projection = project(scenario_id, Language::Rust).expect("Rust projection");
            assert_eq!(
                projection.code,
                rust_body(scenario_id).expect("Rust scenario body")
            );
        }
    }

    #[test]
    fn request_manifest_matches_every_remote_projection() {
        assert_eq!(
            GUIDE_REMOTE_REQUESTS.len(),
            GUIDE_PROJECTION_SCENARIOS.len()
        );
        for spec in GUIDE_REMOTE_REQUESTS {
            for language in Language::ALL {
                let projection = project(spec.scenario_id, language).expect("projection");
                assert_eq!(projection.operation, spec.operation);
                if language != Language::Rust {
                    assert!(projection.code.contains(spec.request));
                }
            }
        }
    }
}
