use crate::{
    CapabilityStatus, Language, RenderedSnippet, ScenarioMetadata, ValidationLevel,
    ValidationReceipt, ValidationStatus, filesystem_scenarios, harness_scenarios,
    inference_scenarios, machines_scenarios, objects_scenarios, workers_scenarios,
};

/// A Rust-owned guide scenario projected onto one generated language package.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuideProjection {
    pub scenario_id: &'static str,
    pub family: &'static str,
    pub source: &'static str,
    pub language: Language,
    pub capability: CapabilityStatus,
    pub code: String,
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

/// Returns the Rust-owned scenario body used by the executable Rust consumer.
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

/// Generated package module identity for each guide family.
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

/// Generated type namespace used by language projections that capitalize the
/// service module name.
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

/// Projects a guide scenario as a package-bound generated-contract probe.
///
/// The non-Rust projections exercise generated package metadata and wire
/// descriptors. Stateful embedded behavior remains in the Rust facade, while
/// remote calls are supplied by each generated service client.
pub fn project(scenario_id: &str, language: Language) -> Option<GuideProjection> {
    let (_, family, source) = GUIDE_PROJECTION_SCENARIOS
        .iter()
        .find(|(id, _, _)| *id == scenario_id)?;
    let (module, version) = package_module(family)?;
    let type_name = package_type(module)?;
    let code = match language {
        Language::Rust => rust_body(scenario_id)?,
        Language::Python => format!(
            r#"# Rust scenario: {scenario_id}
from acyclic_sdk.generated.{module}.{version} import {module}_pb2
descriptor = {module}_pb2.DESCRIPTOR
assert descriptor.services_by_name
print(sorted(descriptor.services_by_name))"#,
        ),
        Language::TypeScript => format!(
            r#"// Rust scenario: {scenario_id}
import * as generated from "@acyclic-labs/{module}/proto";
const exported = Object.keys(generated).sort();
if (exported.length === 0) throw new Error("generated {module} facade is empty");
console.log(exported[0]);"#,
        ),
        Language::Go => format!(
            r#"// Rust scenario: {scenario_id}
package main

import (
    "fmt"
    generated "github.com/acyclic-labs/sdk/go/gen/{module}/{version}"
)

func main() {{
    file := generated.File_{module}_{version}_{module}_proto
    if file == nil {{ panic("generated descriptor is missing") }}
    fmt.Println(file.Path())
}}"#,
        ),
        Language::Java => format!(
            r#"// Rust scenario: {scenario_id}
package dev.acyclic.generated;

import java.util.Objects;
import com.google.protobuf.Descriptors;
import dev.acyclic.{module}.{version}.{type_name};

final class GuideProbe {{
  static Descriptors.FileDescriptor descriptor() {{
    return Objects.requireNonNull({type_name}.getDescriptor());
  }}
}}"#,
        ),
        Language::CSharp => format!(
            r#"// Rust scenario: {scenario_id}
using System;
using Google.Protobuf.Reflection;
using Acyclic.{module}.{version};

var descriptor = {type_name}Reflection.Descriptor;
if (descriptor.Services.Count == 0) throw new InvalidOperationException("generated descriptor is empty");
Console.WriteLine(descriptor.Name);"#,
        ),
        Language::Ruby => format!(
            r#"# Rust scenario: {scenario_id}
require "acyclic/{module}/{version}/{module}_pb"
descriptor = Acyclic::{type_name}::{version}::{type_name}::DESCRIPTOR
abort "generated descriptor is empty" if descriptor.services.empty?
puts descriptor.name"#,
        ),
        Language::Dart => format!(
            r#"// Rust scenario: {scenario_id}
import 'package:acyclic_{module}/{module}_{version}.pb.dart';

void main() {{
  final descriptor = {type_name}Reflection.descriptor;
  if (descriptor.services.isEmpty) throw StateError('generated descriptor is empty');
  print(descriptor.name);
}}"#,
        ),
        Language::Php => format!(
            r#"<?php
// Rust scenario: {scenario_id}
require_once __DIR__ . '/vendor/autoload.php';
$descriptor = \Acyclic\{type_name}\{version}\{type_name}::descriptor();
if ($descriptor === null) throw new RuntimeException('generated descriptor is empty');
echo $descriptor->getName(), PHP_EOL;"#,
        ),
    };
    Some(GuideProjection {
        scenario_id,
        family,
        source,
        language,
        capability: CapabilityStatus::Supported,
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
            title: projection.scenario_id,
            language: projection.language,
            source: projection.source,
            validation: ValidationReceipt {
                level: ValidationLevel::Rendered,
                status: ValidationStatus::NotRun,
                evidence: "generated package descriptor probe; install and execute against the matching package artifact",
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
                assert!(projection.code.contains("Rust scenario"));
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
}
