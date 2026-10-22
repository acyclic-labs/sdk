#!/usr/bin/env bash
set -euo pipefail

binary="${1:?prebuilt harness test binary is required}"
: "${ACYCLIC_LOCAL_CLOUD:?provide a fixture endpoints file}"
# These probes own disjoint paths. Complete their catalog writes before
# conformance, whose hierarchy oracle requires an otherwise stable catalog.
"$binary" local_cloud::cloud5322:: --test-threads=5 --nocapture
"$binary" --exact local_cloud::scenario_families_match_between_memory_and_local_cloud_streams --nocapture
