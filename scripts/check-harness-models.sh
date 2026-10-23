#!/usr/bin/env bash
set -euo pipefail

# Ordinary all-feature tests do not compile the Loom dependency composition.
# Each qualification compiles that composition and checks complete model counts.
node scripts/qualify-mcp-loom.mjs --child-views --all-features
node scripts/qualify-mcp-loom.mjs --harness-models --all-features
node scripts/qualify-mcp-loom.mjs --query-model --all-features
