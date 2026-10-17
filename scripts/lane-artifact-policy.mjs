// Product documentation and byte-input fixtures are the only places for Markdown.
// Reserved operational names still lose inside documentation directories.
const rootDocs = new Set([
  'ARCHITECTURE.md', 'CHANGELOG.md', 'CODE_OF_CONDUCT.md', 'CONTRIBUTING.md',
  'DCO.md', 'NATIVE_MOUNT_QUALIFICATION.md', 'README.md', 'SECURITY.md',
  '.github/pull_request_template.md',
]);
const specialDocs = new Set([
  'arena/evals/REPORT.md', 'arena/evals/labelbias/REPORT.md',
  'arena/evals/variance/REPORT.md', 'arena/evals/variance/variance-space.md',
  'arena/evals/variance/variance.md', 'arena/evals/published/2026-09-22-first.md',
  'ffi/abi-policy.md', 'plugin/identity-api.md', 'plugin/identity-mcp.md',
  'rust/crates/conformance/S3_STORAGE_DESIGN.md', 'rust/crates/filesystem/KANI.md',
  'rust/crates/filesystem/native-exchange-contract.md',
  'rust/crates/sdk-docs/proofs/safe-version-injectivity.md',
  'rust/vendor/tonic-web-wasm-client-0.9.2/ACYCLIC_PROVENANCE.md',
]);

export function laneArtifactViolation(path) {
  const lower = path.toLowerCase();
  const name = lower.slice(lower.lastIndexOf('/') + 1);
  // These are test inputs, not lane artifacts. Keep this exception scoped to
  // actual Rust fixture/corpus directories, never a general "test" exemption.
  if (/^rust\/(?:crates\/[^/]+|fuzz\/[^/]+)\/(?:tests\/fixtures|fuzz\/corpus|corpus)\//.test(path)) return;
  if (/(?:^|\/)(?:scratch|scratch-notes|lane-logs|lane-artifacts|study-classes-evidence)(?:\/|$)/.test(lower)
      || /(?:^|\/)rev-\d+[^/]*verdict[^/]*$/.test(lower)
      || /(?:^|[-_.])(?:verdict|lane-log|build-receipt|scratch-notes)(?:[-_.]|$)/.test(name)
      || /\.log$/.test(lower)) return 'lane artifacts belong outside the product repository';
  if (!/\.(?:md|markdown|mdown)$/i.test(path)) return;
  if (rootDocs.has(path) || specialDocs.has(path)
      || /^(?:arena|conformance|examples|ffi|languages|plugin|proto|provenance|release|rust|scripts\/vendor|tools\/sdk-generator|typescript\/packages)\/(?:[^/]+\/)*(?:README|CHANGELOG)\.md$/.test(path)
      || /^docs\/[^/]+\.md$/.test(path)
      || /^rust\/crates\/harness\/docs\/(?:client\/)?[^/]+\.md$/.test(path)
      || /^typescript\/packages\/[^/]+\/docs\/[^/]+\.md$/.test(path)) return;
  return 'Markdown path is outside the product documentation and fixture allowlist';
}
