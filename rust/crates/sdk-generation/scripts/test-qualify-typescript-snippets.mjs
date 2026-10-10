import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const hash = bytes => `sha256:${createHash('sha256').update(bytes).digest('hex')}`;
const packageReceipt = (directory, revision, packages) => {
  const source = hash('synthetic compiler source');
  const build = JSON.stringify({ schema: 'acyclic.typescript-build-receipt.v1', scope: 'typescript-compiler', source_commit: revision, source_sha256: source, rust_producers_qualified: false });
  writeFileSync(join(directory, 'BUILD.json'), build);
  writeFileSync(join(directory, 'QUALIFICATION.json'), JSON.stringify({ revision: 1, scope: 'typescript-compiler', source_commit: revision, source_sha256: source, compiler_build_receipt_sha256: hash(build), rust_producers_qualified: false, packages }));
};
for (const mutation of ['compiler-receipt', 'compiler-source', 'missing-compiler-provenance', 'sidecar-bytes', 'unregistered-id', 'execution-output', 'snippet-bytes', 'missing-eligible', 'ineligible-projection', 'missing-eligibility', 'duplicate-eligible', 'duplicate-projection']) {
  test(`${mutation} rejects qualification before install`, () => {
    const root = mkdtempSync(join(tmpdir(), 'docs-snippet-admission-'));
    try {
      const bundle = join(root, 'bundle'), packages = join(root, 'packages'), work = join(root, 'consumer');
      mkdirSync(bundle); mkdirSync(packages);
      const revision = 'a'.repeat(40), snippet = 'console.log("fixture");\n';
      const projection = { scenarioId: 'registered', package: '@acyclic-labs/demo', language: 'typescript', path: 'demo.ts', sourceSha256: hash('source'), rustOutputSha256: hash('output'), snippetSha256: hash(snippet) };
      if (mutation === 'unregistered-id') projection.scenarioId = 'invented';
      if (mutation === 'execution-output') projection.rustOutputSha256 = hash('invented');
      const artifacts = {};
      const bound = (name, data) => {
        const bytes = JSON.stringify(data); writeFileSync(join(bundle, name), bytes); artifacts[name] = hash(bytes);
      };
      bound('sdk-docs-scenario-projections.v1.json', { projections: mutation === 'duplicate-projection' ? [projection, projection] : [projection] });
      const scenarios = [{ id: 'registered', family: 'demo', sourceSha256: hash('source'), typescriptProjection: mutation !== 'ineligible-projection' }];
      if (mutation === 'missing-eligible') scenarios.push({ id: 'omitted', family: 'other', sourceSha256: hash('other-source'), typescriptProjection: true });
      if (mutation === 'duplicate-eligible') scenarios.push({ ...scenarios[0] });
      if (mutation === 'missing-eligibility') delete scenarios[0].typescriptProjection;
      bound('sdk-docs-scenarios.v1.json', { scenarios });
      bound('sdk-docs-scenario-executions.v1.json', [{ id: 'registered', sourceSha256: hash('source'), stdoutSha256: hash('output'), status: 'passed' }]);
      writeFileSync(join(bundle, 'demo.ts'), mutation === 'snippet-bytes' ? 'tampered' : snippet);
      artifacts['demo.ts'] = hash(snippet);
      if (mutation === 'sidecar-bytes') writeFileSync(join(bundle, 'sdk-docs-scenario-projections.v1.json'), 'not JSON');
      const manifest = JSON.stringify({ sourceState: 'captured-snapshot', revision, artifacts });
      writeFileSync(join(bundle, 'generation-manifest.v1.json'), manifest);
      writeFileSync(join(bundle, 'generation-manifest.v1.sha256'), hash(manifest));
      packageReceipt(packages, revision, []);
      if (mutation === 'compiler-receipt') writeFileSync(join(packages, 'BUILD.json'), 'tampered');
      if (mutation === 'missing-compiler-provenance') {
        const receipt = JSON.parse(readFileSync(join(packages, 'QUALIFICATION.json'), 'utf8'));
        delete receipt.compiler_build_receipt_sha256;
        writeFileSync(join(packages, 'QUALIFICATION.json'), JSON.stringify(receipt));
      }
      if (mutation === 'compiler-source') {
        const build = JSON.parse(readFileSync(join(packages, 'BUILD.json'), 'utf8'));
        build.source_commit = 'b'.repeat(40);
        const bytes = JSON.stringify(build);
        writeFileSync(join(packages, 'BUILD.json'), bytes);
        const receipt = JSON.parse(readFileSync(join(packages, 'QUALIFICATION.json'), 'utf8'));
        receipt.compiler_build_receipt_sha256 = hash(bytes);
        writeFileSync(join(packages, 'QUALIFICATION.json'), JSON.stringify(receipt));
      }
      const result = spawnSync(process.execPath, [fileURLToPath(new URL('./qualify-typescript-snippets.mjs', import.meta.url)), bundle, packages, work, join(root, 'must-not-install.mjs')], { encoding: 'utf8' });
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, mutation === 'compiler-receipt' ? /package compiler receipt digest differs/ : mutation === 'compiler-source' ? /package compiler source identity differs/ : mutation === 'missing-compiler-provenance' ? /require canonical compiler provenance/ : mutation === 'duplicate-projection' ? /duplicate or absent registered TypeScript projections/ : mutation === 'sidecar-bytes' ? /sidecar digest differs/ : mutation === 'snippet-bytes' ? /snippet digest differs/ : ['missing-eligible', 'ineligible-projection'].includes(mutation) ? /exactly cover Rust-owned TypeScript eligibility/ : ['missing-eligibility', 'duplicate-eligible', 'duplicate-projection'].includes(mutation) ? /invalid Rust-owned projection eligibility/ : /registered Rust scenario execution/);
      assert.equal(existsSync(work), false, 'rejection must precede installation and consumer creation');
    } finally { rmSync(root, { recursive: true, force: true }); }
  });
}

test('coherently wrong npm receipt version rejects using its own catalog among retained versions', () => {
  const root = mkdtempSync(join(tmpdir(), 'docs-snippet-version-'));
  try {
    const bundle = join(root, 'bundle'), packages = join(root, 'packages'), work = join(root, 'consumer');
    mkdirSync(bundle); mkdirSync(packages);
    const revision = 'a'.repeat(40), sourceSha256 = hash('closure'), snippet = 'console.log("fixture");\n', artifacts = {};
    const bound = (name, data) => {
      const bytes = JSON.stringify(data); writeFileSync(join(bundle, name), bytes); artifacts[name] = hash(bytes);
    };
    const scenario = { id: 'registered', package: 'acyclic-demo', family: 'demo', sourceSha256: hash('source'), typescriptProjection: true };
    bound('sdk-docs-scenarios.v1.json', { scenarios: [scenario] });
    bound('sdk-docs-scenario-executions.v1.json', [{ id: scenario.id, sourceSha256: scenario.sourceSha256, stdoutSha256: hash('output'), status: 'passed' }]);
    bound('sdk-docs-scenario-projections.v1.json', { projections: [{ scenarioId: scenario.id, package: '@acyclic-labs/demo', language: 'typescript', path: 'demo.ts', sourceSha256: scenario.sourceSha256, rustOutputSha256: hash('output'), snippetSha256: hash(snippet) }] });
    writeFileSync(join(bundle, 'demo.ts'), snippet); artifacts['demo.ts'] = hash(snippet);
    mkdirSync(join(bundle, 'preview'), { recursive: true });
    bound('preview/sdk-docs-data.v1.json', { version: 'preview', source: { revision }, packages: { entries: [{ packageName: scenario.package, familySlug: scenario.package, version: '1.0.0' }] } });
    mkdirSync(join(bundle, 'releases/older'), { recursive: true });
    bound('releases/older/sdk-docs-data.v1.json', { version: 'older', source: { revision: 'b'.repeat(40) }, packages: { entries: [{ packageName: scenario.package, familySlug: scenario.package, version: '2.0.0' }] } });
    bound('preview/release.json', { version: 'preview', revision, sourceSha256, scenarios: [{ id: scenario.id, package: scenario.package, cargoVersion: '1.0.0' }] });
    const manifest = JSON.stringify({ version: 'preview', sourceState: 'captured-snapshot', revision, sourceSha256, artifacts, releaseManifest: 'preview/release.json' });
    writeFileSync(join(bundle, 'generation-manifest.v1.json'), manifest);
    writeFileSync(join(bundle, 'generation-manifest.v1.sha256'), hash(manifest));
    packageReceipt(packages, revision, [{ name: '@acyclic-labs/demo', version: '2.0.0', asset: 'acyclic-labs-demo-2.0.0.tgz', sha256: hash('coherently different package').slice(7), size: 28 }]);
    const result = spawnSync(process.execPath, [fileURLToPath(new URL('./qualify-typescript-snippets.mjs', import.meta.url)), bundle, packages, work, join(root, 'must-not-install.mjs')], { encoding: 'utf8' });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /package version differs from Rust catalog/);
    assert.equal(existsSync(work), false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});


test('matching Rust package catalog admits its display family through the install boundary', () => {
  const root = mkdtempSync(join(tmpdir(), 'docs-snippet-owner-'));
  try {
    const bundle = join(root, 'bundle'), packages = join(root, 'packages'), work = join(root, 'consumer');
    mkdirSync(bundle); mkdirSync(packages);
    const revision = 'a'.repeat(40), sourceSha256 = hash('closure'), snippet = 'console.log("fixture");\n', artifacts = {};
    const bound = (name, data) => {
      const bytes = JSON.stringify(data); writeFileSync(join(bundle, name), bytes); artifacts[name] = hash(bytes);
    };
    const scenario = { id: 'actors/typescript-consumer', package: 'acyclic-actors', family: 'actors', sourceSha256: hash('source'), typescriptProjection: true };
    bound('sdk-docs-scenarios.v1.json', { scenarios: [scenario] });
    bound('sdk-docs-scenario-executions.v1.json', [{ id: scenario.id, sourceSha256: scenario.sourceSha256, stdoutSha256: hash('output'), status: 'passed' }]);
    bound('sdk-docs-scenario-projections.v1.json', { projections: [{ scenarioId: scenario.id, package: '@acyclic-labs/actors', language: 'typescript', path: 'actors.ts', sourceSha256: scenario.sourceSha256, rustOutputSha256: hash('output'), snippetSha256: hash(snippet) }] });
    writeFileSync(join(bundle, 'actors.ts'), snippet); artifacts['actors.ts'] = hash(snippet);
    mkdirSync(join(bundle, 'releases/current'), { recursive: true });
    bound('releases/current/sdk-docs-data.v1.json', { version: '0.2.0', source: { revision }, packages: { entries: [{ packageName: scenario.package, familySlug: scenario.package, version: '0.2.0' }] } });
    bound('releases/current/release.json', { version: '0.2.0', revision, sourceSha256, scenarios: [{ id: scenario.id, package: scenario.package, cargoVersion: '0.2.0' }] });
    const manifest = JSON.stringify({ version: '0.2.0', sourceState: 'captured-snapshot', revision, sourceSha256, artifacts, releaseManifest: 'releases/current/release.json' });
    writeFileSync(join(bundle, 'generation-manifest.v1.json'), manifest);
    writeFileSync(join(bundle, 'generation-manifest.v1.sha256'), hash(manifest));
    const archive = 'acyclic-labs-actors-0.2.0.tgz', bytes = Buffer.from('qualified package fixture');
    writeFileSync(join(packages, archive), bytes);
    packageReceipt(packages, revision, [{ name: '@acyclic-labs/actors', version: '0.2.0', asset: archive, sha256: hash(bytes).slice(7), size: bytes.length }]);
    const installer = join(root, 'install-boundary.mjs');
    writeFileSync(installer, 'console.error("qualified owner reached install"); process.exit(23);');
    const result = spawnSync(process.execPath, [fileURLToPath(new URL('./qualify-typescript-snippets.mjs', import.meta.url)), bundle, packages, work, installer], { encoding: 'utf8' });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /install failed: qualified owner reached install/);
    assert.equal(existsSync(join(work, 'install.stderr.log')), true);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
