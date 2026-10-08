import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const hash = bytes => `sha256:${createHash('sha256').update(bytes).digest('hex')}`;
for (const mutation of ['sidecar-bytes', 'unregistered-id', 'execution-output', 'snippet-bytes']) {
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
      bound('sdk-docs-scenario-projections.v1.json', { projections: [projection] });
      bound('sdk-docs-scenarios.v1.json', { scenarios: [{ id: 'registered', family: 'demo', sourceSha256: hash('source') }] });
      bound('sdk-docs-scenario-executions.v1.json', [{ id: 'registered', sourceSha256: hash('source'), stdoutSha256: hash('output'), status: 'passed' }]);
      writeFileSync(join(bundle, 'demo.ts'), mutation === 'snippet-bytes' ? 'tampered' : snippet);
      artifacts['demo.ts'] = hash(snippet);
      if (mutation === 'sidecar-bytes') writeFileSync(join(bundle, 'sdk-docs-scenario-projections.v1.json'), 'not JSON');
      const manifest = JSON.stringify({ sourceState: 'captured-snapshot', revision, artifacts });
      writeFileSync(join(bundle, 'generation-manifest.v1.json'), manifest);
      writeFileSync(join(bundle, 'generation-manifest.v1.sha256'), hash(manifest));
      writeFileSync(join(packages, 'QUALIFICATION.json'), JSON.stringify({ revision: 1, source_commit: revision, packages: [] }));
      const result = spawnSync(process.execPath, [fileURLToPath(new URL('./qualify-typescript-snippets.mjs', import.meta.url)), bundle, packages, work, join(root, 'must-not-install.mjs')], { encoding: 'utf8' });
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, mutation === 'sidecar-bytes' ? /sidecar digest differs/ : mutation === 'snippet-bytes' ? /snippet digest differs/ : /registered Rust scenario execution/);
      assert.equal(existsSync(work), false, 'rejection must precede installation and consumer creation');
    } finally { rmSync(root, { recursive: true, force: true }); }
  });
}
