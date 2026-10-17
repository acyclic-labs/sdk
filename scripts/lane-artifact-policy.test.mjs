import assert from 'node:assert/strict';
import test from 'node:test';
import {laneArtifactViolation} from './lane-artifact-policy.mjs';

test('operational names cannot hide under product documentation', () => {
  for (const root of ['', 'docs/', 'rust/crates/harness/docs/']) {
    for (const name of ['rev-839-verdict2.md', 'verdict.md', 'lane-log.txt', 'build-receipt.json', 'scratch-notes.md', 'build.log', 'scratch/README.md']) {
      assert.ok(laneArtifactViolation(root + name), root + name);
    }
  }
  for (const path of ['notes.md', 'plans/fix.markdown', 'lanes/README.md', 'review.mdown',
    'docs/approvals-map.md', 'docs/scratch-plan.md', 'docs/new-product-guide.md', 'docs/README.md']) {
    assert.ok(laneArtifactViolation(path), path);
  }
});

test('product documentation, production receipts and parser fixture bytes remain supported', () => {
  for (const path of [
    'README.md', 'docs/ci-releases.md', 'rust/crates/harness/docs/client/QUALIFICATION.md',
    'typescript/packages/harness/docs/client-views.md', '.github/pull_request_template.md',
    'rust/crates/conformance/schemas/qualification-receipt.schema.json',
    'rust/crates/harness/src/interaction/receipt.rs', 'rust/crates/sdk-docs/src/tests/scratch.rs',
    'rust/crates/markdown/tests/fixtures/scratch-notes.md',
    'rust/crates/markdown/fuzz/corpus/parser/rev-123-verdict.md',
    'rust/fuzz/native-callback-path/corpus/callback_path/build.log',
  ]) assert.equal(laneArtifactViolation(path), undefined, path);
});
