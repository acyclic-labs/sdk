import assert from 'node:assert/strict';
import test from 'node:test';
import {pinnedActions,rustFormatNeeded} from './static-policy.mjs';
const workflow = steps => 'jobs:\n  build:\n    steps:\n' + steps.split('\n').map(line => '      ' + line).join('\n');
test('pins actual actions while allowing script text, comments and YAML syntax',()=>{
  const sha='a'.repeat(40);
  for(const source of [
    '- uses: actions/checkout@'+sha,
    '- {uses: actions/checkout@'+sha+'}',
    '- "uses": actions/checkout@'+sha,
    '- &checkout uses: actions/checkout@'+sha+'\n- *checkout',
    '- uses: |-\n    actions/checkout@'+sha,
    '- uses: ./.github/actions/local',
    '- name: "Build uses: pinned actions"\n  run: echo hello, !world',
    '- name: Build # example: &foo\n  run: | # '+ '$' + '{{\n    echo uses: example@main'
  ]) pinnedActions('workflow.yml',workflow(source));
  pinnedActions('action.yml','runs:\n  steps:\n    - uses: actions/checkout@'+sha);
  pinnedActions('workflow.yml','jobs:\n  build:\n    uses: ./.github/workflows/local.yml');
});
test('unpinned action references cannot hide in aliases, scalar values or comments',()=>{
  for(const source of [
    '- uses: actions/checkout@main', '- {uses: actions/checkout@main}',
    '- "uses": actions/checkout@main', '- "u\\u0073es": actions/checkout@main',
    '- &checkout uses: actions/checkout@main',
    '- uses: |-\n    actions/checkout@main',
    '- run: | # '+ '$' + '{{\n    echo hello\n- uses: actions/checkout@main'
  ]) assert.throws(()=>pinnedActions('workflow.yml',workflow(source)),/must be pinned/);
  assert.throws(()=>pinnedActions('workflow.yml','jobs:\n  build:\n    uses: owner/repo/.github/workflows/test.yml@main'),/must be pinned/);
  assert.throws(()=>pinnedActions('workflow.yml','jobs: {build: 1, build: 2}'));
});
test('formatter settings and nested Rust changes select formatting',()=>{
  for(const file of ['rustfmt.toml','.rustfmt.toml','rust/crates/harness/rustfmt.toml','rust-toolchain.toml','rust/crates/harness/src/lib.rs']) assert.equal(rustFormatNeeded([file]),true,file);
  assert.equal(rustFormatNeeded(['README.md']),false);
});
