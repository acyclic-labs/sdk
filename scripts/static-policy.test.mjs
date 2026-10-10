import assert from 'node:assert/strict';
import test from 'node:test';
import {pinnedActions,rustFormatNeeded} from './static-policy.mjs';

test('every supported action reference is pinned and alternative YAML forms fail closed',()=>{
  pinnedActions('workflow.yml',`  - uses: actions/checkout@${'a'.repeat(40)}\n  - uses: ./.github/actions/local\n# uses: example@main`);
  pinnedActions('workflow.yml',`  - run: |\n      echo 'uses: example@main'\n  - uses: ./.github/actions/local`);
  pinnedActions('workflow.yml',`name: 'Build uses: pinned actions'\nname: "Build, uses: pinned actions"\nname: Build uses: pinned actions`);
  pinnedActions('workflow.yml','if: ${{ !cancelled() }}');
  pinnedActions('workflow.yml','if: ${{\n  !cancelled()\n}}');
  pinnedActions('workflow.yml',"if: ${{ !cancelled() &&\n  needs.build.result == 'success' }}");
  for (const source of ['- {uses: actions/checkout@main}', '- "uses": actions/checkout@main',
    '- uses: actions/checkout@main', '- uses: *action', '- {"uses": actions/checkout@main}',
    '- uses: |-\n    actions/checkout@main', '- uses: >\n    actions/checkout@main',
    '- &checkout uses: actions/checkout@main', '- !!map uses: actions/checkout@main',
    'steps: *steps', '- "u\\u0073es": actions/checkout@main']) {
    assert.throws(()=>pinnedActions('workflow.yml',source));
  }
});
test('formatter settings and nested Rust changes select formatting',()=>{
  for (const file of ['rustfmt.toml','.rustfmt.toml','rust/crates/harness/rustfmt.toml',
    'rust-toolchain.toml','rust/crates/harness/src/lib.rs']) assert.equal(rustFormatNeeded([file]),true,file);
  assert.equal(rustFormatNeeded(['README.md']),false);
});
