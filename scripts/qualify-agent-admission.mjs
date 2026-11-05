import {spawnSync} from 'node:child_process';
import {readFileSync, writeFileSync, mkdirSync} from 'node:fs';
import path from 'node:path';

const features = 'filesystem-local,native-execution,test-support';
const membership = 'agent::children::tests::persistent_pages_and_point_updates_match_membership_after_reopen';
const ancestry = 'agent::layout::tests::canonical_subtree_admission_borrows_ancestry_without_parent_copies';
const writer = 'agent::codec_fuzz::tests::writer_corpus_satisfies_the_shared_agent_oracle';
const membershipOnly = process.argv[2] === '--membership';
const borrowedOnly = process.argv[2] === '--borrowed';
const parentOnly = process.argv[2] === '--parent';

function command(executable, args, expected, capture = false) {
  const result = spawnSync(executable, args, {encoding: 'utf8', maxBuffer: 32 * 1024 * 1024});
  const output = (result.stdout ?? '') + (result.stderr ?? '');
  const recovery = `${executable} ${args.map(arg => JSON.stringify(arg)).join(' ')}`;
  const refused = expected;
  if (result.error || result.signal || (expected
      ? result.status !== 100 || !refused.every(assertion => output.includes(assertion))
        || !output.includes(`${refused.length} ${refused.length === 1 ? 'test' : 'tests'} run: 0 passed, ${refused.length} failed`)
        || !output.includes('panicked at')
      : result.status !== 0)) {
    process.stderr.write(output);
    throw new Error(`step=qualify agent admission; recovery=${recovery}; ${result.error ?? result.signal ?? result.status}`);
  }
  if (expected) process.stdout.write(`Rejected production mutation: ${refused.join('; ')}\n`);
  else if (!capture) process.stdout.write(output);
  return capture ? result.stdout : output;
}

function mutations(cases, binary = ['--lib']) {
  const originals = new Map(cases.map(([, source]) => [source, readFileSync(source)]));
  try {
    for (const [name] of cases) {
      command('git', ['apply', `rust/codec-fuzz/mutations/${name}.patch`]);
    }
    command('cargo', ['nextest', 'run', '--locked', '--no-fail-fast', '-p', 'acyclic-harness-core', '--features', features,
      ...binary, '-E', cases.map(([, , test]) => `test(=${test})`).join(' | ')],
      cases.map(([, , , assertion]) => assertion));
  } finally {
    for (const [source, original] of originals) writeFileSync(source, original);
  }
}

try {
  if (!borrowedOnly && !parentOnly) {
    mutations([['agent-membership-allocation', 'rust/crates/harness-core/src/agent/children.rs', membership,
      'cached membership lookup allocated identity or branch storage']]);
  }
  if (!membershipOnly) {
    mutations([
      ...(!parentOnly ? [['agent-membership-insert-copy', 'rust/crates/harness-core/src/agent/children.rs', membership,
        'cached duplicate insertion copied its borrowed address']] : []),
      ['agent-parent-copy', 'rust/crates/harness-core/src/agent/layout.rs', ancestry,
        'immediate parent admission copied its borrowed address'],
    ]);
  }
  if (!membershipOnly && !borrowedOnly && !parentOnly) {
    mutations([['agent-membership-scan', 'rust/crates/harness-core/src/agent/children.rs', membership,
      'membership operation scanned beyond its fixed node credit']]);
    mutations([['agent-record-version', 'rust/crates/harness-core/src/agent/records.rs', writer,
      'agent record header refusal changed']]);
    mutations([['agent-spawn-reply-read', 'rust/crates/harness-core/src/agent/host.rs',
      'agent_model::runtime::a_spawn_retry_after_later_steps_names_the_same_child',
      'request budget exceeded for control']], ['--test', 'main']);
  }
  const tests = parentOnly ? [ancestry, writer] : membershipOnly ? [membership] : borrowedOnly ? [membership,
    ancestry,
    'agent_model::awaited_children_are_scoped_to_their_parent'] : [membership, writer,
    'agent_model::a_client_spawn_commits_before_the_next_turn_reaches_or_counts_it',
    'agent_model::slot_schedules::admission_schedules_preserve_every_spawn_and_replay',
    'agent_model::slot_schedules::an_invalid_host_wait_fails_before_publishing_status_or_effects',
    'agent_model::slot_schedules::a_child_finishing_during_pause_cannot_strand_a_recorded_replay',
    'agent_model::runtime::native_reopen_preserves_paused_spawns_until_the_matching_approval',
    'agent_model::runtime::a_spawn_reply_is_the_outcome_the_parent_step_committed',
    'agent_model::runtime::a_spawn_retry_after_later_steps_names_the_same_child',
    'agent_model::runtime::a_root_crosses_the_former_child_ceiling_with_a_paged_checkpoint'];
  const positive = command('cargo', ['nextest', 'run', '--locked', '-p', 'acyclic-harness-core', '--features', features,
    '--lib', ...(membershipOnly || parentOnly ? [] : ['--test', 'main']),
    '-E', tests.map(test => `test(=${test})`).join(' | ')]);
  if (!positive.includes(`${tests.length} ${tests.length === 1 ? 'test' : 'tests'} run: ${tests.length} passed`)) {
    throw new Error('selected test inventory differs; recovery=cargo nextest list --locked -p acyclic-harness-core --lib --test main');
  }
  if (parentOnly) {
    command('cargo', ['check', '--locked', '-p', 'acyclic-harness', '-p', 'acyclic-harness-client', '--lib',
      '--features', features.split(',').map(feature => `acyclic-harness/${feature}`).join(',')]);
  }
  if (!membershipOnly && !borrowedOnly && !parentOnly) {
    command('cargo', ['check', '--locked', '-p', 'acyclic-harness', '--lib', '--features', features]);
    const output = command('cargo', ['run', '--quiet', '--locked', '-p', 'acyclic-harness-core',
      '--features', features, '--example', 'agent-record-corpus', '--', '--codecs'], undefined, true);
    const destination = process.argv[2] ?? 'target/agent-codec-corpus.json';
    JSON.parse(output);
    mkdirSync(path.dirname(destination), {recursive: true});
    writeFileSync(destination, output);
    process.stdout.write(`Captured production agent writer output: ${destination}\n`);
  }
} catch (error) {
  process.stderr.write(`step=qualify agent admission; recovery=node scripts/qualify-agent-admission.mjs${membershipOnly ? ' --membership' : borrowedOnly ? ' --borrowed' : parentOnly ? ' --parent' : ''}; ${error.message}\n`);
  process.exitCode = 1;
}
