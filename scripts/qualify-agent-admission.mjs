import {spawnSync} from 'node:child_process';
import {readFileSync, writeFileSync, mkdirSync} from 'node:fs';
import path from 'node:path';

const features = 'filesystem-local,native-execution,test-support';
const membership = 'agent::children::tests::persistent_pages_and_point_updates_match_membership_after_reopen';
const writer = 'agent::codec_fuzz::tests::writer_corpus_satisfies_the_shared_agent_oracle';

function command(executable, args, expected, capture = false) {
  const result = spawnSync(executable, args, {encoding: 'utf8', maxBuffer: 32 * 1024 * 1024});
  const output = (result.stdout ?? '') + (result.stderr ?? '');
  const recovery = `${executable} ${args.map(arg => JSON.stringify(arg)).join(' ')}`;
  if (result.error || result.signal || (expected
      ? result.status !== 101 || !output.includes(expected)
        || !output.includes('test result: FAILED') || !output.includes('panicked at')
      : result.status !== 0)) {
    process.stderr.write(output);
    throw new Error(`step=qualify agent admission; recovery=${recovery}; ${result.error ?? result.signal ?? result.status}`);
  }
  if (expected) process.stdout.write(`Rejected production mutation: ${expected}\n`);
  else if (!capture) process.stdout.write(output);
  return capture ? result.stdout : output;
}

function mutation(name, source, test, assertion) {
  const original = readFileSync(source);
  try {
    command('git', ['apply', `rust/codec-fuzz/mutations/${name}.patch`]);
    command('cargo', ['test', '--locked', '-p', 'acyclic-harness-core', '--features', features,
      '--lib', test, '--', '--exact'], assertion);
  } finally {
    writeFileSync(source, original);
  }
}

try {
  mutation('agent-membership-scan', 'rust/crates/harness-core/src/agent/children.rs', membership,
    'membership operation scanned beyond its fixed node credit');
  mutation('agent-record-version', 'rust/crates/harness-core/src/agent/records.rs', writer,
    'agent record header refusal changed');
  const tests = [membership, writer,
    'agent_model::a_client_spawn_commits_before_the_next_turn_reaches_or_counts_it',
    'agent_model::slot_schedules::admission_schedules_preserve_every_spawn_and_replay',
    'agent_model::slot_schedules::an_invalid_host_wait_fails_before_publishing_status_or_effects',
    'agent_model::slot_schedules::a_child_finishing_during_pause_cannot_strand_a_recorded_replay',
    'agent_model::runtime::native_reopen_preserves_paused_spawns_until_the_matching_approval',
    'agent_model::runtime::a_root_crosses_the_former_child_ceiling_with_a_paged_checkpoint'];
  const positive = command('cargo', ['nextest', 'run', '--locked', '-p', 'acyclic-harness-core', '--features', features,
    '--lib', '--test', 'main', '-E', tests.map(test => `test(=${test})`).join(' | ')]);
  if (!positive.includes(`${tests.length} tests run: ${tests.length} passed`)) {
    throw new Error('selected test inventory differs; recovery=cargo nextest list --locked -p acyclic-harness-core --lib --test main');
  }
  command('cargo', ['check', '--locked', '-p', 'acyclic-harness', '--lib', '--features', features]);
  const output = command('cargo', ['run', '--quiet', '--locked', '-p', 'acyclic-harness-core',
    '--features', features, '--example', 'agent-record-corpus', '--', '--codecs'], undefined, true);
  const destination = process.argv[2] ?? 'target/agent-codec-corpus.json';
  JSON.parse(output);
  mkdirSync(path.dirname(destination), {recursive: true});
  writeFileSync(destination, output);
  process.stdout.write(`Captured production agent writer output: ${destination}\n`);
} catch (error) {
  process.stderr.write(`step=qualify agent admission; recovery=node scripts/qualify-agent-admission.mjs; ${error.message}\n`);
  process.exitCode = 1;
}
