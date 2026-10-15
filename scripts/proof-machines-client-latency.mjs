import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';

// Run only inside the single proof-labelled remote job before opening the PR.
const source = 'rust/crates/machines/src/grpc/recovery.rs';
const saved = readFileSync(source);
const candidate = saved.toString('utf8').replaceAll('\r\n', '\n');
const baseline = candidate
  .replace('            let mut immediate_reconnect = true;\n', '')
  .replace(/                        \/\/ One healthy inspection can repair an interrupted watch without\n                        \/\/ delaying completion\. Repeated interruptions still back off\.\n                        if immediate_reconnect \{\n                            immediate_reconnect = false;\n                            continue;\n                        \}\n/, '');
const waitSource = text => text.slice(0, text.indexOf('pub(super) async fn decode_recovered('));
// Original wait source at 25218c52c; shallow remote checkouts need no history fetch.
assert.equal(createHash('sha256').update(waitSource(baseline)).digest('hex'),
  'd5379b840765cd379dd3468e5adfab33d21356532e0ef1520d67a36099ce3d28',
  'baseline wait must match the original reconnect policy');
assert.notEqual(baseline, candidate, 'the pair must differ in the reconnect policy');

const results = [];
try {
  for (const [revision, contents] of [['before', baseline], ['after', candidate]]) {
    writeFileSync(source, contents);
    const child = spawnSync('/home/var/bin/build-slot', [
      'cargo', 'test', '--locked', '-p', 'acyclic-machines',
      'create_exec_round_trip_measurement', '--', '--ignored', '--nocapture',
    ], { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 });
    process.stdout.write(child.stdout ?? '');
    process.stderr.write(child.stderr ?? '');
    assert.equal(child.status, 0, `${revision} failed: ${child.error ?? child.signal ?? child.status}`);
    const samples = `${child.stdout}\n${child.stderr}`.split('\n')
      .filter(line => line.startsWith('{"case":')).map(line => JSON.parse(line));
    assert.deepEqual(samples.map(sample => sample.case).sort(), ['healthy', 'interrupted_watch']);
    for (const sample of samples) {
      assert.equal(sample.samples, 7);
      assert.equal(sample.create_us.length, 7);
      assert.equal(sample.exec_us.length, 7);
    }
    results.push({ revision, samples });
  }
} finally {
  writeFileSync(source, saved);
}
console.log(JSON.stringify({ transport: 'loopback TCP gRPC with bearer authentication; connection setup excluded', results }, null, 2));
