import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

test('PR dispatch owns the reviewed payload even when the installed runner reports stale success', () => {
  const cwd = mkdtempSync(join(tmpdir(), 'pr-payload-custody-'));
  try {
    const work = join(cwd, 'work');
    mkdirSync(join(work, 'scripts'), { recursive: true });
    mkdirSync(join(cwd, 'Q:/lanes'), { recursive: true });
    writeFileSync(join(cwd, 'Q:/lanes/gate-test-runner.sh'), '# reviewed artifact dispatch\n');
    writeFileSync(join(cwd, 'Q:/lanes/pr-gate-checks.sh'), '#!/bin/bash\necho "GATE_DONE rc=0"\n');
    const reviewed = '#!/bin/bash\nset -e\nbash scripts/gate-windows-features.sh affected\n';
    writeFileSync(join(work, 'scripts/pr-gate-checks.sh'), reviewed);
    writeFileSync(join(work, 'scripts/gate-windows-features.sh'),
      '#!/bin/bash\necho "GATE_WINDOWS_FEATURES_START scope=$1"\necho "error[E0425]: missing Windows feature dependency"\nexit 101\n');
    const source = readFileSync(join(process.env.SDK_BUILD_TOOLS_DIR || '/home/var/lib/sdk-build-tools', 'coordinator/pr-gate.sh'), 'utf8');
    const start = source.indexOf('# The payload belongs');
    const end = source.indexOf('if [ "$(tr -d', start);
    assert.ok(start > 0 && end > start);
    const assembly = source.slice(start, end);
    for (const negative of [false, true]) {
      // Restore the actual old selection path: the independently installed
      // runner passes without executing the candidate's required MSVC phase.
      const selected = negative ? assembly.replace(
        'cp "$W/scripts/pr-gate-checks.sh"', 'cp Q:/lanes/pr-gate-checks.sh') : assembly;
      assert.equal(selected === assembly, !negative);
      const result = spawnSync('bash', ['-c', `${selected}\ncd "$W" && bash .gate-checks.sh`], {
        cwd, encoding: 'utf8', env: { ...process.env, W: work, log: join(cwd, 'gate.log') },
      });
      assert.equal(result.status, negative ? 0 : 101, result.stdout + result.stderr);
      if (negative) assert.doesNotMatch(result.stdout, /GATE_WINDOWS_FEATURES_START/);
      else assert.match(result.stdout, /error\[E0425\]: missing Windows feature dependency/);
    }
    rmSync(join(work, 'scripts/pr-gate-checks.sh'));
    const missing = spawnSync('bash', ['-c', `${assembly}\necho dispatched`], {
      cwd, encoding: 'utf8', env: { ...process.env, W: work, log: join(cwd, 'gate.log') },
    });
    assert.equal(missing.status, 6, missing.stdout + missing.stderr);
    assert.doesNotMatch(missing.stdout, /dispatched/);
    assert.match(readFileSync(join(cwd, 'gate.log'), 'utf8'), /reviewed PR payload is unavailable/);
  } finally {
    rmSync(cwd, { recursive: true, force: true });
  }
});
