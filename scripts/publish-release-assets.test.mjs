import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { admitAsset, admitRelease, publish } from './publish-release-assets.mjs';

test('retries accept only the original source and version', () => {
  const source = 'a'.repeat(40);
  const release = { tag_name: 'acyclic-v1.2.3', target_commitish: source };
  admitRelease(release, source, '1.2.3');
  assert.throws(() => admitRelease(release, 'b'.repeat(40), '1.2.3'));
  assert.throws(() => admitRelease(release, source, '1.2.4'));
});

test('matching published assets resume; differing bytes are rejected', () => {
  admitAsset('plugin.tgz', 'sha256:abc', 'sha256:abc');
  assert.throws(() => admitAsset('plugin.tgz', 'sha256:abc', 'sha256:def'));
});

test('first publication creates a draft at the qualified source and publishes only its downloaded assets', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-first-release-'));
  const file = path.join(directory, 'qualified-plugin.tgz');
  fs.writeFileSync(file, 'qualified bytes');
  t.after(() => { fs.unlinkSync(file); fs.rmdirSync(directory); });
  const env = {SOURCE_SHA:'a'.repeat(40), VERSION:'1.2.3', GITHUB_REPOSITORY:'owner/sdk', RELEASE_ASSET_DIR:directory};
  let release;
  const calls = [];
  const notFound = () => { throw Object.assign(new Error('missing'), {stderr:'HTTP 404'}); };
  const gh = (...args) => {
    calls.push(args);
    if (args[0] === 'api') {
      if (args[1].includes('/git/ref/')) return notFound();
      if (!release) return notFound();
      return JSON.stringify(release);
    }
    if (args[1] === 'create') {
      assert.equal(args[2], 'acyclic-v1.2.3');
      assert.equal(args[args.indexOf('--target') + 1], env.SOURCE_SHA);
      assert.ok(args.includes('--draft'));
      release = {tag_name:args[2], target_commitish:env.SOURCE_SHA, draft:true, assets:[]};
    } else if (args[1] === 'upload') {
      assert.equal(release.draft, true);
      assert.equal(args[3], file);
      release.assets.push({name:path.basename(file)});
    } else if (args[1] === 'edit') {
      assert.deepEqual(release.assets.map(asset=>asset.name), ['qualified-plugin.tgz']);
      assert.ok(args.includes('--draft=false'));
      release.draft = false;
    } else throw new Error('unexpected mutation');
    return '';
  };
  await publish({env, gh});
  assert.equal(release.draft, false);
  assert.equal(calls.filter(args=>args[1]==='create').length, 1);
  assert.equal(calls.filter(args=>args[1]==='upload').length, 1);
  await assert.rejects(publish({env:{...env, RELEASE_ASSET_DIR:undefined}, gh}), /explicit qualified/);
});

test('an interrupted publication resumes its exact remaining assets', async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-release-test-'));
  try {
    fs.writeFileSync(path.join(directory, 'a.tgz'), 'original a');
    fs.writeFileSync(path.join(directory, 'b.tgz'), 'original b');
    const env = { SOURCE_SHA: 'a'.repeat(40), VERSION: '1.2.3', GITHUB_REPOSITORY: 'owner/sdk' };
    /** @type {{tag_name: string, target_commitish: string, draft: boolean, assets: {name: string, digest: string}[]}} */
    const release = { tag_name: 'acyclic-v1.2.3', target_commitish: env.SOURCE_SHA, draft: true, assets: [] };
    const uploaded = [];
    let interrupted = false;
    const gh = (...args) => {
      if (args[0] === 'api') {
        if (args[1].includes('/git/ref/')) return JSON.stringify({ object: { type: 'commit', sha: env.SOURCE_SHA } });
        return JSON.stringify(release);
      }
      if (args[1] === 'upload') {
        const file = args[3], name = path.basename(file);
        if (name === 'b.tgz' && !interrupted) { interrupted = true; throw new Error('simulated interruption'); }
        uploaded.push(name);
        release.assets.push({ name, digest: 'sha256:' + createHash('sha256').update(fs.readFileSync(file)).digest('hex') });
      } else if (args[1] === 'edit') release.draft = false;
      else throw new Error('unexpected mutation ' + args.join(' '));
      return '';
    };
    await assert.rejects(publish({ env, directory, gh }), /simulated interruption/);
    assert.equal(release.draft, true);
    await publish({ env, directory, gh });
    assert.deepEqual(uploaded, ['a.tgz', 'b.tgz']);
    assert.equal(release.draft, false);
    fs.writeFileSync(path.join(directory, 'a.tgz'), 'conflicting a');
    await assert.rejects(publish({ env, directory, gh }), /different contents/);
    assert.deepEqual(uploaded, ['a.tgz', 'b.tgz']);
  } finally {
    const relative = path.relative(path.resolve(os.tmpdir()), path.resolve(directory));
    assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative));
    fs.rmSync(directory, { recursive: true });
  }
});
