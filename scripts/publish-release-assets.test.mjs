import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { admitAsset, admitRelease, assets, publish } from './publish-release-assets.mjs';
import { commandOutput } from './process-capture.mjs';

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
        if (uploaded.length === 1 && !interrupted) { interrupted = true; throw new Error('simulated interruption'); }
        uploaded.push(name);
        release.assets.push({ name, digest: 'sha256:' + createHash('sha256').update(fs.readFileSync(file)).digest('hex') });
      } else if (args[1] === 'edit') release.draft = false;
      else throw new Error('unexpected mutation ' + args.join(' '));
      return '';
    };
    await assert.rejects(publish({ env, directory, gh }), /simulated interruption/);
    assert.equal(release.draft, true);
    await publish({ env, directory, gh });
    assert.deepEqual([...uploaded].sort(), ['a.tgz', 'b.tgz']);
    assert.equal(release.draft, false);
    fs.writeFileSync(path.join(directory, 'a.tgz'), 'conflicting a');
    await assert.rejects(publish({ env, directory, gh }), /different contents/);
    assert.deepEqual([...uploaded].sort(), ['a.tgz', 'b.tgz']);
  } finally {
    const relative = path.relative(path.resolve(os.tmpdir()), path.resolve(directory));
    assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative));
    fs.rmSync(directory, { recursive: true });
  }
});


test('nested real Git tags resolve completely before release mutation', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-release-tags-'));
  t.after(() => {
    const relative = path.relative(path.resolve(os.tmpdir()), path.resolve(directory));
    assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative));
    fs.rmSync(directory, { recursive: true });
  });
  const repository = path.join(directory, 'repository');
  const qualified = path.join(directory, 'assets');
  fs.mkdirSync(repository);
  fs.mkdirSync(qualified);
  const git = (args, input) => commandOutput('git', args, { cwd: repository, input, encoding: 'utf8' }).trim();
  git(['init', '--quiet']);
  const tree = git(['hash-object', '-w', '-t', 'tree', '--stdin'], '');
  const source = git(['hash-object', '-w', '-t', 'commit', '--stdin'],
    `tree ${tree}\nauthor Release <release@example.test> 1 +0000\ncommitter Release <release@example.test> 1 +0000\n\nqualified source\n`);
  let head = source, kind = 'commit';
  for (let index = 0; index < 33; index++) {
    head = git(['mktag'], `object ${head}\ntype ${kind}\ntag nested-${index}\ntagger Release <release@example.test> 1 +0000\n\nnested tag\n`);
    kind = 'tag';
  }
  git(['update-ref', 'refs/tags/acyclic-v1.2.3', head]);
  const file = path.join(qualified, 'artifact.tgz');
  fs.writeFileSync(file, 'complete qualified asset');
  const asset = { name: path.basename(file), digest: 'sha256:' + createHash('sha256').update(fs.readFileSync(file)).digest('hex') };
  const env = { SOURCE_SHA: source, VERSION: '1.2.3', GITHUB_REPOSITORY: 'owner/sdk' };
  for (const mode of ['complete', 'wrong-source', 'cycle']) {
    let tagReads = 0, mutations = 0;
    const gh = (...args) => {
      if (args[0] === 'api' && args[1].includes('/git/ref/')) {
        return JSON.stringify({ object: { type: 'tag', sha: git(['rev-parse', 'refs/tags/acyclic-v1.2.3']) } });
      }
      if (args[0] === 'api' && args[1].includes('/git/tags/')) {
        tagReads++;
        const sha = args[1].split('/').at(-1);
        assert.equal(git(['cat-file', '-t', sha]), 'tag');
        const tag = git(['cat-file', '-p', sha]);
        const object = { sha: /^object ([0-9a-f]+)$/m.exec(tag)[1], type: /^type (.+)$/m.exec(tag)[1] };
        if (mode === 'cycle') object.sha = sha;
        return JSON.stringify({ object });
      }
      assert.equal(mode, 'complete', 'invalid tag reached release observation or mutation');
      assert.equal(tagReads, 33, 'release began before resolving the entire tag chain');
      if (args[0] === 'api') return JSON.stringify({ tag_name: 'acyclic-v1.2.3', target_commitish: source, assets: [asset] });
      assert.deepEqual(args, ['release', 'edit', 'acyclic-v1.2.3', '--repo', 'owner/sdk', '--draft=false']);
      mutations++;
      return '';
    };
    const publishing = publish({ env: mode === 'wrong-source' ? { ...env, SOURCE_SHA: tree } : env, directory: qualified, gh });
    if (mode === 'complete') {
      await publishing;
      assert.equal(tagReads, 33);
      assert.equal(mutations, 1);
    } else {
      await assert.rejects(publishing, mode === 'cycle' ? /cycle/ : /different source/);
      assert.equal(tagReads, mode === 'cycle' ? 1 : 33);
      assert.equal(mutations, 0);
    }
  }
});


test('asset traversal yields before scanning the tree and retires directories on interruption', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-release-walk-'));
  t.after(() => {
    const relative = path.relative(path.resolve(os.tmpdir()), path.resolve(directory));
    assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative));
    fs.rmSync(directory, { recursive: true });
  });
  const expected = [];
  for (let branch = 0; branch < 17; branch++) {
    const child = path.join(directory, `branch-${branch}`);
    fs.mkdirSync(child);
    for (let index = 0; index < 33; index++) {
      const file = path.join(child, `asset-${branch}-${index}.tgz`);
      fs.writeFileSync(file, `${branch}:${index}`);
      expected.push(file);
    }
  }
  let deep = directory;
  for (let depth = 0; depth < 129; depth++) {
    deep = path.join(deep, 'd');
    fs.mkdirSync(deep);
  }
  const leaf = path.join(deep, 'deep.tgz');
  fs.writeFileSync(leaf, 'deep asset');
  expected.push(leaf);
  const opendir = fs.promises.opendir;
  const readdir = fs.readdirSync;
  let opened = 0, closed = 0, entries = 0, failOpen = false;
  t.mock.method(fs, 'readdirSync', (...args) => {
    const found = readdir(...args);
    entries += found.length;
    return found;
  });
  t.mock.method(fs.promises, 'opendir', async (...args) => {
    if (failOpen && opened === 1) throw new Error('injected directory failure');
    const dir = await opendir(...args);
    opened++;
    const read = dir.read.bind(dir), close = dir.close.bind(dir);
    dir.read = async () => {
      const entry = await read();
      if (entry) entries++;
      return entry;
    };
    dir.close = async () => { await close(); closed++; };
    return dir;
  });
  const iterator = assets(directory);
  const started = performance.now();
  const rss = process.memoryUsage().rss;
  const first = await iterator.next();
  t.diagnostic(JSON.stringify({ files: expected.length, firstYieldEntries: entries,
    firstYieldMs: performance.now() - started, firstYieldRssDelta: process.memoryUsage().rss - rss }));
  assert.equal(first.done, false);
  assert.ok(expected.includes(first.value));
  assert.ok(entries < expected.length, 'first yield scanned the complete asset tree');
  await iterator.return();
  assert.equal(closed, opened, 'early retirement retained a directory');
  const complete = [];
  for await (const file of assets(directory)) complete.push(file);
  assert.deepEqual(complete.sort(), expected.sort());
  assert.equal(closed, opened, 'complete traversal retained a directory');
  opened = 0;
  closed = 0;
  failOpen = true;
  await assert.rejects(async () => {
    for await (const file of assets(directory)) void file;
  }, /injected directory failure/);
  assert.equal(closed, opened, 'failed traversal retained an ancestor directory');
});


test('empty or duplicate asset selections never publish a release', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-release-selection-'));
  t.after(() => {
    const relative = path.relative(path.resolve(os.tmpdir()), path.resolve(directory));
    assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative));
    fs.rmSync(directory, { recursive: true });
  });
  const env = { SOURCE_SHA: 'a'.repeat(40), VERSION: '1.2.3', GITHUB_REPOSITORY: 'owner/sdk' };
  let uploads = 0, publications = 0;
  const gh = (...args) => {
    if (args[0] === 'api' && args[1].includes('/git/ref/')) {
      return JSON.stringify({ object: { type: 'commit', sha: env.SOURCE_SHA } });
    }
    if (args[0] === 'api') {
      return JSON.stringify({ tag_name: 'acyclic-v1.2.3', target_commitish: env.SOURCE_SHA, assets: [] });
    }
    if (args[1] === 'upload') uploads++;
    else if (args[1] === 'edit') publications++;
    else throw new Error('unexpected publication operation');
    return '';
  };
  await assert.rejects(publish({ env, directory, gh }), /directory is empty/);
  assert.equal(uploads, 0);
  assert.equal(publications, 0);
  for (const name of ['a', 'b']) {
    const child = path.join(directory, name);
    fs.mkdirSync(child);
    fs.writeFileSync(path.join(child, 'duplicate.tgz'), name);
  }
  await assert.rejects(publish({ env, directory, gh }), /Duplicate release asset name/);
  assert.equal(uploads, 1);
  assert.equal(publications, 0);
});


test('large release resume examines remote names once and preserves exact digest admission', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-release-index-'));
  t.after(() => {
    const relative = path.relative(path.resolve(os.tmpdir()), path.resolve(directory));
    assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative));
    fs.rmSync(directory, { recursive: true });
  });
  const env = { SOURCE_SHA: 'a'.repeat(40), VERSION: '1.2.3', GITHUB_REPOSITORY: 'owner/sdk' };
  const remote = [];
  for (let index = 0; index < 1025; index++) {
    const name = `asset-${index}.tgz`;
    const bytes = `qualified body ${index}`;
    remote.push({ name, digest: 'sha256:' + createHash('sha256').update(bytes).digest('hex') });
    if (index >= 705) fs.writeFileSync(path.join(directory, name), bytes);
  }
  const originalParse = JSON.parse;
  let namesExamined = 0, mutations = 0, mode = 'complete';
  t.mock.method(JSON, 'parse', (...args) => {
    const decoded = originalParse(...args);
    if (decoded.tag_name) {
      for (const asset of decoded.assets) {
        const name = asset.name;
        Object.defineProperty(asset, 'name', { get() { namesExamined++; return name; } });
      }
    }
    return decoded;
  });
  const gh = (...args) => {
    if (args[0] === 'api' && args[1].includes('/git/ref/')) {
      return JSON.stringify({ object: { type: 'commit', sha: env.SOURCE_SHA } });
    }
    if (args[0] === 'api') {
      const assets = mode === 'duplicate' ? [...remote, remote.at(-1)] : remote;
      return JSON.stringify({ tag_name: 'acyclic-v1.2.3', target_commitish: env.SOURCE_SHA, assets });
    }
    assert.deepEqual(args, ['release', 'edit', 'acyclic-v1.2.3', '--repo', 'owner/sdk', '--draft=false']);
    mutations++;
    return '';
  };
  await publish({ env, directory, gh });
  assert.equal(mutations, 1);
  assert.ok(namesExamined <= remote.length + 320,
    `release resume examined ${namesExamined} remote names for320 files/${remote.length} remote assets`);
  t.diagnostic(`RELEASE_RESUME remote=1025 files=320 names_examined=${namesExamined}`);

  const changed = path.join(directory, remote.at(-1).name);
  fs.writeFileSync(changed, 'different bytes');
  await assert.rejects(publish({ env, directory, gh }), /different contents/);
  assert.equal(mutations, 1, 'conflicting digest published the release');
  mode = 'duplicate';
  await assert.rejects(publish({ env, directory, gh }), /Duplicate published asset name/);
  assert.equal(mutations, 1, 'duplicate remote identity published the release');
});
