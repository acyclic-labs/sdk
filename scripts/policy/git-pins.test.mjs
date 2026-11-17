import assert from 'node:assert/strict';
import {execFileSync, spawnSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import test from 'node:test';
import {bazelGitPins, checkFetchableGitPins} from '../policy-core.mjs';

test('Static fetches exact pins from each declared remote and rejects unavailable commits', t => {
  const root = mkdtempSync(resolve(tmpdir(), 'git-pin-conformance-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  const git = (...args) => execFileSync('git', args, {cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe']}).trim();
  git('init', '-q');
  writeFileSync(resolve(root, 'source'), 'first commit\n');
  git('add', 'source');
  git('-c', 'user.name=Pin conformance', '-c', 'user.email=pins@example.invalid', 'commit', '--no-gpg-sign', '-qm', 'reachable');
  const commit = git('rev-parse', 'HEAD'), remote = pathToFileURL(root).href;
  const declaration = (kind, sha = commit, url = remote) => `${kind}(remote = ${JSON.stringify(url)}, commit = ${JSON.stringify(sha)})\n`;
  const module = resolve(root, 'MODULE.bazel'), bzl = resolve(root, 'repos.bzl');
  writeFileSync(module, '# git_override(remote = "ignored", commit = "ignored")\n' + declaration('git_override'));
  writeFileSync(bzl, 'load("@bazel_tools//tools/build_defs/repo:git.bzl", repo = "git_repository")\n' + declaration('repo'));
  git('add', 'MODULE.bazel', 'repos.bzl');
  assert.equal(bazelGitPins(root).length, 2);
  assert.equal(checkFetchableGitPins(root), 1, 'identical remote/commit pairs fetch once');
  const cli = resolve(import.meta.dirname, '../check-git-pins.mjs');
  assert.equal(spawnSync(process.execPath, [cli], {cwd: root}).status, 0);
  const unreachable = '0'.repeat(40);
  writeFileSync(module, declaration('git_override', unreachable));
  assert.throws(() => checkFetchableGitPins(root), /MODULE\.bazel:1: Git commit 0{40} is not fetchable/);
  assert.equal(spawnSync(process.execPath, [cli], {cwd: root}).status, 1);
  mkdirSync(resolve(root, 'empty.git'));
  git('init', '--bare', '-q', 'empty.git');
  writeFileSync(module, declaration('git_override'));
  writeFileSync(bzl, declaration('git_repository', commit, pathToFileURL(resolve(root, 'empty.git')).href));
  assert.throws(() => checkFetchableGitPins(root), /repos\.bzl:1:.*not fetchable/, 'another remote cannot borrow an already fetched object');
  writeFileSync(bzl, 'repo = use_repo_rule("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")\n' + declaration('repo'));
  assert.equal(checkFetchableGitPins(root), 1);
  for (const source of [
    'git_override(remote = "somewhere", commit = SHA)',
    'git_repository(**kwargs)',
    'alias = git_repository',
    declaration('git_override', 'f4f972e06c32'),
    'load("git.bzl", repo = "git_repository")\nrepo(remote = URL, commit = SHA)',
  ]) {
    writeFileSync(module, source);
    assert.throws(() => bazelGitPins(root), /Git (?:commit|remote|rules)/);
  }
});
