import {execFileSync} from 'node:child_process';
import {laneArtifactViolation} from './lane-artifact-policy.mjs';

const [base, head, ...extra] = process.argv.slice(2);
if (base === undefined || !head || extra.length) throw new Error('usage: check-lane-artifacts.mjs <base-or-empty> <head>');
const git = (...args) => execFileSync('git', args, {encoding: 'utf8'});
// Resolve untrusted event inputs as commits before using them as diff arguments.
const commit = ref => git('rev-parse', '--verify', '--end-of-options', `${ref}^{commit}`).trim();
const tip = commit(head);
const paths = !base || /^0+$/.test(base)
  ? git('ls-tree', '-r', '-z', '--name-only', tip)
  : git('diff', '--no-renames', '--name-only', '-z', '--diff-filter=ACMT', commit(base), tip, '--');
const violations = paths.split('\0').filter(Boolean).flatMap(path => {
  const reason = laneArtifactViolation(path);
  return reason ? [`${JSON.stringify(path)}: ${reason}`] : [];
});
if (violations.length) {
  console.error(violations.join('\n'));
  process.exitCode = 1;
} else {
  console.log('Changed-path lane artifact policy passed');
}
