import { existsSync, lstatSync, mkdirSync, realpathSync } from "node:fs";
import { relative, resolve, sep, join } from "node:path";

export function failPath(message) {
  throw new Error(`qualification-path: ${message}`);
}

export function samePath(left, right) {
  return process.platform === "win32"
    ? resolve(left).toLowerCase() === resolve(right).toLowerCase()
    : resolve(left) === resolve(right);
}

export function isWithin(root, candidate) {
  const rootPath = `${resolve(root).replace(/[\\/]$/u, "")}${sep}`;
  const candidatePath = resolve(candidate);
  return candidatePath === resolve(root) || candidatePath.startsWith(rootPath);
}

export function assertCanonicalParents(root, candidate, label, includeTarget = false) {
  const worktree = resolve(root);
  const target = resolve(candidate);
  if (!isWithin(worktree, target)) failPath(`${label} path escapes the owned root: ${candidate}`);
  const segments = relative(worktree, target).split(sep).filter(Boolean);
  const count = includeTarget ? segments.length : Math.max(0, segments.length - 1);
  let current = worktree;
  for (let index = 0; index < count; index += 1) {
    current = join(current, segments[index]);
    if (!existsSync(current)) break;
    const metadata = lstatSync(current);
    const canonical = realpathSync(current);
    if (!samePath(canonical, current)) failPath(`${label} parent is a junction or reparse point: ${current}`);
    if (metadata.isSymbolicLink() || !metadata.isDirectory()) failPath(`${label} parent is not a regular directory: ${current}`);
  }
}

export function ensureOwnedDirectory(target, worktree, label) {
  const destination = resolve(target);
  const root = resolve(worktree);
  assertCanonicalParents(root, destination, label);
  const segments = relative(root, destination).split(sep).filter(Boolean);
  let current = root;
  for (const segment of segments) {
    current = join(current, segment);
    if (!existsSync(current)) mkdirSync(current);
    assertCanonicalParents(root, current, label, true);
  }
  assertCanonicalParents(root, destination, label, true);
  return destination;
}
