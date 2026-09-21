#!/usr/bin/env node
"use strict";

const { createHash } = require("node:crypto");
const { spawnSync } = require("node:child_process");
const {
  chmodSync, copyFileSync, existsSync, openSync, closeSync, fsyncSync, mkdirSync,
  readFileSync, renameSync, rmSync, statSync, writeFileSync,
} = require("node:fs");
const { basename, dirname, join } = require("node:path");
const { hostTarget, verifyTarget } = require("./verify.js");

function sha256(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function syncFile(path) {
  const handle = openSync(path, process.platform === "win32" ? "r+" : "r");
  try { fsyncSync(handle); } finally { closeSync(handle); }
}

function syncDirectory(path) {
  if (process.platform === "win32") return;
  const handle = openSync(path, "r");
  try { fsyncSync(handle); } finally { closeSync(handle); }
}

function durableRename(from, to, helper, replace) {
  if (process.platform === "win32" && process.env.NODE_ENV !== "test") {
    const result = spawnSync(helper, ["__installer-rename", from, to, replace ? "replace" : "no-replace"], {
      stdio: "inherit",
    });
    if (result.error || result.status !== 0) {
      throw new Error(`durable Acyclic rename failed${result.error ? `: ${result.error.message}` : ""}`);
    }
    return;
  }
  renameSync(from, to);
  syncDirectory(dirname(to));
  if (dirname(from) !== dirname(to)) syncDirectory(dirname(from));
}

function durableRemove(path, helper) {
  if (!existsSync(path)) return;
  if (process.platform === "win32") {
    const tombstone = `${path}.delete-${process.pid}-${Date.now()}`;
    durableRename(path, tombstone, helper, false);
    rmSync(tombstone, { recursive: true, force: true });
    return;
  }
  rmSync(path, { recursive: true, force: true });
  syncDirectory(dirname(path));
}

function durableJson(path, value, helper) {
  const next = `${path}.next`;
  writeFileSync(next, `${JSON.stringify(value, null, 2)}\n`);
  syncFile(next);
  durableRename(next, path, helper, true);
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === "EPERM";
  }
}

function wait(milliseconds) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

function acquireInstallLock(directory) {
  const path = join(directory, "install.lock");
  const ownerPath = join(path, "owner.json");
  const deadline = Date.now() + 120_000;
  while (true) {
    let acquired = false;
    try {
      mkdirSync(path);
      acquired = true;
      writeFileSync(ownerPath, `${JSON.stringify({ pid: process.pid, createdAt: Date.now() })}\n`);
      syncFile(ownerPath);
      return () => rmSync(path, { recursive: true, force: true });
    } catch (error) {
      if (acquired) {
        rmSync(path, { recursive: true, force: true });
        throw error;
      }
      if (error?.code !== "EEXIST") throw error;
      let owner;
      try { owner = readJson(ownerPath); } catch { owner = null; }
      let initialized = true;
      if (!Number.isInteger(owner?.pid) || owner.pid <= 0) {
        try { initialized = Date.now() - statSync(path).mtimeMs >= 5_000; } catch { initialized = false; }
      }
      if (initialized && (!Number.isInteger(owner?.pid) || owner.pid <= 0 || !processExists(owner.pid))) {
        const stale = `${path}.stale-${process.pid}-${Date.now()}`;
        try {
          renameSync(path, stale);
          rmSync(stale, { recursive: true, force: true });
        } catch (claimError) {
          if (!["ENOENT", "EACCES", "EPERM"].includes(claimError?.code)) throw claimError;
        }
        continue;
      }
      if (Date.now() >= deadline) {
        throw new Error(`timed out waiting for Acyclic installer process ${owner.pid}`);
      }
      wait(25);
    }
  }
}

function testCrash(point) {
  if (process.env.NODE_ENV === "test" && process.env.ACYCLIC_INSTALL_TEST_CRASH === point) {
    process.exit(86);
  }
}

function testFailure(point) {
  if (process.env.NODE_ENV === "test" && process.env.ACYCLIC_INSTALL_TEST_FAIL === point) {
    throw new Error(`injected Acyclic installer failure at ${point}`);
  }
}

function recoverInterruptedInstall(directory, installed, identityPath, expectedSha256, helper) {
  const journalPath = join(directory, "install-transaction.json");
  if (!existsSync(journalPath)) return;
  const journal = readJson(journalPath);
  if (journal.version !== 1 || !["staged", "backed-up", "activated"].includes(journal.phase)) {
    throw new Error("unsupported Acyclic install transaction journal");
  }
  const paths = [
    [journal.staged, `${basename(installed)}.next-`],
    [journal.backup, `${basename(installed)}.backup-`],
    [journal.identityNext, `${basename(identityPath)}.next-`],
  ];
  for (const [name, prefix] of paths) {
    const nonce = typeof name === "string" && name.startsWith(prefix) ? name.slice(prefix.length) : "";
    if (basename(name ?? "") !== name || !/^\d+-\d+$/.test(nonce)) {
      throw new Error("unsafe Acyclic install transaction path");
    }
  }
  const staged = join(directory, journal.staged);
  const backup = join(directory, journal.backup);
  const identityNext = join(directory, journal.identityNext);
  const activated = existsSync(installed) && sha256(installed) === journal.sha256;
  if (activated) {
    if (journal.sha256 !== expectedSha256) {
      throw new Error("interrupted install belongs to different release bytes");
    }
    if (existsSync(identityNext)) durableRename(identityNext, identityPath, helper, true);
    else durableJson(identityPath, journal.identity, helper);
    durableRemove(backup, helper);
  } else if (existsSync(backup)) {
    durableRemove(installed, helper);
    durableRename(backup, installed, helper, false);
  }
  durableRemove(staged, helper);
  durableRemove(identityNext, helper);
  durableRemove(journalPath, helper);
}

function ensureInstalledLocked() {
  const { source, manifest } = verifyTarget(__dirname);
  const packageVersion = readJson(join(__dirname, "..", "package.json")).version;
  const target = hostTarget();
  const expectedSha256 = manifest.targets[target].sha256;
  const installed = join(__dirname, process.platform === "win32" ? "acyclic.exe" : "acyclic");
  const identityPath = join(__dirname, "installed-binary.json");
  const journalPath = join(__dirname, "install-transaction.json");
  recoverInterruptedInstall(__dirname, installed, identityPath, expectedSha256, source);
  const identity = existsSync(identityPath) ? readJson(identityPath) : null;
  if (identity) {
    if (identity.version === packageVersion && identity.sha256 !== expectedSha256) {
      throw new Error(`immutable Acyclic release ${packageVersion} has conflicting binary bytes`);
    }
    if (existsSync(installed) && sha256(installed) !== identity.sha256) {
      throw new Error("installed Acyclic binary does not match its durable identity");
    }
  }
  const nextIdentity = { version: packageVersion, target, sha256: expectedSha256 };
  if (existsSync(installed) && sha256(installed) === expectedSha256) {
    if (!identity || JSON.stringify(identity) !== JSON.stringify(nextIdentity)) {
      durableJson(identityPath, nextIdentity, source);
    }
    return installed;
  }

  const skipDrain = process.env.NODE_ENV === "test" && process.env.ACYCLIC_INSTALL_SKIP_DRAIN === "1";
  const drain = skipDrain ? { status: 0 } : spawnSync(source, ["__service-drain"], { stdio: "inherit" });
  if (drain.error || drain.status !== 0) {
    throw new Error(`Acyclic service did not drain before upgrade${drain.error ? `: ${drain.error.message}` : ""}`);
  }
  const nonce = `${process.pid}-${Date.now()}`;
  const staged = `${installed}.next-${nonce}`;
  const backup = `${installed}.backup-${nonce}`;
  const identityNext = `${identityPath}.next-${nonce}`;
  const journal = {
    version: 1, phase: "staged", sha256: expectedSha256,
    staged: basename(staged), backup: basename(backup),
    identityNext: basename(identityNext), identity: nextIdentity,
  };
  let backedUp = false;
  let identityPublished = false;
  try {
    copyFileSync(source, staged);
    if (process.platform !== "win32") chmodSync(staged, 0o755);
    syncFile(staged);
    if (sha256(staged) !== expectedSha256) {
      throw new Error("staged Acyclic binary checksum verification failed");
    }
    writeFileSync(identityNext, `${JSON.stringify(nextIdentity, null, 2)}\n`);
    syncFile(identityNext);
    durableJson(journalPath, journal, source);
    testCrash("staged");
    if (existsSync(installed)) {
      durableRename(installed, backup, source, false);
      backedUp = true;
    }
    journal.phase = "backed-up";
    durableJson(journalPath, journal, source);
    testCrash("backed-up");
    durableRename(staged, installed, source, false);
    journal.phase = "activated";
    durableJson(journalPath, journal, source);
    testCrash("activated");
    durableRename(identityNext, identityPath, source, true);
    identityPublished = true;
    testFailure("cleanup-after-identity");
    durableRemove(backup, source);
    durableRemove(journalPath, source);
    return installed;
  } catch (error) {
    if (identityPublished) throw error;
    if (backedUp) {
      durableRemove(installed, source);
      durableRename(backup, installed, source, false);
    }
    durableRemove(staged, source);
    durableRemove(identityNext, source);
    durableRemove(journalPath, source);
    throw error;
  }
}

function ensureInstalled() {
  const release = acquireInstallLock(__dirname);
  try {
    return ensureInstalledLocked();
  } finally {
    release();
  }
}

module.exports = { ensureInstalled };

if (require.main === module) ensureInstalled();
