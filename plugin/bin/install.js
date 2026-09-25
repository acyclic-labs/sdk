#!/usr/bin/env node
"use strict";

// Installs the verified native executable as the package's `acyclic` command.
// Its full SHA-256 is checked against the release manifest here, once; the
// command then runs the executable directly, with nothing in between. Only
// installation mutates the package directory, so hosts may keep an installed
// package in an immutable store.

const { spawnSync } = require("node:child_process");
const { homedir, tmpdir } = require("node:os");
const {
  chmodSync, copyFileSync, existsSync, openSync, closeSync, fsyncSync, mkdirSync,
  readFileSync, renameSync, rmSync, statSync, writeFileSync,
} = require("node:fs");
const { basename, dirname, join } = require("node:path");
const { hostTarget, sha256File, verifyTarget } = require("./verify.js");

const sha256 = sha256File;

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

function durableStateDirectory() {
  if (process.platform === "win32" && process.env.LOCALAPPDATA) {
    return join(process.env.LOCALAPPDATA, "Acyclic", "state-v5");
  }
  if (process.platform !== "win32" && process.env.XDG_STATE_HOME) {
    return join(process.env.XDG_STATE_HOME, "acyclic", "state-v5");
  }
  const home = process.env.HOME || homedir();
  if (process.platform !== "win32" && home) {
    return join(home, ".local", "state", "acyclic", "state-v5");
  }
  return join(tmpdir(), "acyclic-state-v5");
}

function installCertification(packageVersion, helper) {
  const target = hostTarget();
  const platform = { linux: "linux", darwin: "macos", win32: "windows" }[process.platform];
  const architecture = { x64: "x86_64", arm64: "aarch64" }[process.arch];
  const backend = { linux: "linux-fuse", darwin: "macos-nfs", win32: "windows-projfs" }[process.platform];
  if (!platform || !architecture || !backend) return;
  const name = `native-mount-${target}.json`;
  const source = join(__dirname, "..", "certification", name);
  if (!existsSync(source)) return;
  const receipt = readJson(source);
  if (
    receipt.schema !== "acyclic-native-mount-qualification-v2"
    || receipt.os !== platform
    || receipt.arch !== architecture
    || receipt.required_kind !== backend
    || receipt.release_version !== packageVersion
    || receipt.passed !== true
    || typeof receipt.executable_blake3 !== "string"
    || !/^[0-9a-f]{64}$/.test(receipt.executable_blake3)
    || receipt.capability?.kind !== backend
    || receipt.capability?.available !== true
    || receipt.capability?.writable !== true
    || receipt.capability?.provider_process_io_observable !== (platform !== "windows")
    || receipt.capability?.session_isolation !== "SharedProcess"
    || receipt.capability?.unavailable_reason !== null
  ) {
    throw new Error(`invalid Acyclic platform certification receipt: ${name}`);
  }
  const testBypass = process.env.NODE_ENV === "test"
    && process.env.ACYCLIC_INSTALL_TEST_CERTIFICATION === "1";
  if (!testBypass) {
    const verified = spawnSync(helper, ["__verify-certification", source], { stdio: "inherit" });
    if (verified.error || verified.status !== 0) {
      throw new Error(`Acyclic platform certification does not match the installed binary${verified.error ? `: ${verified.error.message}` : ""}`);
    }
  }
  const directory = join(durableStateDirectory(), "certification");
  mkdirSync(directory, { recursive: true });
  durableJson(join(directory, name), receipt, helper);
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

// The package's `acyclic` command links to `bin/acyclic`, which ships as an
// interpreter-less placeholder so that every package manager links the command
// to the executable itself. On Unix the install replaces it with the
// executable; on Windows the command resolves `bin/acyclic` to `acyclic.exe`
// only once the placeholder is gone.
function removeCommandPlaceholder(helper) {
  if (process.platform === "win32") durableRemove(join(__dirname, "acyclic"), helper);
}

// A newly written executable is scanned on its first run (Windows Defender,
// macOS Gatekeeper), which costs up to a second. Running the installed
// executable once here pays that before a host's first hook does. Every
// install ends here, whether it wrote new bytes, recovered an interrupted
// install or found the executable already in place, so an upgrade or a moved
// package is warmed the same way. The warm-up is only an optimisation: its
// failure never fails the install.
function warmUp(installed) {
  const test = process.env.NODE_ENV === "test";
  const executable = (test && process.env.ACYCLIC_INSTALL_TEST_WARM_UP_EXECUTABLE) || installed;
  const result = spawnSync(executable, ["--version"], {
    stdio: "ignore", timeout: 30_000, windowsHide: true,
  });
  if (test && process.env.ACYCLIC_INSTALL_TEST_WARM_UP_LOG) {
    const outcome = result.error ? `error ${result.error.code}` : `status ${result.status}`;
    writeFileSync(process.env.ACYCLIC_INSTALL_TEST_WARM_UP_LOG, `${executable} ${outcome}\n`, { flag: "a" });
  }
}

function ensureInstalled() {
  const release = acquireInstallLock(__dirname);
  try {
    const installed = ensureInstalledLocked();
    removeCommandPlaceholder(installed);
    const packageVersion = readJson(join(__dirname, "..", "package.json")).version;
    installCertification(packageVersion, installed);
    warmUp(installed);
    return installed;
  } finally {
    release();
  }
}

ensureInstalled();
