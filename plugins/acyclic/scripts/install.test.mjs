import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import {
  copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const plugin = dirname(dirname(fileURLToPath(import.meta.url)));
const target = `${{ win32: "win32", darwin: "darwin", linux: "linux" }[process.platform]}-${process.arch}`;
const executableName = process.platform === "win32" ? "acyclic.exe" : "acyclic";

function digest(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "acyclic-install-"));
  const bin = join(root, "bin");
  cpSync(join(plugin, "bin"), bin, {
    recursive: true,
    filter: source => !basename(source).startsWith("acyclic.") || ["acyclic.js"].includes(basename(source)),
  });
  writeFileSync(join(root, "package.json"), JSON.stringify({ version: "9.8.7-test.1" }));
  const source = join(bin, target, executableName);
  mkdirSync(dirname(source), { recursive: true });
  copyFileSync(process.execPath, source);
  writeFileSync(join(bin, "platform-binaries.json"), JSON.stringify({
    version: 1,
    targets: { [target]: { path: `${target}/${executableName}`, sha256: digest(source) } },
  }));
  return { root, bin, source, installed: join(bin, executableName) };
}

function install(bin, crash, failure) {
  return spawnSync(process.execPath, [join(bin, "install.js")], {
    encoding: "utf8",
    env: {
      ...process.env,
      NODE_ENV: "test",
      ACYCLIC_INSTALL_SKIP_DRAIN: "1",
      ...(crash ? { ACYCLIC_INSTALL_TEST_CRASH: crash } : {}),
      ...(failure ? { ACYCLIC_INSTALL_TEST_FAIL: failure } : {}),
    },
  });
}

test("installer recovers cleanup failure after durable identity publication", () => {
  const value = fixture();
  try {
    writeFileSync(value.installed, "previous binary bytes");
    const interrupted = install(value.bin, null, "cleanup-after-identity");
    assert.notEqual(interrupted.status, 0);
    assert.match(interrupted.stderr, /injected Acyclic installer failure/);
    assert.equal(digest(value.installed), digest(value.source));
    assert.equal(existsSync(join(value.bin, "install-transaction.json")), true);

    const recovered = install(value.bin);
    assert.equal(recovered.status, 0, recovered.stderr);
    assert.equal(digest(value.installed), digest(value.source));
    const identity = JSON.parse(readFileSync(join(value.bin, "installed-binary.json")));
    assert.equal(identity.sha256, digest(value.source));
    assert.equal(existsSync(join(value.bin, "install-transaction.json")), false);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});

function installAsync(bin) {
  return new Promise(resolve => {
    const child = spawn(process.execPath, [join(bin, "install.js")], {
      env: {
        ...process.env,
        NODE_ENV: "test",
        ACYCLIC_INSTALL_SKIP_DRAIN: "1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", chunk => { stderr += chunk; });
    child.on("close", status => resolve({ status, stderr }));
  });
}

test("installer is idempotent and launcher rejects modified installed bytes", () => {
  const value = fixture();
  try {
    const first = install(value.bin);
    assert.equal(first.status, 0, first.stderr);
    const second = install(value.bin);
    assert.equal(second.status, 0, second.stderr);
    writeFileSync(value.installed, "tampered");
    const launched = spawnSync(process.execPath, [join(value.bin, "acyclic.js"), "--version"], {
      encoding: "utf8",
      env: { ...process.env, NODE_ENV: "test", ACYCLIC_INSTALL_SKIP_DRAIN: "1" },
    });
    assert.notEqual(launched.status, 0);
    assert.match(launched.stderr, /durable identity/);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});

test("concurrent launchers serialize first install", async () => {
  const value = fixture();
  try {
    const results = await Promise.all(Array.from({ length: 8 }, () => installAsync(value.bin)));
    for (const result of results) assert.equal(result.status, 0, result.stderr);
    assert.equal(digest(value.installed), digest(value.source));
    assert.equal(existsSync(join(value.bin, "install.lock")), false);
    assert.equal(existsSync(join(value.bin, "install-transaction.json")), false);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});

test("installer recovers a lock whose owner exited", () => {
  const value = fixture();
  try {
    const lock = join(value.bin, "install.lock");
    mkdirSync(lock);
    writeFileSync(join(lock, "owner.json"), JSON.stringify({ pid: 2147483647, createdAt: 0 }));
    const recovered = install(value.bin);
    assert.equal(recovered.status, 0, recovered.stderr);
    assert.equal(digest(value.installed), digest(value.source));
    assert.equal(existsSync(lock), false);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});

test("installer rejects traversal in a recovery journal", () => {
  const value = fixture();
  try {
    const first = install(value.bin);
    assert.equal(first.status, 0, first.stderr);
    const identity = JSON.parse(readFileSync(join(value.bin, "installed-binary.json")));
    writeFileSync(join(value.bin, "install-transaction.json"), JSON.stringify({
      version: 1,
      phase: "activated",
      sha256: identity.sha256,
      staged: `${executableName}.next-1-1`,
      backup: ".",
      identityNext: "installed-binary.json.next-1-1",
      identity,
    }));
    const rejected = install(value.bin);
    assert.notEqual(rejected.status, 0);
    assert.match(rejected.stderr, /unsafe Acyclic install transaction path/);
    assert.equal(existsSync(value.installed), true);
    assert.equal(existsSync(value.bin), true);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});

for (const crash of ["staged", "backed-up", "activated"]) {
  test(`installer recovers after the ${crash} transaction boundary`, () => {
    const value = fixture();
    try {
      const interrupted = install(value.bin, crash);
      assert.equal(interrupted.status, 86, interrupted.stderr);
      const recovered = install(value.bin);
      assert.equal(recovered.status, 0, recovered.stderr);
      assert.equal(digest(value.installed), digest(value.source));
      const identity = JSON.parse(readFileSync(join(value.bin, "installed-binary.json")));
      assert.equal(identity.sha256, digest(value.source));
    } finally {
      rmSync(value.root, { recursive: true, force: true });
    }
  });
}

test("same version cannot be replaced with different bytes", () => {
  const value = fixture();
  try {
    const first = install(value.bin);
    assert.equal(first.status, 0, first.stderr);
    writeFileSync(value.source, "different release bytes");
    const manifest = JSON.parse(readFileSync(join(value.bin, "platform-binaries.json")));
    manifest.targets[target].sha256 = digest(value.source);
    writeFileSync(join(value.bin, "platform-binaries.json"), JSON.stringify(manifest));
    const conflict = install(value.bin);
    assert.notEqual(conflict.status, 0);
    assert.match(conflict.stderr, /immutable Acyclic release/);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});
