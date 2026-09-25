import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import {
  chmodSync, copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync,
  realpathSync, rmSync, statSync, writeFileSync,
} from "node:fs";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const plugin = dirname(dirname(fileURLToPath(import.meta.url)));
const require = createRequire(import.meta.url);
const { hostTarget, linuxLibc, sha256File } = require("../bin/verify.js");
const target = hostTarget();
const executableName = process.platform === "win32" ? "acyclic.exe" : "acyclic";
const scratch = process.env.ACYCLIC_TEST_TEMP_ROOT
  || join(dirname(plugin), "target", "plugin-installer-tests");
mkdirSync(scratch, { recursive: true });

function digest(path) {
  return sha256File(path);
}

function treeSnapshot(root, prefix = "") {
  return readdirSync(root, { withFileTypes: true })
    .flatMap(entry => {
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      const path = join(root, entry.name);
      return entry.isDirectory()
        ? treeSnapshot(path, relative)
        : [`${relative}:${statSync(path).size}:${digest(path)}`];
    })
    .sort();
}

function fixture() {
  const root = mkdtempSync(join(scratch, "install-"));
  const bin = join(root, "bin");
  cpSync(join(plugin, "bin"), bin, {
    recursive: true,
    filter: source => !basename(source).startsWith("acyclic."),
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

function install(bin, crash, failure, extraEnvironment = {}) {
  return spawnSync(process.execPath, [join(bin, "install.js")], {
    encoding: "utf8",
    env: {
      ...process.env,
      NODE_ENV: "test",
      ACYCLIC_INSTALL_SKIP_DRAIN: "1",
      ...extraEnvironment,
      ...(crash ? { ACYCLIC_INSTALL_TEST_CRASH: crash } : {}),
      ...(failure ? { ACYCLIC_INSTALL_TEST_FAIL: failure } : {}),
    },
  });
}

test("installer publishes the exact release certification receipt", () => {
  const value = fixture();
  const state = mkdtempSync(join(scratch, "state-"));
  try {
    const platform = { linux: "linux", darwin: "macos", win32: "windows" }[process.platform];
    const architecture = { x64: "x86_64", arm64: "aarch64" }[process.arch];
    const backend = { linux: "linux-fuse", darwin: "macos-nfs", win32: "windows-projfs" }[process.platform];
    const name = `native-mount-${target}.json`;
    const certification = join(value.root, "certification");
    mkdirSync(certification);
    const receipt = {
      schema: "acyclic-native-mount-qualification-v2",
      os: platform,
      arch: architecture,
      required_kind: backend,
      release_version: "9.8.7-test.1",
      executable_blake3: "a".repeat(64),
      capability: {
        kind: backend,
        available: true,
        writable: true,
        provider_process_io_observable: platform !== "windows",
        session_isolation: "SharedProcess",
        unavailable_reason: null,
      },
      passed: true,
    };
    writeFileSync(join(certification, name), JSON.stringify(receipt));
    const environment = {
      ACYCLIC_INSTALL_TEST_CERTIFICATION: "1",
      ...(process.platform === "win32"
        ? { LOCALAPPDATA: state }
        : { XDG_STATE_HOME: state }),
    };
    const installed = install(value.bin, null, null, environment);
    assert.equal(installed.status, 0, installed.stderr);
    const destination = process.platform === "win32"
      ? join(state, "Acyclic", "state-v5", "certification", name)
      : join(state, "acyclic", "state-v5", "certification", name);
    assert.deepEqual(JSON.parse(readFileSync(destination, "utf8")), receipt);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
    rmSync(state, { recursive: true, force: true });
  }
});

test("Linux libc selection is explicit and fail closed", () => {
  assert.equal(linuxLibc({ header: { glibcVersionRuntime: "2.39" } }), "gnu");
  assert.equal(linuxLibc({ header: {} }), "musl");
});

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

test("installed command is the verified executable itself", () => {
  const value = fixture();
  try {
    const placeholder = join(value.bin, "acyclic");
    assert.equal(readFileSync(placeholder, "utf8").startsWith("#!"), false);
    const installed = install(value.bin);
    assert.equal(installed.status, 0, installed.stderr);
    assert.equal(digest(value.installed), digest(value.source));
    // On Windows the command's `bin/acyclic` resolves to acyclic.exe only
    // once the placeholder is gone; on Unix the placeholder is the executable.
    assert.equal(existsSync(placeholder), process.platform !== "win32");
    const ran = spawnSync(value.installed, ["--version"], { encoding: "utf8" });
    assert.equal(ran.status, 0, ran.stderr);
    assert.equal(ran.stdout.trim(), process.version);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});

test("installer is idempotent and rejects modified installed bytes", () => {
  const value = fixture();
  try {
    const first = install(value.bin);
    assert.equal(first.status, 0, first.stderr);
    const second = install(value.bin);
    assert.equal(second.status, 0, second.stderr);
    writeFileSync(value.installed, "tampered");
    const rejected = install(value.bin);
    assert.notEqual(rejected.status, 0);
    assert.match(rejected.stderr, /durable identity/);
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});

test("running the installed command leaves the package unchanged", () => {
  const value = fixture();
  try {
    const installed = install(value.bin);
    assert.equal(installed.status, 0, installed.stderr);
    const before = treeSnapshot(value.root);
    if (process.platform !== "win32") {
      // Read-only, but still executable.
      for (const name of readdirSync(value.bin)) chmodSync(join(value.bin, name), 0o555);
      chmodSync(value.bin, 0o555);
    }
    const launched = spawnSync(value.installed, ["--version"], { encoding: "utf8" });
    assert.equal(launched.status, 0, launched.stderr);
    assert.deepEqual(treeSnapshot(value.root), before);
  } finally {
    if (process.platform !== "win32" && existsSync(value.bin)) {
      chmodSync(value.bin, 0o755);
      for (const name of readdirSync(value.bin)) chmodSync(join(value.bin, name), 0o755);
    }
    rmSync(value.root, { recursive: true, force: true });
  }
});

test("concurrent installers serialize first install", async () => {
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

// Installs a package whose "native executable" is this Node binary with each
// package manager, and checks that the `acyclic` command runs the executable
// with no interpreter in between, across an upgrade to different bytes and an
// uninstall.
function packageRelease(version, extraBytes) {
  const value = fixture();
  const manifest = JSON.parse(readFileSync(join(value.bin, "platform-binaries.json")));
  if (extraBytes) {
    writeFileSync(value.source, Buffer.concat([readFileSync(value.source), extraBytes]));
    manifest.targets[target].sha256 = digest(value.source);
    writeFileSync(join(value.bin, "platform-binaries.json"), JSON.stringify(manifest));
  }
  const packageJson = JSON.parse(readFileSync(join(plugin, "package.json")));
  writeFileSync(join(value.root, "package.json"), JSON.stringify({
    name: packageJson.name,
    version,
    bin: packageJson.bin,
    scripts: packageJson.scripts,
  }));
  const packed = run(`npm pack --silent --pack-destination "${value.root}"`, value.root);
  return { ...value, tarball: join(value.root, packed.trim().split(/\r?\n/).pop()) };
}

function run(command, cwd, environment = {}) {
  const result = spawnSync(command, {
    cwd,
    shell: true,
    encoding: "utf8",
    env: { ...process.env, NODE_ENV: "test", ...environment },
  });
  assert.equal(result.status, 0, `${command}\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

function available(tool) {
  return spawnSync(`${tool} --version`, { shell: true }).status === 0;
}

const managers = {
  npm: {
    install: (tarball, home) => `npm install --global --prefix "${home}" --cache "${join(home, "cache")}" "${tarball}"`,
    remove: home => `npm uninstall --global --prefix "${home}" @acyclic-labs/plugin`,
    bin: home => (process.platform === "win32" ? home : join(home, "bin")),
    root: home => (process.platform === "win32"
      ? join(home, "node_modules", "@acyclic-labs", "plugin")
      : join(home, "lib", "node_modules", "@acyclic-labs", "plugin")),
  },
  pnpm: {
    project: { pnpm: { onlyBuiltDependencies: ["@acyclic-labs/plugin"] } },
    install: (tarball, home) => `pnpm add "${tarball}" --dir "${home}" --store-dir "${join(home, "store")}"`,
    remove: home => `pnpm remove @acyclic-labs/plugin --dir "${home}" --store-dir "${join(home, "store")}"`,
    bin: home => join(home, "node_modules", ".bin"),
    root: home => join(home, "node_modules", "@acyclic-labs", "plugin"),
  },
  bun: {
    project: { trustedDependencies: ["@acyclic-labs/plugin"] },
    // Bun leaves its Windows shims behind on removal for every package.
    keepsShims: process.platform === "win32",
    environment: home => ({ BUN_INSTALL_CACHE_DIR: join(home, "cache") }),
    // `bun add` of a second tarball of the same package reports a dependency
    // loop, so the upgrade changes the declared dependency and reinstalls.
    install: (tarball, home) => {
      const manifest = JSON.parse(readFileSync(join(home, "package.json")));
      manifest.dependencies = { "@acyclic-labs/plugin": `file:${tarball}` };
      writeFileSync(join(home, "package.json"), JSON.stringify(manifest));
      return `bun install --cwd "${home}"`;
    },
    remove: home => `bun remove @acyclic-labs/plugin --cwd "${home}"`,
    bin: home => join(home, "node_modules", ".bin"),
    root: home => join(home, "node_modules", "@acyclic-labs", "plugin"),
  },
};

for (const [name, manager] of Object.entries(managers)) {
  test(`${name} links the acyclic command to the native executable`, { skip: !available(name) }, () => {
    const first = packageRelease("9.8.7-test.1");
    const second = packageRelease("9.8.7-test.2", Buffer.from("upgraded release bytes"));
    const home = mkdtempSync(join(scratch, `${name}-`));
    try {
      if (manager.project) {
        writeFileSync(join(home, "package.json"), JSON.stringify({ name: "consumer", private: true, ...manager.project }));
      }
      const environment = manager.environment?.(home) ?? {};
      const commandPath = { PATH: `${manager.bin(home)}${process.platform === "win32" ? ";" : ":"}${process.env.PATH}` };
      const invoke = () => run("acyclic --version", home, { ...environment, ...commandPath }).trim();
      for (const release of [first, second]) {
        run(manager.install(release.tarball, home), home, environment);
        const bin = join(manager.root(home), "bin");
        const executable = join(bin, executableName);
        assert.equal(digest(executable), digest(release.source));
        assert.equal(invoke(), process.version);
        if (process.platform === "win32") {
          assert.equal(existsSync(join(bin, "acyclic")), false);
          // No shim may start an interpreter: each runs bin/acyclic itself.
          for (const shim of readdirSync(manager.bin(home)).filter(file => file.startsWith("acyclic"))) {
            const path = join(manager.bin(home), shim);
            if (statSync(path).size > 64 * 1024) continue;
            assert.doesNotMatch(readFileSync(path, "utf8"), /node(\.exe)?["' ]|_prog/, `${shim} starts an interpreter`);
          }
        } else if (name === "npm") {
          assert.equal(realpathSync(join(manager.bin(home), "acyclic")), realpathSync(executable));
        }
      }
      run(manager.remove(home), home, environment);
      assert.equal(existsSync(manager.root(home)), false);
      if (!manager.keepsShims) {
        const left = existsSync(manager.bin(home))
          ? readdirSync(manager.bin(home)).filter(file => file.startsWith("acyclic")) : [];
        assert.deepEqual(left, [], `${name} left ${left} in ${manager.bin(home)}`);
      }
    } finally {
      for (const path of [first.root, second.root, home]) rmSync(path, { recursive: true, force: true });
    }
  });
}
