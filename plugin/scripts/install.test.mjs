import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
  chmodSync, existsSync, mkdirSync, readFileSync, readdirSync,
  realpathSync, rmSync, statSync, writeFileSync,
} from "node:fs";
import { join, relative } from "node:path";
import test from "node:test";
import { schedule } from "../../scripts/graphcoder-e2e/process.mjs";
import { admitManagers } from "./package-managers.mjs";

import { plugin, target, executableName, nativeVersion, digest, fixture, scope } from "./install-fixture.mjs";
const require = createRequire(import.meta.url);
const { linuxLibc } = require("../bin/verify.js");
const cases = [];
const scenario = (name, run) => cases.push({ name, run });

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

scenario("Linux libc selection is explicit and fail closed", async () => {
  assert.equal(linuxLibc({ header: { glibcVersionRuntime: "2.39" } }), "gnu");
  assert.equal(linuxLibc({ header: {} }), "musl");
});

scenario("installer recovers cleanup failure after durable identity publication", async () => {
  const value = fixture();
  try {
    writeFileSync(value.installed, "previous binary bytes");
    const interrupted = await value.install(null, "cleanup-after-identity");
    assert.notEqual(interrupted.code, 0);
    assert.match(interrupted.stderr, /injected Acyclic installer failure/);
    assert.equal(digest(value.installed), digest(value.source));
    assert.equal(existsSync(join(value.bin, "install-transaction.json")), true);

    const recovered = await value.install();
    assert.equal(recovered.code, 0, recovered.stderr);
    assert.equal(digest(value.installed), digest(value.source));
    const identity = JSON.parse(readFileSync(join(value.bin, "installed-binary.json"), "utf8"));
    assert.equal(identity.sha256, digest(value.source));
    assert.equal(existsSync(join(value.bin, "install-transaction.json")), false);
  } finally {
    await value.close();
  }
});

scenario("installed command is the verified executable itself", async () => {
  const value = fixture();
  try {
    const placeholder = join(value.bin, "acyclic");
    assert.equal(readFileSync(placeholder, "utf8").startsWith("#!"), false);
    const installed = await value.install();
    assert.equal(installed.code, 0, installed.stderr);
    assert.equal(digest(value.installed), digest(value.source));
    // On Windows the command's `bin/acyclic` resolves to acyclic.exe only
    // once the placeholder is gone; on Unix the placeholder is the executable.
    assert.equal(existsSync(placeholder), process.platform !== "win32");
    const ran = await value.run(value.installed, ["--version"]);
    assert.equal(ran.code, 0, ran.stderr);
    assert.equal(ran.stdout.trim(), nativeVersion);
  } finally {
    await value.close();
  }
});

scenario("every install runs the installed executable once, and a failed run does not fail it", async () => {
  const value = fixture();
  try {
    const { root, bin, installed } = value;
    const log = join(root, "warm-up.log");
    const warmUps = () => readFileSync(log, "utf8").trim().split("\n");
    const first = await value.install(undefined, undefined, { ACYCLIC_INSTALL_TEST_WARM_UP_LOG: log });
    assert.equal(first.code, 0, first.stderr);
    assert.deepEqual(warmUps(), [`${installed} status 0`]);
    const again = await value.install(undefined, undefined, { ACYCLIC_INSTALL_TEST_WARM_UP_LOG: log });
    assert.equal(again.code, 0, again.stderr);
    assert.deepEqual(warmUps(), [`${installed} status 0`, `${installed} status 0`]);
    const missing = join(root, "missing-executable");
    const failed = await value.install(undefined, undefined, {
      ACYCLIC_INSTALL_TEST_WARM_UP_LOG: log,
      ACYCLIC_INSTALL_TEST_WARM_UP_EXECUTABLE: missing,
    });
    assert.equal(failed.code, 0, failed.stderr);
    assert.equal(warmUps().at(-1), `${missing} error ENOENT`);
    assert.equal(digest(installed), digest(join(bin, target, executableName)));
  } finally { await value.close(); }
});

scenario("installer is idempotent and rejects modified installed bytes", async () => {
  const value = fixture();
  try {
    const first = await value.install();
    assert.equal(first.code, 0, first.stderr);
    const second = await value.install();
    assert.equal(second.code, 0, second.stderr);
    writeFileSync(value.installed, "tampered");
    const rejected = await value.install();
    assert.notEqual(rejected.code, 0);
    assert.match(rejected.stderr, /durable identity/);
  } finally {
    await value.close();
  }
});

scenario("running the installed command leaves the package unchanged", async () => {
  const value = fixture();
  try {
    const installed = await value.install();
    assert.equal(installed.code, 0, installed.stderr);
    const before = treeSnapshot(value.root);
    if (process.platform !== "win32") {
      for (const name of readdirSync(value.bin)) {
        const path = join(value.bin, name);
        chmodSync(path, statSync(path).mode & 0o555);
      }
      chmodSync(value.bin, 0o555);
      assert.equal(statSync(value.installed).mode & 0o111, 0o111);
    }
    const launched = await value.run(value.installed, ["--version"]);
    assert.equal(launched.code, 0, launched.stderr);
    assert.deepEqual(treeSnapshot(value.root), before);
  } finally {
    if (process.platform !== "win32" && existsSync(value.bin)) {
      chmodSync(value.bin, 0o755);
      for (const name of readdirSync(value.bin)) chmodSync(join(value.bin, name), 0o755);
    }
    await value.close();
  }
});

scenario("concurrent installers serialize first install", async () => {
  const value = fixture();
  try {
    const results = await Promise.all(Array.from({ length: 8 }, () => value.install()));
    for (const result of results) assert.equal(result.code, 0, result.stderr);
    assert.equal(digest(value.installed), digest(value.source));
    assert.equal(statSync(join(value.bin, "install.lock")).isFile(), true);
    assert.equal(existsSync(join(value.bin, "install-transaction.json")), false);
  } finally {
    await value.close();
  }
});

scenario("installer rejects traversal in a recovery journal", async () => {
  const value = fixture();
  try {
    const first = await value.install();
    assert.equal(first.code, 0, first.stderr);
    const identity = JSON.parse(readFileSync(join(value.bin, "installed-binary.json"), "utf8"));
    writeFileSync(join(value.bin, "install-transaction.json"), JSON.stringify({
      version: 1,
      phase: "activated",
      sha256: identity.sha256,
      staged: `${executableName}.next-1-1`,
      backup: ".",
      identityNext: "installed-binary.json.next-1-1",
      identity,
    }));
    const rejected = await value.install();
    assert.notEqual(rejected.code, 0);
    assert.match(rejected.stderr, /step=publish native package/);
    assert.equal(existsSync(value.installed), true);
    assert.equal(existsSync(value.bin), true);
  } finally {
    await value.close();
  }
});

for (const crash of ["staged", "ready", "published", "activated"]) {
  scenario(`installer recovers after the ${crash} transaction boundary`, async () => {
    const value = fixture();
    try {
      const interrupted = await value.install(crash);
      assert.equal(interrupted.code, 86, interrupted.stderr);
      const recovered = await value.install();
      assert.equal(recovered.code, 0, recovered.stderr);
      assert.equal(digest(value.installed), digest(value.source));
      const identity = JSON.parse(readFileSync(join(value.bin, "installed-binary.json"), "utf8"));
      assert.equal(identity.sha256, digest(value.source));
    } finally {
      await value.close();
    }
  });
}

scenario("same version cannot be replaced with different bytes", async () => {
  const value = fixture();
  try {
    const first = await value.install();
    assert.equal(first.code, 0, first.stderr);
    writeFileSync(value.source, Buffer.concat([readFileSync(value.source), Buffer.from("different release bytes")]));
    const manifest = JSON.parse(readFileSync(join(value.bin, "platform-binaries.json"), "utf8"));
    manifest.targets[target].sha256 = digest(value.source);
    writeFileSync(join(value.bin, "platform-binaries.json"), JSON.stringify(manifest));
    const conflict = await value.install();
    assert.notEqual(conflict.code, 0);
    assert.match(conflict.stderr, /immutable Acyclic release/);
  } finally {
    await value.close();
  }
});

scenario("the published package declares the native session MCP server on the installed command", async () => {
  // npm 12 keys the report by package name; earlier npm returns an array.
  const [packed] = Object.values(JSON.parse(await managers.npm.run(preparation, ["pack", "--dry-run", "--json", "--ignore-scripts"], plugin)));
  const files = packed.files.map(file => file.path.replaceAll("\\", "/"));
  for (const path of [".mcp.json", ".codex-plugin/plugin.json", "hooks/hooks.json", "bin/acyclic"]) {
    assert.ok(files.includes(path), `package omits ${path}`);
  }
  assert.equal(JSON.parse(readFileSync(join(plugin, ".codex-plugin", "plugin.json"), "utf8")).mcpServers, "./.mcp.json");
  const servers = JSON.parse(readFileSync(join(plugin, ".mcp.json"), "utf8")).mcpServers;
  assert.deepEqual(Object.keys(servers), ["acyclic-sessions"]);
  // Codex starts the server from the plugin root. `bin/acyclic` is the
  // native executable on Unix; on Windows the installer removes it, so Codex
  // resolves the command through PATHEXT to `bin/acyclic.exe` (checked for
  // every package manager below).
  assert.equal(servers["acyclic-sessions"].cwd, ".");
  assert.equal(servers["acyclic-sessions"].command, "./bin/acyclic");
  assert.deepEqual(servers["acyclic-sessions"].args, ["__mcp", "codex"]);
  assert.deepEqual(servers["acyclic-sessions"].env_vars, ["ACYCLIC_HOST_ARGS", "ACYCLIC_WORKSPACE", "ACYCLIC_TASK"]);
  assert.equal(servers["acyclic-sessions"].required, true);
});

// The same two immutable tarballs are read by every independent manager case.
// Each manager owns its consumer profile, cache/store and process-tree scope.
let managers, releases, preparation;
async function packageRelease(version, extraBytes) {
  const value = fixture();
  try {
    const manifest = JSON.parse(readFileSync(join(value.bin, "platform-binaries.json"), "utf8"));
    if (extraBytes) {
      writeFileSync(value.source, Buffer.concat([readFileSync(value.source), extraBytes]));
      manifest.targets[target].sha256 = digest(value.source);
      writeFileSync(join(value.bin, "platform-binaries.json"), JSON.stringify(manifest));
    }
    const packageJson = JSON.parse(readFileSync(join(plugin, "package.json"), "utf8"));
    writeFileSync(join(value.root, "package.json"), JSON.stringify({
      name: packageJson.name, version, bin: packageJson.bin,
      scripts: packageJson.scripts, files: packageJson.files,
    }));
    const packed = await managers.npm.run(value, ["pack", "--silent", "--pack-destination", value.root]);
    return { ...value, tarball: join(value.root, packed.trim().split(/\r?\n/).at(-1) ?? "") };
  } catch (cause) {
    await value.close();
    throw cause;
  }
}

const layouts = {
  npm: {
    install: (tarball, home) => ["install", "--global", "--prefix", home,
      "--allow-scripts=" + tarball, tarball],
    remove: home => ["uninstall", "--global", "--prefix", home, "@acyclic-labs/plugin"],
    bin: home => process.platform === "win32" ? home : join(home, "bin"),
    root: home => process.platform === "win32"
      ? join(home, "node_modules", "@acyclic-labs", "plugin")
      : join(home, "lib", "node_modules", "@acyclic-labs", "plugin"),
  },
  pnpm: {
    project: {},
    // Local tarballs require their exact resolved dependency paths. A bare
    // package-name approval does not authorize an arbitrary file artifact.
    prepare: home => writeFileSync(join(home, "pnpm-workspace.yaml"), JSON.stringify({
      allowBuilds: Object.fromEntries(releases.map(({ tarball }) =>
        [`@acyclic-labs/plugin@file:${relative(home, tarball).replaceAll("\\", "/")}`, true])),
    })),
    install: (tarball, home) => ["add", tarball, "--dir", home,
      "--store-dir", join(home, "store"), "--offline"],
    remove: home => ["remove", "@acyclic-labs/plugin", "--dir", home, "--store-dir", join(home, "store")],
    bin: home => join(home, "node_modules", ".bin"),
    root: home => join(home, "node_modules", "@acyclic-labs", "plugin"),
  },
  bun: {
    project: { trustedDependencies: ["@acyclic-labs/plugin"] },
    // The native Windows shim is owned by Bun and survives its package removal.
    keepsShims: process.platform === "win32",
    install: (tarball, home) => {
      const manifest = JSON.parse(readFileSync(join(home, "package.json"), "utf8"));
      manifest.dependencies = { "@acyclic-labs/plugin": `file:${tarball}` };
      writeFileSync(join(home, "package.json"), JSON.stringify(manifest));
      return ["install", "--cwd", home];
    },
    remove: home => ["remove", "@acyclic-labs/plugin", "--cwd", home],
    bin: home => join(home, "node_modules", ".bin"),
    root: home => join(home, "node_modules", "@acyclic-labs", "plugin"),
  },
};

// A fixed shell program exercises the package manager's real command shim.
// All variable paths remain in the environment, never in shell source text.
function invoke(owner, directory) {
  const env = { PATH: `${directory}${process.platform === "win32" ? ";" : ":"}${owner.env.PATH}` };
  return process.platform === "win32"
    ? owner.run(owner.env.ComSpec, ["/d", "/s", "/c", "acyclic-plugin --version"], env)
    : owner.run("sh", ["-c", "exec acyclic-plugin --version"], env);
}

for (const [name, layout] of Object.entries(layouts)) {
  scenario(`${name} links the acyclic-plugin command to the native executable`, async () => {
    const owner = scope(`${name}-`), home = join(owner.root, "consumer");
    mkdirSync(home);
    try {
      if (layout.project) {
        writeFileSync(join(home, "package.json"), JSON.stringify({ name: "consumer", private: true, ...layout.project }));
      }
      layout.prepare?.(home);
      for (const release of releases) {
        await managers[name].run(owner, layout.install(release.tarball, home), home);
        const bin = join(layout.root(home), "bin"), executable = join(bin, executableName);
        assert.equal(digest(executable), digest(release.source));
        const ran = await invoke(owner, layout.bin(home));
        assert.equal(ran.code, 0, ran.stderr);
        assert.equal(ran.stdout.trim(), nativeVersion);
        if (process.platform === "win32") {
          assert.equal(existsSync(join(bin, "acyclic")), false);
          for (const shim of readdirSync(layout.bin(home)).filter(file => file.startsWith("acyclic"))) {
            const path = join(layout.bin(home), shim);
            if (statSync(path).size > 64 * 1024) continue;
            assert.doesNotMatch(readFileSync(path, "utf8"), /node(\.exe)?["' ]|_prog/, `${shim} starts an interpreter`);
          }
        } else if (name === "npm") {
          assert.equal(realpathSync(join(layout.bin(home), "acyclic-plugin")), realpathSync(executable));
        }
      }
      await managers[name].run(owner, layout.remove(home), home);
      assert.equal(existsSync(layout.root(home)), false);
      if (!layout.keepsShims) {
        const left = existsSync(layout.bin(home))
          ? readdirSync(layout.bin(home)).filter(file => file.startsWith("acyclic")) : [];
        assert.deepEqual(left, [], `${name} left ${left} in ${layout.bin(home)}`);
      }
    } finally { await owner.close(); }
  });
}

scenario("the acyclic-plugin command recovers when install scripts were skipped", async () => {
  const owner = scope("npm-no-scripts-"), home = join(owner.root, "consumer");
  mkdirSync(home);
  try {
    const release = releases[0];
    await managers.npm.run(owner, [...layouts.npm.install(release.tarball, home), "--ignore-scripts"], home);
    const bin = join(layouts.npm.root(home), "bin");
    assert.equal(existsSync(join(bin, "installed-binary.json")), false);
    if (process.platform === "win32") {
      // CreateProcess cannot execute the POSIX fallback. Explicit bootstrap is
      // required before the interpreter-free Windows command exists.
      const missing = await invoke(owner, layouts.npm.bin(home));
      assert.notEqual(missing.code, 0);
      const installed = await owner.run(process.execPath, [join(bin, "install.js")]);
      assert.equal(installed.code, 0, installed.stderr);
    }
    const first = await invoke(owner, layouts.npm.bin(home));
    assert.equal(first.code, 0, first.stderr);
    assert.equal(first.stdout.trim(), nativeVersion);
    assert.equal(digest(join(bin, executableName)), digest(release.source));
    if (process.platform !== "win32") {
      assert.equal(realpathSync(join(layouts.npm.bin(home), "acyclic-plugin")), realpathSync(join(bin, executableName)));
    }
    const again = await invoke(owner, layouts.npm.bin(home));
    assert.equal(again.code, 0, again.stderr);
    assert.equal(again.stdout.trim(), nativeVersion);
  } finally { await owner.close(); }
});

scenario("the acyclic-plugin command explains how to reinstall when it cannot install itself", async () => {
  const value = fixture();
  try {
    rmSync(join(value.bin, "platform-binaries.json"));
    const ran = process.platform === "win32"
      ? await value.run(process.execPath, [join(value.bin, "install.js")])
      : await value.run("sh", [join(value.bin, "acyclic"), "--version"]);
    assert.equal(ran.code, 1);
    assert.match(ran.stderr, /npm install -g --allow-scripts=@acyclic-labs\/plugin @acyclic-labs\/plugin/);
  } finally { await value.close(); }
});

scenario("package-manager admission refuses a missing pinned installation", async () => {
  await assert.rejects(admitManagers(preparation, join(preparation.root, "absent")),
    /step=admit pinned installer package managers:.*recovery=npm ci/s);
});

test("native package installation through every package manager", { concurrency: process.platform !== "win32" }, async context => {
  preparation = scope("package-preparation-");
  releases = [];
  try {
    managers = await admitManagers(preparation);
    // Each tarball is immutable once packing completes. All cases share these
    // read-only inputs; no consumer store or mutable profile is shared.
    releases.push(await packageRelease("9.8.7-test.1"));
    releases.push(await packageRelease("9.8.7-test.2", Buffer.from("upgraded release bytes")));
    await schedule(cases, process.platform, ({ name, run }) => context.test(name, run));
  } finally {
    for (const release of releases) await release.close();
    await preparation.close();
  }
});
