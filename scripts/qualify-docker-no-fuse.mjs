#!/usr/bin/env node
// R27: the shipping harness in stock Docker, using the existing acceptance cases.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Processes } from "./graphcoder-e2e/process.mjs";
import { commandCancellation, withOwnedCase } from "./graphcoder-e2e/suite.mjs";
import { acceptanceBinding } from "./graphcoder-e2e/serial.mjs";
import { inventory } from "./graphcoder-e2e/coverage.mjs";
import { nativeEnvironment } from "./graphcoder-e2e/scratch.mjs";
import { sha256File } from "./file-digest.mjs";

// Reuse the original assertions, provider scripts, capture and event waits.
// Auto must work without a flag; physical proves the explicit selection too.
const cases = ["file-tools-responses", "file-tools-responses-physical",
  "bash-capture", "bash-capture-physical", "child-fork-message-merge",
  "child-fork-message-merge-physical", "jsonl-reopen", "kill-and-resume"];
const root = fileURLToPath(new URL("../", import.meta.url));
const recovery = "node scripts/qualify-docker-no-fuse.mjs IMAGE GRAPHCODER MOCK_MODEL FRESH_ABSOLUTE_OUTPUT";

export function confinement(status, fuse, helpers) {
  assert.equal(fuse, false, "docker-no-fuse requires /dev/fuse to be absent");
  assert.deepEqual(helpers, [], "docker-no-fuse must not depend on a mount helper");
  const fields = new Map(status.trim().split("\n").map(line => line.split(/:\s*/, 2)));
  for (const key of ["CapEff", "CapBnd"]) {
    assert.ok(fields.has(key), `missing kernel ${key}`);
    assert.equal(BigInt(`0x${fields.get(key)}`) & (1n << 21n), 0n,
      `docker-no-fuse must not have CAP_SYS_ADMIN (${key})`);
  }
  assert.equal(fields.get("Seccomp"), "2", "Docker's seccomp filter must be active");
}

async function inside(binary, mock, output) {
  const status = await readFile("/proc/self/status", "utf8");
  const helpers = ["/usr/bin/fusermount", "/bin/fusermount", "/usr/bin/fusermount3", "/bin/fusermount3"]
    .filter(existsSync);
  confinement(status, existsSync("/dev/fuse"), helpers);
  const directory = join(root, "scripts/graphcoder-e2e");
  const selected = inventory(directory).filter(test => cases.includes(test.id));
  const binding = await acceptanceBinding(directory, binary, mock, selected, nativeEnvironment());
  binding.source["qualify-docker-no-fuse.mjs"] = await sha256File(fileURLToPath(import.meta.url));
  const processes = new Processes();
  const cancellation = commandCancellation();
  try {
    await withOwnedCase({ processes, signal: cancellation.signal, run: async () => {
      const result = await processes.run(process.execPath,
        [join(directory, "run.mjs"), binary, mock, ...cases], {
          cwd: root, env: { ...nativeEnvironment(), HOME: "/tmp", E2E_OUTPUT: join(output, "acceptance") },
        });
      process.stdout.write(result.stdout);
      process.stderr.write(result.stderr);
      assert.equal(result.code, 0, "shipping acceptance failed inside stock Docker");
      const report = JSON.parse(await readFile(join(output, "acceptance/matrix.json")));
      assert.deepEqual(report.selected, cases);
      assert.equal(report.results.length, cases.length);
      assert.ok(report.results.every(result => result.status === "pass"));
      for (const result of report.results.filter(result => !result.case.endsWith("-physical"))) {
        assert.ok(result.evidence.captures.some(capture => capture.stderr.includes("using physical workspace mounting")),
          `${result.case}: auto must select physical mode and report why`);
      }
      await writeFile(join(output, "container.json"), JSON.stringify({
        schema: "graphcoder.docker-no-fuse.v1", status: "PASS", cases, binding,
        confinement: { status, fuse: false, helpers },
      }, null, 2) + "\n");
    } });
  } finally { cancellation.dispose(); }
}

async function outside(image, binary, mock, output) {
  assert.ok(image && [binary, mock, output].every(path => path && isAbsolute(path)), recovery);
  await mkdir(output); // Evidence is fresh; an earlier PASS can never qualify this run.
  const processes = new Processes({ transcripts: join(output, "transcripts") });
  const cancellation = commandCancellation();
  const name = `graphcoder-no-fuse-${randomUUID()}`;
  let creating;
  const docker = async args => {
    const result = await processes.run("docker", args);
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    assert.equal(result.code, 0, `docker ${args[0]} failed; recovery: ${recovery}`);
    return result.stdout.trim();
  };
  // Docker owns processes outside the CLI's process tree. Remove that same
  // container before closing the CLI owner, including on caller cancellation.
  const owner = { async close() {
    const cleanup = new Processes();
    try {
      const created = creating && (await creating).code === 0;
      if (created) {
        const result = await cleanup.run("docker", ["rm", "--force", name]);
        assert.equal(result.code, 0, result.stderr);
      }
    } finally { await cleanup.close(); await processes.close(); }
  } };
  try {
    await withOwnedCase({ processes: owner, signal: cancellation.signal, prepare: async () => {
      const id = await docker(["image", "inspect", "--format", "{{.Id}}", image]);
      const volume = (source, target, mode) => ["--volume", `${source}:${target}:${mode}`];
      // Cleanup joins create before removing its daemon-owned object. Merely
      // killing the CLI could otherwise leave a late successful create behind.
      creating = processes.run("docker", ["create", "--name", name,
        "--network", "none", "--user", `${process.getuid()}:${process.getgid()}`,
        ...volume(root, "/sdk", "ro"), ...volume(binary, "/artifacts/graphcoder", "ro"),
        ...volume(mock, "/artifacts/harness-mock-model", "ro"), ...volume(output, "/evidence", "rw"),
        "--workdir", "/sdk", "--entrypoint", "node", id,
        "scripts/qualify-docker-no-fuse.mjs", "--inside", "/artifacts/graphcoder",
        "/artifacts/harness-mock-model", "/evidence"]);
      const result = await creating;
      assert.equal(result.code, 0, result.stderr);
      // Inspect only the contract's fields: unrelated daemon configuration
      // (for example logging-driver credentials) must not enter evidence.
      const format = '{"Privileged":{{json .HostConfig.Privileged}},"CapAdd":{{json .HostConfig.CapAdd}},'
        + '"Devices":{{json .HostConfig.Devices}},"DeviceRequests":{{json .HostConfig.DeviceRequests}},'
        + '"SecurityOpt":{{json .HostConfig.SecurityOpt}},"NetworkMode":{{json .HostConfig.NetworkMode}},'
        + '"User":{{json .Config.User}}}';
      const config = JSON.parse(await docker(["inspect", "--format", format, name]));
      assert.equal(config.Privileged, false);
      for (const key of ["CapAdd", "Devices", "DeviceRequests", "SecurityOpt"])
        assert.equal(config[key]?.length ?? 0, 0, `stock Docker requires default ${key}`);
      await writeFile(join(output, "docker.json"), JSON.stringify({ image: id, config }, null, 2) + "\n");
    }, run: async () => {
      await docker(["start", "--attach", name]);
      const code = await docker(["inspect", "--format", "{{.State.ExitCode}}", name]);
      assert.equal(code, "0", "Docker container failed");
      const report = JSON.parse(await readFile(join(output, "container.json")));
      assert.equal(report.status, "PASS");
    } });
    console.log(`docker-no-fuse PASS ${JSON.stringify({ cases, output })}`);
  } finally { cancellation.dispose(); }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  assert.equal(process.platform, "linux", "docker-no-fuse is Linux acceptance");
  const [mode, ...args] = process.argv.slice(2);
  try {
    if (mode === "--inside") await inside(...args);
    else await outside(mode, ...args);
  } catch (error) { console.error(error); process.exitCode = 1; }
}
