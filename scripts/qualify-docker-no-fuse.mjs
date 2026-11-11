#!/usr/bin/env node
// R27: the shipping harness in stock Docker, using the existing acceptance cases.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createReadStream, existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { pipeline } from "node:stream/promises";
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
  "child-fork-message-merge-physical", "child-completion-merge",
  "child-completion-merge-physical", "jsonl-reopen", "kill-and-resume"];
const root = fileURLToPath(new URL("../", import.meta.url));
const recovery = "node scripts/qualify-docker-no-fuse.mjs IMAGE GRAPHCODER MOCK_MODEL FRESH_ABSOLUTE_OUTPUT";

export function confinement(status, fuse, helpers, mountinfo) {
  assert.equal(fuse, false, "docker-no-fuse requires /dev/fuse to be absent");
  assert.deepEqual(helpers, [], "docker-no-fuse must not depend on a mount helper");
  assert.equal(typeof mountinfo, "string", "docker-no-fuse requires mount namespace evidence");
  assert.ok(mountinfo.trim(), "docker-no-fuse requires nonempty mount namespace evidence");
  assert.ok(!mountinfo.split("\n").some(line => line.split(" - ")[1]?.startsWith("fusectl ")),
    "docker-no-fuse must not have fusectl mounted");
  const fields = new Map(status.trim().split("\n").map(line => line.split(/:\s*/, 2)));
  for (const key of ["CapEff", "CapBnd"]) {
    assert.ok(fields.has(key), `missing kernel ${key}`);
    assert.equal(BigInt(`0x${fields.get(key)}`) & (1n << 21n), 0n,
      `docker-no-fuse must not have CAP_SYS_ADMIN (${key})`);
  }
  assert.equal(fields.get("Seccomp"), "2", "Docker's seccomp filter must be active");
}

async function inside(binary, mock, output) {
  await mkdir(output);
  const status = await readFile("/proc/self/status", "utf8");
  const mountinfo = await readFile("/proc/self/mountinfo", "utf8");
  const helpers = ["/usr/bin/fusermount", "/bin/fusermount", "/usr/bin/fusermount3", "/bin/fusermount3"]
    .filter(existsSync);
  confinement(status, existsSync("/dev/fuse"), helpers, mountinfo);
  const directory = join(root, "scripts/graphcoder-e2e");
  const selected = inventory(directory).filter(test => cases.includes(test.id));
  const environment = { ...nativeEnvironment(), HOME: "/tmp", E2E_PRIMARY_MOUNTING: "auto" };
  const binding = await acceptanceBinding(directory, binary, mock, selected, environment);
  binding.source["qualify-docker-no-fuse.mjs"] = await sha256File(fileURLToPath(import.meta.url));
  const processes = new Processes();
  const cancellation = commandCancellation();
  try {
    await withOwnedCase({ processes, signal: cancellation.signal, run: async () => {
      const result = await processes.run(process.execPath,
        [join(directory, "run.mjs"), binary, mock, ...cases], {
          cwd: root, env: { ...environment, E2E_OUTPUT: join(output, "acceptance") },
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
        confinement: { status, fuse: false, fusectl: false, helpers, mountinfo },
      }, null, 2) + "\n");
    } });
  } finally { cancellation.dispose(); }
}

async function outside(image, binary, mock, output) {
  assert.ok(image && [binary, mock, output].every(path => path && isAbsolute(path)), recovery);
  await mkdir(output); // Evidence is fresh; an earlier PASS can never qualify this run.
  await mkdir(join(output, "transcripts"));
  const processes = new Processes({ transcripts: join(output, "transcripts") });
  const cancellation = commandCancellation();
  const name = `graphcoder-no-fuse-${randomUUID()}`;
  let creating, started = false;
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
        try {
          const stopped = await cleanup.run("docker", ["stop", name]);
          assert.equal(stopped.code, 0, stopped.stderr);
          if (started) {
            const copied = await cleanup.run("docker", ["cp", `${name}:/tmp/evidence/.`, output]);
            assert.equal(copied.code, 0, copied.stderr);
          }
        } finally {
          const removed = await cleanup.run("docker", ["rm", "--force", name]);
          assert.equal(removed.code, 0, removed.stderr);
        }
      }
    } finally { await cleanup.close(); await processes.close(); }
  } };
  try {
    await withOwnedCase({ processes: owner, signal: cancellation.signal, prepare: async () => {
      const id = await docker(["image", "inspect", "--format", "{{.Id}}", image]);
      // Cleanup joins create before removing its daemon-owned object. Merely
      // killing the CLI could otherwise leave a late successful create behind.
      creating = processes.run("docker", ["create", "--name", name,
        "--network", "none", "--user", `${process.getuid()}:${process.getgid()}`,
        "--workdir", "/sdk", "--entrypoint", "node", id,
        "scripts/qualify-docker-no-fuse.mjs", "--inside", "/graphcoder",
        "/harness-mock-model", "/tmp/evidence"]);
      const result = await creating;
      assert.equal(result.code, 0, result.stderr);
      // Client paths need not exist in the daemon's filesystem namespace.
      // Transfer a committed source snapshot through the Docker API instead.
      const archive = join(output, "source.tar");
      const archived = await processes.run("git", ["-C", root, "archive", "--format=tar",
        `--output=${archive}`, "HEAD", "scripts", "plugin/tests/hosts", "typescript/packages/sdk/native"]);
      assert.equal(archived.code, 0, archived.stderr);
      const copy = processes.start("docker", ["cp", "-", `${name}:/sdk`]);
      await pipeline(createReadStream(archive), copy.input);
      const copied = await copy.closed;
      assert.equal(copied.code, 0, copied.stderr);
      // Preserve the caller's executable ownership, including owner-only modes.
      await docker(["cp", "--archive", "--follow-link", binary, `${name}:/graphcoder`]);
      await docker(["cp", "--archive", "--follow-link", mock, `${name}:/harness-mock-model`]);
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
      await writeFile(join(output, "docker.json"), JSON.stringify({
        image: id, config, sourceArchive: await sha256File(archive),
      }, null, 2) + "\n");
    }, run: async () => {
      started = true;
      await docker(["start", "--attach", name]);
      const code = await docker(["inspect", "--format", "{{.State.ExitCode}}", name]);
      assert.equal(code, "0", "Docker container failed");
    } });
    const report = JSON.parse(await readFile(join(output, "container.json")));
    assert.equal(report.status, "PASS");
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
