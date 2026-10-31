import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { resolveTagTarget } from "./publish-release-assets.mjs";
import test from "node:test";

// The fixture executes Node and the platform shell. Its output is captured from
// those actual processes; no npm, Cargo or registry response is manufactured.
test("a pending publication preserves its exact caller arguments through the native shell", () => {
  const directory = mkdtempSync(join(tmpdir(), "sdk-publication-"));
  const fixture = join(directory, "caller.mjs");
  const marker = join(directory, "accepted");
  const arguments_ = [marker, "space name", "single'quote", 'double"quote', "$() remains data", "` remains data", "line\nbreak"];
  const module = new URL("./registry-publication.mjs", import.meta.url).href;
  writeFileSync(fixture, `
import { RegistryPublicationPending } from ${JSON.stringify(module)};
import { existsSync, writeFileSync } from "node:fs";
const args = process.argv.slice(2);
if (existsSync(args[0])) {
  process.stdout.write(JSON.stringify({ state: "accepted", args }));
} else {
  writeFileSync(args[0], "accepted");
  const pending = new RegistryPublicationPending("fixture", "archive", "1", { visible: false });
  process.stdout.write(JSON.stringify({ code: pending.code, recovery: pending.recovery, args }));
}
`);
  const invoke = (program, args) => {
    const result = spawnSync(program, args, { encoding: "utf8", maxBuffer: Infinity });
    assert.ifError(result.error);
    return result;
  };
  const shell = command => process.platform === "win32"
    ? invoke("pwsh", ["-NoProfile", "-NonInteractive", "-Command", command])
    : invoke("/bin/sh", ["-c", command]);
  try {
    // The one original caller creates its own acceptance marker and exits with
    // Pending. Only this explicit caller resend invokes its printed command.
    const pending = invoke(process.execPath, ["--no-warnings", fixture, ...arguments_]);
    assert.equal(pending.status, 0, pending.stderr);
    const first = JSON.parse(pending.stdout);
    assert.equal(first.code, "REGISTRY_PUBLICATION_PENDING");
    assert.deepEqual(first.args, arguments_);
    const resumed = shell(first.recovery);
    assert.equal(resumed.status, 0, `step=publication-resume ${resumed.stderr}; recovery command: ${first.recovery}`);
    assert.deepEqual(JSON.parse(resumed.stdout), { state: "accepted", args: arguments_ });

    // Restoring a naive quote leaves the original apostrophe unpaired. The
    // real shell must refuse it rather than executing a different argument list.
    const broken = (process.platform === "win32" ? "& " : "")
      + [process.execPath, "--no-warnings", fixture, ...arguments_].map(value => `'${value}'`).join(" ");
    assert.notEqual(shell(broken).status, 0);
  } finally {
    assert.equal(resolve(dirname(directory)), resolve(tmpdir()));
    rmSync(directory, { recursive: true });
  }
});

test("release ancestry follows real immutable tag objects beyond the former depth bound", () => {
  const directory = mkdtempSync(join(tmpdir(), "sdk-release-tags-"));
  const git = args => {
    const result = spawnSync("git", ["-c", "user.name=Publication Contract", "-c", "user.email=publication@example.invalid",
      "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", ...args],
      { cwd: directory, encoding: "utf8", maxBuffer: Infinity });
    assert.ifError(result.error);
    assert.equal(result.status, 0, `step=release-tag-fixture ${result.stderr}; recovery command: git -C '${directory}' status`);
    return result.stdout.trim();
  };
  try {
    git(["init", "-q", "--initial-branch=control", "--template="]);
    git(["commit", "-q", "--allow-empty", "-m", "original publication"]);
    const source = git(["rev-parse", "HEAD"]);
    let target = source;
    for (let index = 0; index < 14; index++) {
      git(["tag", "-a", `release-${index}`, target, "-m", `ancestor ${index}`]);
      target = git(["rev-parse", `refs/tags/release-${index}`]);
    }
    // A later HEAD cannot replace the original source retained by those tags.
    git(["commit", "-q", "--allow-empty", "-m", "later unrelated publication"]);
    assert.notEqual(git(["rev-parse", "HEAD"]), source);
    const visited = new Set();
    const readTag = sha => {
      assert.equal(visited.has(sha), false, "one read per immutable tag object");
      visited.add(sha);
      // Object identity/type are parsed from real Git output, never a fabricated
      // GitHub response or a handwritten object header.
      const tag = git(["cat-file", "tag", sha]);
      const object = /^object ([0-9a-f]+)$/m.exec(tag);
      const type = /^type (commit|tag)$/m.exec(tag);
      assert.ok(object && type, tag);
      return { type: type[1], sha: object[1] };
    };
    const resolved = resolveTagTarget({ type: git(["cat-file", "-t", target]), sha: target }, readTag);
    assert.deepEqual(resolved, { type: "commit", sha: source });
    assert.equal(visited.size, 14);
    // Restoring the old depth > 10 refusal fails this same real-object path
    // before the pinned source can be resolved; no larger budget is supplied.
  } finally {
    assert.equal(resolve(dirname(directory)), resolve(tmpdir()));
    rmSync(directory, { recursive: true });
  }
});
