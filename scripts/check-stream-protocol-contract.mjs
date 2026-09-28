import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const buf = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
const temporary = mkdtempSync(join(tmpdir(), "acyclic-stream-protocol-"));

try {
  const output = join(temporary, "stream.json");
  const result = spawnSync(buf, ["build", "--path", "rust/crates/stream/proto/stream", "--exclude-source-info", "-o", output], {
    cwd: root,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(`Stream descriptor build failed with status ${result.status ?? "unknown"}`);
  }

  const files = JSON.parse(readFileSync(output, "utf8")).file;
  const stream = files.find(file => file.name === "stream/v2/stream.proto");
  assert.ok(stream, "Stream v2 descriptor is missing");
  const messages = new Map(stream.messageType.map(message => [message.name, message]));
  const service = stream.service.find(item => item.name === "StreamService");
  assert.ok(service, "StreamService is missing");
  assert.deepEqual(service.method.map(method => method.name), [
    "InspectIdempotency", "Append", "Tail", "Fork", "Read", "Follow",
    "Children", "ChildrenPage", "Commit", "ReadCommit",
  ]);
  assert.equal(service.method.find(method => method.name === "Read").serverStreaming, true);
  assert.equal(service.method.find(method => method.name === "Follow").serverStreaming, true);

  for (const name of ["TrimRequest", "TrimReceipt", "DeleteRequest", "DeleteReceipt",
    "TrimMutation", "DeleteMutation", "CommittedTrim", "CommittedDelete", "RetiredCommitConflict"]) {
    assert.equal(messages.has(name), false, `${name} must not be published`);
  }

  const assertFields = (name, expected) => {
    const message = messages.get(name);
    assert.ok(message, `${name} is missing`);
    assert.deepEqual(message.field.map(field => [field.name, field.number]), expected);
  };
  const assertReserved = (name, numbers, names) => {
    const message = messages.get(name);
    assert.deepEqual(message.reservedRange?.map(range => [range.start, range.end]), numbers);
    assert.deepEqual(message.reservedName, names);
  };
  assertFields("TailResponse", [["tail", 1]]);
  assertReserved("TailResponse", [[2, 3]], ["trim_point"]);
  assertFields("CommitMutation", [["append", 1], ["fork", 2]]);
  assertReserved("CommitMutation", [[3, 4], [4, 5]], ["trim", "delete"]);
  assertFields("CommittedMutation", [["append", 1], ["fork", 2]]);
  assertReserved("CommittedMutation", [[3, 4], [4, 5]], ["trim", "delete"]);
  assertFields("CommitConflict", [["tail", 1], ["exists", 2]]);
  assertReserved("CommitConflict", [[3, 4]], ["retired"]);
  assertFields("IdempotencyObservation", [
    ["idempotency_key", 1], ["request_digest", 2], ["append", 3],
    ["fork", 4], ["commit", 7],
  ]);
  assertReserved("IdempotencyObservation", [[5, 6], [6, 7]], ["trim", "delete"]);
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
