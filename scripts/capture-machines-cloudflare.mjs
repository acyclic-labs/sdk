// Captures native local Container responses; never synthesizes provider fixtures.
import { createHash, randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";

const [endpoint, output, sourceCommit] = process.argv.slice(2);
if (endpoint !== "http://127.0.0.1:15440" || !output || !/^[a-f0-9]{40}$/.test(sourceCommit ?? "")) {
  throw new Error("step=snapshot-capture-admission; recovery: node scripts/capture-machines-cloudflare.mjs http://127.0.0.1:15440 OUTPUT.json SOURCE_COMMIT");
}
const namespace = randomUUID();
const parent = `acyclic-${namespace}-${randomUUID()}-0`;
const child = `acyclic-${namespace}-${randomUUID()}-0`;
const exchanges = [];
const hash = bytes => createHash("sha256").update(bytes).digest("hex");
let snapshot;
let failure;
const cleanupFailures = [];
async function request(name, suffix, method, body) {
  const url = `${endpoint}/sandboxes/${name}${suffix}`;
  const sent = body === undefined ? undefined : Buffer.from(typeof body === "string" ? body : JSON.stringify(body));
  const response = await fetch(url, {
    method, body: sent, signal: AbortSignal.timeout(30_000),
    headers: sent === undefined ? {} : { "content-type": "application/json" },
  });
  const bytes = Buffer.from(await response.arrayBuffer());
  exchanges.push({
    captured_at: new Date().toISOString(), url, method,
    request_base64: sent?.toString("base64") ?? null,
    status: response.status, headers: [...response.headers],
    response_base64: bytes.toString("base64"), response_sha256: hash(bytes),
  });
  if (!response.ok) throw new Error(`step=${method} ${suffix || "/"}: HTTP ${response.status}; recovery: inspect capture ${output} and namespace ${namespace} before rerunning`);
  return bytes;
}
try {
  const create = { image: "workspace", env: {}, internet: false };
  await request(parent, "", "POST", create);
  await request(parent, "/file?path=/workspace/snapshot-probe", "PUT", "before\n");
  snapshot = JSON.parse((await request(parent, "/snapshot", "POST")).toString("utf8"));
  if (typeof snapshot.id !== "string" || snapshot.id.length === 0) {
    throw new Error(`step=native-snapshot-handle; recovery: inspect capture ${output}`);
  }
  await request(parent, "/file?path=/workspace/snapshot-probe", "PUT", "after\n");
  await request(child, "", "POST", { ...create, snapshot });
  const restored = await request(child, "/file?path=/workspace/snapshot-probe", "GET");
  const current = await request(parent, "/file?path=/workspace/snapshot-probe", "GET");
  if (!restored.equals(Buffer.from("before\n")) || !current.equals(Buffer.from("after\n"))) {
    throw new Error(`step=immutable-snapshot-restore; recovery: inspect capture ${output}`);
  }
} catch (error) {
  failure = String(error);
} finally {
  // Names are known before dispatch. Inspect uncertain creates, then stop any
  // observed running resource. An unknown reply never authorizes another create.
  for (const name of [child, parent]) {
    try {
      const state = JSON.parse((await request(name, "", "GET")).toString("utf8"));
      if (state.running) await request(name, "", "DELETE");
    } catch (error) { cleanupFailures.push(String(error)); }
  }
  const record = JSON.stringify({
    schema: "acyclic.machines.cloudflare-native-snapshot-capture.v1",
    source_commit: sourceCommit, namespace,
    provenance: "Actual HTTP responses from pinned Wrangler/workerd native local Containers on bb2; no account or credentials; request bodies are probe inputs.",
    provider_versions: { wrangler: "4.148.0", sandbox: "1.0.3" },
    snapshot_retention: snapshot === undefined ? null : "Native provider TTL; Worker API exposes no snapshot deletion. This capture does not claim physical snapshot retirement.",
    snapshot, failure, cleanup_failures: cleanupFailures, exchanges,
  }, null, 2) + "\n";
  // bb2 retires private job storage. Preserve the original bytes in its durable
  // log even when the build wrapper cannot copy an output file back.
  console.log(`CLOUDFLARE_SNAPSHOT_CAPTURE_BEGIN\n${record}CLOUDFLARE_SNAPSHOT_CAPTURE_END`);
  try { await writeFile(output, record, { flag: "wx" }); }
  catch (error) { throw new Error(`step=snapshot-capture-publication: ${error}; recovery: inspect the CLOUDFLARE_SNAPSHOT_CAPTURE_BEGIN record in the bb2 job log`); }
}
if (failure || cleanupFailures.length) {
  throw new Error(`step=native-snapshot-probe: ${failure ?? cleanupFailures.join("; ")}; recovery: inspect ${output} before rerunning`);
}
console.log(`Native immutable snapshot restored; actual response capture: ${output}`);
