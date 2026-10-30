// Capture a published MCP server's stdout; never manufacture response fixtures.
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, writeFile, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const recovery = "node capture-mcp.mjs /path/to/node_modules/@modelcontextprotocol/server-filesystem";
const digest = bytes => createHash("sha256").update(bytes).digest("hex");
let step = "read published server identity";

async function capture() {
  const source = process.argv[2];
  if (!source) throw new Error("provide the installed server package directory");
  const server = resolve(source);
  const identity = JSON.parse(await readFile(join(server, "package.json"), "utf8"));
  if (identity.name !== "@modelcontextprotocol/server-filesystem" || identity.version !== "2.1.0") {
    throw new Error("capture requires @modelcontextprotocol/server-filesystem@2.1.0");
  }
  const installed = JSON.parse(await readFile(join(server, "../../.package-lock.json"), "utf8"));
  const packages = Object.fromEntries(Object.entries(installed.packages).map(([name, value]) => [
    name.replace(/^node_modules\//, ""), { version: value.version, integrity: value.integrity },
  ]));
  const directory = await mkdtemp(join(tmpdir(), "mcp-capture-"));
  const initial = "06060606-0606-0606-0606-060606060606";
  const operation = "07070707-0707-0707-0707-070707070707";
  const protocol = "2025-11-25";
  const requests = [];
  const responses = [];
  const stdout = [];
  const stderr = [];
  const child = spawn(process.execPath, [join(server, "dist/index.js"), directory], {
    stdio: ["pipe", "pipe", "pipe"], windowsHide: true,
  });
  const closed = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  // A startup error must also terminate stdout consumption without a lost rejection.
  closed.catch(() => {});
  child.stderr.on("data", bytes => stderr.push(bytes));
  child.stdin.on("error", error => child.stdout.destroy(error));
  function send(value) {
    const bytes = Buffer.from(JSON.stringify(value) + "\n");
    requests.push(bytes);
    child.stdin.write(bytes);
  }
  try {
    step = "initialize published MCP server";
    send({ jsonrpc: "2.0", id: initial, method: "initialize", params: {
      protocolVersion: protocol, capabilities: {}, clientInfo: { name: "codec-capture", version: "1" },
    } });
    let pending = Buffer.alloc(0);
    for await (const bytes of child.stdout) {
      stdout.push(bytes);
      pending = Buffer.concat([pending, bytes]);
      for (let end; (end = pending.indexOf(10)) >= 0;) {
        const line = pending.subarray(0, end + 1);
        pending = pending.subarray(end + 1);
        const value = JSON.parse(line.toString("utf8"));
        if (value.jsonrpc !== "2.0" || value.error || !value.result) {
          throw new Error("server refused discovery or emitted an unexpected envelope");
        }
        if (responses.length === 0 && value.id === initial && value.result.protocolVersion === protocol) {
          responses.push(line);
          step = "discover published MCP tools";
          send({ jsonrpc: "2.0", method: "notifications/initialized" });
          send({ jsonrpc: "2.0", id: operation, method: "tools/list", params: {} });
        } else if (responses.length === 1 && value.id === operation && Array.isArray(value.result.tools)) {
          responses.push(line);
          child.stdin.end();
        } else {
          throw new Error("server emitted an unexpected response identity or order");
        }
      }
    }
    step = "observe server EOF";
    const result = await closed;
    if (result.code !== 0 || pending.length || responses.length !== 2) {
      throw new Error(`incomplete discovery: exit=${result.code}, signal=${result.signal}, responses=${responses.length}, trailing=${pending.length}; ${Buffer.concat(stderr)}`);
    }
    step = "save byte-exact external output";
    const transcript = Buffer.concat(stdout);
    const outputs = [
      ["mcp-filesystem-2.1.0-stdio", transcript],
      ["mcp-filesystem-2.1.0-result", responses[1]],
    ];
    await mkdir(join(root, "corpus/harness_codecs"), { recursive: true });
    let manifest = "# Captured external server output\n# path\tbytes\tsha256\tproducer\n";
    for (const [name, bytes] of outputs) {
      await writeFile(join(root, "corpus/harness_codecs", name), bytes);
      manifest += `harness_codecs/${name}\t${bytes.length}\t${digest(bytes)}\t@modelcontextprotocol/server-filesystem@2.1.0 stdio discovery; capture-mcp.mjs\n`;
    }
    await writeFile(join(root, "corpus/EXTERNAL.tsv"), manifest);
    await mkdir(join(root, "captures"), { recursive: true });
    await writeFile(join(root, "captures/mcp-filesystem-2.1.0.requests.jsonl"), Buffer.concat(requests));
    await writeFile(join(root, "captures/mcp-filesystem-2.1.0.json"), JSON.stringify({
      package: { name: identity.name, version: identity.version }, node: process.version,
      packages,
      protocol, transport: "stdio", directory: "empty temporary directory",
      requests_sha256: digest(Buffer.concat(requests)), stdout_sha256: digest(transcript),
      response_sha256: responses.map(digest), tools: JSON.parse(responses[1]).result.tools.length,
    }, null, 2) + "\n");
    console.log(`Captured ${outputs.length} inputs from the published server; review before admitting golden bytes.`);
  } finally {
    if (child.exitCode === null) child.kill();
    await closed;
    if (resolve(dirname(directory)) !== resolve(tmpdir())) {
      throw new Error("temporary directory escaped its allocated parent");
    }
    await rm(directory, { recursive: true });
  }
}

capture().catch(error => {
  console.error(`MCP capture failed at ${step}: ${error.message}; recovery: ${recovery}`);
  process.exitCode = 1;
});
