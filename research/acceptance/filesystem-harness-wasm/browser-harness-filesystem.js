import {
  DEFAULT_OBJECT_CACHE_OPTIONS,
  openMemoryFs,
  portableVolumeOptions,
} from "@acyclic-labs/filesystem/memory";
import {
  MemoryConversation,
  NativeContracts,
  Harness,
} from "@acyclic-labs/harness";

const result = document.querySelector("#result");
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const toolOperationId = "11111111-1111-1111-1111-111111111111";
const toolCallId = "22222222-2222-2222-2222-222222222222";

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function waiting(label) {
  result.dataset.waiting = label;
}

function parseWriteInput(value) {
  if (value === null || typeof value !== "object" || typeof value.path !== "string" || value.path.length === 0) {
    throw new TypeError("write-file input is invalid");
  }
  return { path: value.path };
}

function parseWriteOutput(value) {
  if (value === null || typeof value !== "object" || typeof value.path !== "string" || !Number.isSafeInteger(value.bytes)) {
    throw new TypeError("write-file output is invalid");
  }
  return { path: value.path, bytes: value.bytes };
}

async function qualifyFilesystem() {
  waiting("Filesystem openMemoryFs");
  const filesystem = await openMemoryFs({
    maximumObjectBytes: 1024 * 1024,
    maximumMemoryBytes: 64 * 1024 * 1024,
    objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
  });
  try {
    waiting("Filesystem createVolume");
    const volume = await filesystem.createVolume(portableVolumeOptions("ephemeral"));
    waiting("Filesystem checkout");
    const checkout = await volume.checkout({
      access: "read-write",
      consistency: "tracking-safe",
      mutationMode: "private-cow",
    });
    const bytes = encoder.encode("filesystem wasm artifact");
    waiting("Filesystem createFile");
    await checkout.createFile("/browser-proof.txt", bytes);
    waiting("Filesystem readFileRange");
    const read = await checkout.readFileRange("/browser-proof.txt", 0n, BigInt(bytes.byteLength));
    assert(decoder.decode(read.bytes) === "filesystem wasm artifact", "Filesystem checkout readback differed");
  } finally {
    filesystem.close();
  }
}

async function qualifyHarness() {
  const agent = "01010101-0101-0101-0101-010101010101";
  const host = await MemoryConversation.create({
    agent,
    maxResidentBytes: 64 * 1024 * 1024,
    maxResidentFiles: 256,
  });
  try {
    waiting("Harness NativeContracts");
    const contracts = await NativeContracts.create();
    const tool = {
      name: "write-browser-file",
      revision: "v1",
      description: "Stage one file in the owning conversation store.",
      inputSchema: {
        type: "object",
        properties: { path: { type: "string", minLength: 1 } },
        required: ["path"],
        additionalProperties: false,
      },
      outputSchema: {
        type: "object",
        properties: { path: { type: "string" }, bytes: { type: "integer" } },
        required: ["path", "bytes"],
        additionalProperties: false,
      },
      parseInput: parseWriteInput,
      parseOutput: parseWriteOutput,
    };
    const executor = {
      async execute(invocation) {
        assert(invocation.operationId === toolOperationId, "Harness executor operation identity changed");
        const payload = encoder.encode("harness wasm tool payload");
        const file = await host.stage(invocation.arguments.path, payload, "text/plain", "tool-output.txt");
        return { value: { path: file.path, bytes: payload.byteLength } };
      },
      async reconcile() {
        return undefined;
      },
    };
    const runtime = Harness.builder(contracts)
      .content(host.contentBindings())
      .tool(tool, executor)
      .agentLoop({
        async run(context) {
          const write = await context.call(
            context.tool("write-browser-file"),
            { path: "tool/output.txt" },
            toolOperationId,
            toolCallId,
          );
          const stored = await context.readPrivatePath(host.volume, "", write.path);
          assert(decoder.decode(stored.bytes) === "harness wasm tool payload", "Harness private store readback differed");
          return { text: `stored ${write.bytes} bytes`, attachments: [] };
        },
      })
      .grant(...host.scope.capabilities, "tool:call:write-browser-file")
      .build();
    waiting("Harness runPrompt");
    const output = await host.runPrompt(runtime, "Store the browser acceptance payload.");
    waiting("Harness conversation readback");
    assert(output.text === "stored 25 bytes", "Harness agent output did not contain the tool result");
    const state = host.conversation();
    const stored = await host.readPrivatePath(host.volume, "", "tool/output.txt");
    assert(decoder.decode(stored.bytes) === "harness wasm tool payload", "Harness executor store write was not retained");
    const assistant = state.messages.find((message) => message.kind === "assistant");
    assert(assistant !== undefined, "Harness conversation omitted the assistant message");
    assert(decoder.decode(await host.read(assistant.content)) === output.text, "Harness assistant store readback differed");
  } finally {
    host.free();
  }
}

async function run() {
  await qualifyFilesystem();
  await qualifyHarness();
  result.dataset.status = "passed";
  result.dataset.waiting = "";
  result.textContent = "Installed Filesystem and Harness WASM APIs passed";
}

run().catch((error) => {
  result.dataset.status = "failed";
  result.dataset.waiting = "";
  const cause = error?.cause;
  result.textContent = `${error?.stack ?? error}${cause === undefined ? "" : `\nCause: ${cause?.stack ?? JSON.stringify(cause)}`}`;
});
