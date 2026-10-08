import { DEFAULT_LIMITS, Harness, NativeContracts, descriptorFor, projectModelFile, verifiedContentResolver } from "../dist/index.js";
import { WasmReducer } from "../generated/wasm/acyclic_harness_wasm.js";

function assert(value, message) { if (!value) throw new Error(message); }
async function rejects(action, message) {
  let rejected = false;
  try { await action(); } catch { rejected = true; }
  assert(rejected, message);
}

/** Consumer of public Harness and Filesystem APIs; retention uses filesystem pins. */
export async function exerciseDiscovery(engine, label, reopen) {
  const contracts = await NativeContracts.create();
  const workspace = await engine.createWorkspace(`discovery-${label}`);
  const provider = { namespace: "discovery", family: "filesystem", version: "2" };
  const volume = contracts.validate("volume_ref", { provider, id: workspace.name,
    class: "session_shared", owner: { kind: "session", id: label } });
  const core = new WasmReducer({ kind: "conversation", id: label }, label, new Uint8Array(32).fill(31), []);
  const encoder = new TextEncoder();
  const hex = bytes => Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
  const generationRef = generation => ({ kind: "generation", provider,
    key: Array.from(generation.id), version: workspace.name });
  const pinName = generation => `discovery-pin-${label}-${hex(generation.key ?? generation.id)}`;
  const root = { volume, directory: "" };
  const readerAgent = "09090909-0909-0909-0909-090909090909";
  const grant = core.directoryReadCapability(volume, "");
  const scope = core.issueScopeForAgent(readerAgent, "reader", [grant]);
  let prefixes = 0;
  let bodies = 0;
  async function retained(reference) {
    const pinned = await engine.openWorkspace(reference.version ?? workspace.name);
    const generation = await pinned.generation(Uint8Array.from(reference.key));
    assert(hex(generation.id) === hex(reference.key), "owner returned another generation");
    return generation;
  }
  function authorize(root, path) {
    assert(contracts.canonicalEqual(root.volume, volume), "owner received another volume");
    core.verifyPrivateDirectoryRead(scope, root.volume, root.directory, path);
  }
  const reader = {
    async list(query) {
      authorize(query.root, query.path);
      const generation = query.expected_generation ? await retained(query.expected_generation) : await workspace.sync();
      await generation.pin(pinName(generation));
      const after = query.after === null ? undefined : { encoding: "utf8", bytes: encoder.encode(query.after) };
      const page = await generation.listDirectory(`/${query.path}`, after, query.maximum_entries);
      return { generation: generationRef(generation), hasMore: page.hasMore,
        entries: page.entries.map(entry => {
          assert(entry.name.encoding === "utf8", "unsupported owner filename encoding");
          assert(entry.kind === "regular" || entry.kind === "directory", "unsupported owner entry kind");
          return { name: new TextDecoder("utf-8", { fatal: true }).decode(entry.name.bytes),
            kind: entry.kind === "regular" ? "file" : "directory" };
        }) };
    },
    async prefix(query) {
      authorize(query.source.root, query.source.path);
      prefixes++;
      const generation = await retained(query.source.generation);
      const stat = await generation.stat(`/${query.source.path}`);
      assert(stat.kind === "regular", "prefix requires regular file");
      return generation.readRange(`/${query.source.path}`, 0n,
        stat.logicalBytes < query.maximum_bytes ? stat.logicalBytes : query.maximum_bytes);
    },
    async read(query) {
      authorize(query.source.root, query.source.path);
      bodies++;
      const generation = await retained(query.source.generation);
      const bytes = await generation.read(`/${query.source.path}`, query.maximum_bytes);
      const file = contracts.validate("file_ref", { volume, path: query.source.path,
        version: hex(generation.id), descriptor: await descriptorFor(bytes, "text/plain"),
        display_name: query.source.path.split("/").at(-1) });
      return { file, bytes };
    },
  };
  try {
    const transaction = await workspace.beginTransaction();
    await transaction.createDirAll("/inspect");
    await transaction.createDirAll("/src");
    await transaction.write("/AGENTS.md", encoder.encode("root instruction"));
    await transaction.write("/src/AGENTS.md", encoder.encode("nested instruction"));
    await transaction.write("/inspect/SKILL.md", encoder.encode(
      "---\nname: inspect\ndescription: Inspect old files\nrank: 7\nexample: {sha256: note, byte_length: 7, media_type: note}\n---\nSECRET BODY" + "x".repeat(100_000)));
    await transaction.commit();
    const declaration = { instructions: [{ root, active_directory: "src" }], skills: [root],
      policy: { instruction_names: ["AGENTS.md"], skill_name: "SKILL.md" },
      limits: { entries: 64, directories: 8, header_bytes: 256, instruction_bytes: 512 } };
    const snapshot = await contracts.captureDiscoveredContext(declaration, reader);
    assert(snapshot.instructions.length === 2 && snapshot.skills.length === 1, "discovery omitted sources");
    assert(bodies === 2 && prefixes === 3, "discovery eagerly read skill body");
    assert(snapshot.skills[0].fields.rank === 7n, "frontmatter integer wire changed");
    const projected = contracts.projectDiscoveredContext(snapshot, { messages: [], metadata: {} }, "prepend", DEFAULT_LIMITS);
    assert(typeof projected.messages[0].content.file.descriptor.byte_length === "number", "file byte length is not admitted wire");
    let dispatches = 0;
    const content = {
      validate(file) { contracts.validate("file_ref", file); },
      verify(file, bytes) { contracts.verifyFileBytes(file, bytes); },
      async read(file) {
        core.verifyContentRead(core.issueScopeForAgent(readerAgent, "model-reader", [core.volumeCapability(volume, "read")]), file);
        return (await retained({ key: Array.from(file.version.match(/../g), byte => parseInt(byte, 16)) })).read(`/${file.path}`, 512n);
      },
      fileReadCapability: file => core.fileReadCapability(file),
      volumeReadCapability: value => core.volumeCapability(value, "read"),
      directoryReadCapability: (value, path) => core.directoryReadCapability(value, path),
    };
    const runtime = await Harness.builder(contracts).declaredContext(snapshot).content(content)
      .grant(core.volumeCapability(volume, "read")).model({ provider: "mock", name: "discovery", revision: "1", options: {} }, {
      async *generate(request) {
        dispatches++;
        const admitted = new TextDecoder().decode(request.serializedInput);
        for (const [index, expected] of ["root instruction", "nested instruction"].entries()) {
          const part = request.messages[index].content;
          assert(contracts.canonicalEqual(part.file, snapshot.instructions[index]), "prepared request changed pinned instruction ref");
          const projected = await projectModelFile(part, {
            resolveFile: verifiedContentResolver(content, DEFAULT_LIMITS), maxResolvedBytes: 512,
          });
          assert(projected.kind === "text" && projected.text === expected, "model file projection changed pinned instruction bytes");
        }
        assert(admitted.includes("Inspect old files") && !admitted.includes("SECRET BODY"), "model metadata/body selection changed");
        yield { kind: "completed", metadata: {} };
      }, async reconcile() { return undefined; },
    }).build();
    await workspace.write("/inspect/SKILL.md", encoder.encode("---\nname: inspect\ndescription: Inspect new files\n---\nNEW BODY"));
    await workspace.write("/AGENTS.md", encoder.encode("changed instruction"));
    const explicit = await contracts.contextForRequest(declaration, reader, snapshot, "explicit");
    assert(explicit.skills[0].description === "Inspect old files", "explicit policy reloaded sources");
    const fresh = await contracts.contextForRequest(declaration, reader, snapshot, "next_request");
    assert(fresh.skills[0].description === "Inspect new files", "next-request refresh stayed stale");
    await workspace.write("/inspect/SKILL.md", encoder.encode("---\nname: Invalid\ndescription: bad\n---\n"));
    await rejects(() => contracts.contextForRequest(declaration, reader, fresh, "next_request"), "invalid replacement accepted");
    const durableSnapshot = contracts.encodeCanonicalJson(snapshot);
    if (reopen) engine = await reopen(engine);
    await runtime.run("inspect");
    assert(dispatches === 1, "source reload altered model dispatch");
    const restored = contracts.decodeCanonicalJson(durableSnapshot);
    const reopenedWorkspace = await engine.openWorkspace(workspace.name);
    await rejects(() => reopenedWorkspace.generation(new Uint8Array(31)), "malformed generation accepted");
    await rejects(() => reopenedWorkspace.generation(new Uint8Array(32)), "missing generation accepted");
    const foreign = await engine.createWorkspace(`foreign-${label}`);
    const foreignGeneration = await foreign.sync();
    await rejects(() => reopenedWorkspace.generation(foreignGeneration.id), "foreign generation accepted");
    const body = await contracts.readPinnedContextPath(restored.skills[0].source, reader, 200_000);
    assert(new TextDecoder().decode(body.bytes).includes("SECRET BODY"), "old body did not survive retained restart value");
    await rejects(() => contracts.readPinnedContextPath(restored.skills[0].source, reader, 512), "oversize body accepted");
    await rejects(() => contracts.captureDiscoveredContext(declaration, {
      ...reader, async list(query) {
        core.verifyPrivateDirectoryRead(core.issueScopeForAgent(readerAgent, "denied", []), query.root.volume, query.root.directory, query.path);
        return reader.list(query);
      },
    }), "declarations granted directory authority");
    await rejects(() => contracts.captureDiscoveredContext(declaration, { ...reader,
      async list() { return { generation: snapshot.skills[0].source.generation,
        entries: [], hasMore: true }; },
    }), "nonadvancing page accepted");
  } finally { core.free(); }
}
