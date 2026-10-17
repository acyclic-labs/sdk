import { create, toBinary } from "@bufbuild/protobuf";
import { AppendRequestSchema } from "../../stream/generated/proto/stream/v1/stream_pb.js";
import { SessionLatestValueSchema } from "../../stream/generated/proto/stream/v1/session_pb.js";
import { SwarmClient, TaskVolume, WasmStream, initializeClientQueries } from "../src/client-query.js";

const encode = new TextEncoder(), decode = new TextDecoder();
const turn = 9007199254740993n;
const chunk = (sequence: bigint) => ({ Task: { task: "task", file: { Chunk: { turn, sequence } } } }) as const;
const text = { Task: { task: "task", file: { TextHead: { turn, channel: "Text" } } } } as const;
function check(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }
export const limits = { agents: 2n, selected: 1n, layout_bytes: 1024n, config_files: 2n, snapshot_files: 8n };
export const viewLimits = { agents: 2n, selected: 1n, overview_values: 2n, overview_bytes: 4096n, query: { files: 8n, segments: 16n, bytes: 4096n } };

export async function exerciseSwarm(stream: WasmStream, recover?: () => Promise<void>) {
  await initializeClientQueries();
  const configStream = new WasmStream();
  const config = await TaskVolume.create(configStream, "client/config");
  const parent = await TaskVolume.create(stream, "swarm/files");
  const a = await TaskVolume.create(stream, "swarm/a/files"), b = await TaskVolume.create(stream, "swarm/b/files");
  const put = async (volume: TaskVolume, sequence: bigint, value: string) => volume.write(await volume.head(), chunk(sequence), encode.encode(value), "Text");
  const layout = async (file: string, value: unknown) => config.write(await config.head(), { Input: `workspace/${file}.json` }, encode.encode(JSON.stringify(value)), null);
  const status = async (agent: string, attention: boolean) => {
    const value = toBinary(SessionLatestValueSchema, create(SessionLatestValueSchema, { key: encode.encode("status"), value: encode.encode("running"), attention }));
    await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, { path: `${agent}/status`, records: [value] })));
  };
  for (const reference of ["a", "b"]) {
    await parent.write(await parent.head(), { Child: { reference, file: "Status" } }, encode.encode(JSON.stringify({
      task: "01010101-0101-0101-0101-010101010101", state: "Running", settled: false, inputs: 1,
    })), null);
  }
  await a.write(await a.head(), { Task: { task: "task", file: { Notification: "settled" } } },
    encode.encode('{"task":"task","sequence":9007199254740993,"event":"needs_approval","detail":"approval requested"}'), null);
  await put(a, 1n, "before"); await put(b, 1n, "other");
  await status("swarm/a", false); await status("swarm/b", false);
  await layout("layout", { prefetch: ["swarm/a"] });
  const client = new SwarmClient("swarm", limits, viewLimits), views = client.views();
  const stop = Promise.withResolvers<void>();
  const seen = new Set<string>(), waiting = new Map<string, ReturnType<typeof Promise.withResolvers<void>>>();
  const notify = (name: string) => { seen.add(name); waiting.get(name)?.resolve(); };
  let snapshots = 0;
  const deltas: string[] = [];
  const segments = (agent: string) => views.segments(agent, text)?.map(bytes => decode.decode(bytes));
  const running = client.follow(config, parent, stream, change => {
    // A callback can read the same views without retaining a Rust borrow across JS.
    if (change.kind === "snapshot") { snapshots++; notify(`snapshot:${change.agent}`); }
    if (change.kind === "data" && !change.overview && change.file) {
      deltas.push(decode.decode(change.file.bytes)); notify("text");
    }
    if (change.kind === "data" && change.overview && views.status("swarm/b")?.values.some(value => value.attention)) notify("attention");
    if (change.kind === "layout") {
      const value = views.layout();
      if (value.in_view?.includes("swarm/a")) notify("visible");
      else if (value.prefetch?.includes("swarm/a")) notify("prefetch");
    }
  }, stop.promise);
  const ended = running.then(() => { throw new Error("swarm ended before its publication"); });
  const wait = async (name: string) => {
    if (seen.has(name)) return;
    const deferred = Promise.withResolvers<void>(); waiting.set(name, deferred);
    await Promise.race([deferred.promise, ended]);
  };
  try {
    await wait("snapshot:swarm/a");
    check(segments("swarm/a")?.join("") === "before", "snapshot bytes missing");
    const notice = views.notifications()[0];
    check(notice?.notification.kind === "present" && notice.notification.notification.sequence === turn,
      "notifications lost the followed file or its full-width identity");
    await status("swarm/b", true); await wait("attention");
    check(views.agents().join(",") === "swarm/a,swarm/b", "overview discovery incomplete");
    if (recover) await recover();
    await put(a, 2n, " live"); await wait("text");
    check(deltas.join("") === " live", "live delta duplicated or missing");
    const cursor = views.cursor("swarm/a")!;
    seen.delete("prefetch");
    await layout("open-views", { in_view: ["swarm/a"] }); await wait("visible");
    check(views.cursor("swarm/a")!.position === cursor.position, "promotion reset the cursor");
    check(segments("swarm/a")?.join("") === "before live", "promotion lost streaming text");
    check(views.files("swarm/a")[0]?.segments === 2n, "text was concatenated instead of segmented");
    await layout("open-views", {}); await wait("prefetch");
    await layout("layout", { in_view: ["swarm/b"] }); await wait("snapshot:swarm/b");
    check(views.cursor("swarm/a") === null && segments("swarm/b")?.join("") === "other", "tier switch retained or lost content");
    check(views.status("swarm/a") !== null && snapshots === 2, "promotion resubscribed or overview was removed");
    stop.resolve(); await running;
    return { agents: views.agents().length, snapshots, textDeltas: deltas.length, recovered: !!recover };
  } finally {
    stop.resolve(); await running.catch(() => {});
    views.free(); client.free(); a.free(); b.free(); parent.free(); config.free(); configStream.free();
  }
}
