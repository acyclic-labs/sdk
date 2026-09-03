export async function exerciseWorkspace(engine) {
  const workspace = await engine.createWorkspace("main");
  const payload = Uint8Array.of(0, 255, 1, 0, 128);
  const commit = await workspace.write("/binary", payload);
  if (commit.status !== "committed") {
    throw new Error(`Node memory workspace did not commit: ${commit.status}`);
  }
  const generation = await workspace.head();
  if (!(generation instanceof Uint8Array) || generation.byteLength !== 32) {
    throw new Error("Node memory loader did not expose one canonical generation identity");
  }
  const observed = await workspace.read("/binary", BigInt(payload.byteLength));
  if (!payload.every((byte, index) => observed[index] === byte)) {
    throw new Error("Node memory workspace did not preserve binary bytes");
  }
  const exact = await workspace.sync();
  if (exact.id.byteLength !== 32 || exact.workspaceId.byteLength !== 16) {
    throw new Error("immutable generation identities have an invalid shape");
  }
  await workspace.checkpoint("before-output");
  await exact.pin("input-generation");
  const transaction = await workspace.beginTransaction(Uint8Array.from({ length: 16 }, (_, i) => i));
  await transaction.createDirAll("/output/nested");
  await transaction.copy("/binary", "/output/nested/copied");
  await transaction.rename("/output/nested/copied", "/output/result");
  await transaction.write("/output/status", new TextEncoder().encode("ready"));
  await transaction.createDirectory("/shapes");
  await transaction.write("/shapes/source", new TextEncoder().encode("abcdef"));
  await transaction.write("/shapes/destination", new TextEncoder().encode("......"));
  await transaction.createSymbolicLink("/shapes/symlink", new TextEncoder().encode("source"));
  await transaction.hardLink("/shapes/source", "/shapes/hard-link");
  await transaction.writeRange("/shapes/source", 1n, new TextEncoder().encode("Z"));
  await transaction.zeroRange("/shapes/source", 2n, 2n, false, false);
  await transaction.preallocate("/shapes/source", 0n, 6n, true);
  await transaction.cloneRange("/shapes/source", 0n, "/shapes/destination", 1n, 5n);
  await transaction.resize("/shapes/destination", 8n);
  const transactionCommit = await transaction.commit();
  if (transactionCommit.status !== "committed") {
    throw new Error(`atomic workspace transaction did not commit: ${transactionCommit.status}`);
  }
  if (new TextDecoder().decode(await workspace.read("/output/status", 5n)) !== "ready") {
    throw new Error("atomic workspace transaction did not publish complete state");
  }
  if (new TextDecoder().decode(await workspace.readRange("/output/status", 1n, 3n)) !== "ead") {
    throw new Error("bounded range read returned the wrong bytes");
  }
  const sourceStat = await workspace.stat("/shapes/source");
  const hardLinkStat = await workspace.stat("/shapes/hard-link");
  if (
    sourceStat.kind !== "regular"
    || sourceStat.linkCount !== 2n
    || sourceStat.logicalBytes !== 6n
    || !sourceStat.fileId.every((byte, index) => byte === hardLinkStat.fileId[index])
  ) {
    throw new Error("workspace stat lost hard-link identity or exact size");
  }
  const firstDirectoryPage = await workspace.listDirectory("/shapes", undefined, 1);
  const remainingDirectoryPage = await workspace.listDirectory(
    "/shapes",
    firstDirectoryPage.entries[0].name,
    16,
  );
  if (!firstDirectoryPage.hasMore || remainingDirectoryPage.entries.length !== 3) {
    throw new Error("bounded directory cursor did not cover each child exactly once");
  }
  if (new TextDecoder().decode(await workspace.readSymbolicLink("/shapes/symlink")) !== "source") {
    throw new Error("symbolic-link target bytes changed at the SDK boundary");
  }
  const extentPlan = await workspace.planExtents("/shapes/source", 0n, 6n, 8);
  if (
    !extentPlan.spans.some((span) => span.kind === "content")
    || !extentPlan.spans.some((span) => span.kind === "allocated-zero")
  ) {
    throw new Error("topology-free extent plan lost content or represented zeros");
  }
  const firstRacer = await workspace.beginTransaction(new Uint8Array(16).fill(21));
  const disjointRacer = await workspace.beginTransaction(new Uint8Array(16).fill(22));
  await firstRacer.write("/race-a", Uint8Array.of(1));
  await disjointRacer.write("/race-b", Uint8Array.of(2));
  if ((await firstRacer.commit()).status !== "committed") {
    throw new Error("first transaction race did not publish");
  }
  if ((await disjointRacer.commit()).status !== "conflict") {
    throw new Error("stale disjoint transaction did not retain its candidate");
  }
  const safeRebase = await disjointRacer.rebase(16);
  if (safeRebase.status !== "rebased" || safeRebase.conflicts.length !== 0) {
    throw new Error("disjoint transaction did not rebase safely");
  }
  if ((await disjointRacer.commit()).status !== "committed") {
    throw new Error("rebased transaction did not publish with its original identity");
  }
  const overlapWinner = await workspace.beginTransaction(new Uint8Array(16).fill(23));
  const overlapLoser = await workspace.beginTransaction(new Uint8Array(16).fill(24));
  await overlapWinner.write("/race-a", Uint8Array.of(3));
  await overlapLoser.write("/race-a", Uint8Array.of(4));
  if ((await overlapWinner.commit()).status !== "committed") {
    throw new Error("overlap winner did not publish");
  }
  if ((await overlapLoser.commit()).status !== "conflict") {
    throw new Error("overlap loser did not retain its candidate");
  }
  const unsafeRebase = await overlapLoser.rebase(16);
  if (
    unsafeRebase.status !== "conflicted"
    || unsafeRebase.conflicts.length === 0
    || unsafeRebase.truncated
  ) {
    throw new Error("overlapping transaction crossed an exact dependency");
  }
  const sparse = Uint8Array.of(97, 90, 0, 0, 101, 102);
  for (const path of ["/shapes/source", "/shapes/hard-link"]) {
    const bytes = await workspace.read(path, 8n);
    if (!sparse.every((byte, index) => bytes[index] === byte)) {
      throw new Error(`arbitrary-shape transaction lost sparse hard-link bytes at ${path}`);
    }
  }
  const cloned = await workspace.read("/shapes/destination", 8n);
  const expectedClone = Uint8Array.of(46, 97, 90, 0, 0, 101, 0, 0);
  if (!expectedClone.every((byte, index) => cloned[index] === byte)) {
    throw new Error("arbitrary-shape transaction did not preserve COW clone bytes");
  }
  try {
    await exact.read("/output/status", 5n);
    throw new Error("immutable generation moved after workspace publication");
  } catch (error) {
    if (error instanceof Error && error.message === "immutable generation moved after workspace publication") {
      throw error;
    }
  }
  const exactFork = await workspace.forkAt("exact-fork", exact);
  try {
    await exactFork.read("/output/status", 5n);
    throw new Error("exact fork selected a newer generation");
  } catch (error) {
    if (error instanceof Error && error.message === "exact fork selected a newer generation") {
      throw error;
    }
  }
  const fork = await workspace.fork("fork");
  if (fork.id.every((byte, index) => byte === workspace.id[index])) {
    throw new Error("workspace fork reused the source identity");
  }
  const forked = await fork.read("/binary", BigInt(payload.byteLength));
  if (!payload.every((byte, index) => forked[index] === byte)) {
    throw new Error("workspace fork did not preserve the exact filesystem state");
  }
  const branchBase = await fork.sync();
  await fork.write("/branch-first", Uint8Array.of(7));
  const branchMiddle = await fork.sync();
  await fork.write("/branch-second", Uint8Array.of(8));
  const branchEnd = await fork.sync();
  const firstChange = await fork.diff(branchBase, branchMiddle, 32);
  const secondChange = await fork.diff(branchMiddle, branchEnd, 32);
  const composed = await firstChange.compose(secondChange, 32);
  if (
    !composed.from.id.every((byte, index) => byte === branchBase.id[index])
    || !composed.to.id.every((byte, index) => byte === branchEnd.id[index])
    || composed.changes().files.length === 0
  ) {
    throw new Error("workspace change-set composition lost its immutable endpoints");
  }
  await workspace.write("/upstream-rebase", Uint8Array.of(9));
  const workspaceRebase = await fork.liveRebase(
    { maximumGenerations: 64, maximumChanges: 64, maximumConflicts: 16 },
    new Uint8Array(16).fill(77),
  );
  if (workspaceRebase.status !== "rebased" || workspaceRebase.conflicts.length !== 0) {
    throw new Error(`workspace live rebase failed: ${workspaceRebase.status}`);
  }
  if ((await fork.read("/upstream-rebase", 1n))[0] !== 9) {
    throw new Error("workspace live rebase omitted upstream state");
  }
  if ((await fork.read("/branch-second", 1n))[0] !== 8) {
    throw new Error("workspace live rebase omitted local state");
  }
  const join = await fork.joinInto(workspace, {
    history: "merge",
    maximumGenerations: 64,
    maximumChanges: 64,
    maximumConflicts: 16,
  });
  const joined = await join.apply(join.targetHead, new Uint8Array(16).fill(91));
  if (joined.status !== "applied") {
    throw new Error(`workspace join did not publish atomically: ${joined.status}`);
  }
  if ((await workspace.read("/branch-second", 1n))[0] !== 8) {
    throw new Error("workspace join omitted branch state");
  }
  const disposable = await engine.createWorkspace("disposable");
  if (await disposable.delete(Uint8Array.from({ length: 16 }, () => 42)) !== "deleted") {
    throw new Error("workspace deletion did not become durable");
  }
}
