import {
  CrossVolumeError,
  DEFAULT_OBJECT_CACHE_OPTIONS,
  MountedView,
  openBrowserFs,
  portableVolumeOptions,
} from "../dist/browser.js";

const result = document.querySelector("#result");

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function run() {
  const suffix = `${Date.now()}-${crypto.randomUUID()}`;
  const source = await openBrowserFs({
    databaseName: `acyclic-fs-smoke-source-${suffix}`,
    maximumObjectBytes: 64 * 1024 * 1024,
    objectAcceleration: "opfs-if-available",
    objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
  });
  const volume = await source.createVolume(portableVolumeOptions("durable"));
  const populatedCache = source.objectCacheStats();
  assert(populatedCache.residentEntries > 0n, "browser object cache did not retain authenticated objects");
  source.clearObjectCache();
  const clearedCache = source.objectCacheStats();
  assert(clearedCache.residentEntries === 0n && clearedCache.residentBytes === 0n, "browser object cache clear retained disposable state");
  const volumeId = volume.id.slice();
  const selectedVolumeId = new Uint8Array(16).fill(41);
  const liveOptions = {
    ...portableVolumeOptions("durable"),
    concurrency: "serialized-authority",
  };
  const selectedVolume = await source.createVolumeWithId(selectedVolumeId, liveOptions);
  assert(
    selectedVolume.id.every((value, index) => value === selectedVolumeId[index]),
    "caller-selected volume identity was not preserved",
  );
  const selectedRetry = await source.createVolumeWithId(selectedVolumeId, liveOptions);
  assert(
    selectedRetry.id.every((value, index) => value === selectedVolumeId[index]),
    "caller-selected volume creation was not idempotent",
  );
  const liveCheckout = await selectedVolume.checkout({
    access: "read-write",
    consistency: "live",
    mutationMode: "direct-live",
  });
  const liveTransaction = await liveCheckout.mutateLive(
    [
      { kind: "create-file", path: "/live.txt", bytes: new TextEncoder().encode("live") },
      { kind: "write", path: "/live.txt", offset: 4n, bytes: new TextEncoder().encode("-published") },
    ],
    new Uint8Array(16).fill(42),
    3,
    8,
  );
  assert(liveTransaction.status === "committed", "authored live transaction did not publish");
  assert(
    liveTransaction.createdFileIds.length === 2
      && liveTransaction.createdFileIds[0]?.byteLength === 16
      && liveTransaction.createdFileIds[1] === undefined,
    "authored live transaction lost stable create result positions",
  );
  assert(
    new TextDecoder().decode((await liveCheckout.readFileRange("/live.txt", 0n, 14n)).bytes)
      === "live-published",
    "authored live transaction was not atomically visible",
  );
  const checkout = await volume.checkout({
    access: "read-write",
    consistency: "tracking-safe",
    mutationMode: "private-cow",
  });
  const encoder = new TextEncoder();
  await checkout.createDirectory("/workspace");
  const transaction = await checkout.applyTransaction([
    { kind: "create-directory", path: "/workspace/batch" },
    { kind: "create-file", path: "/workspace/batch/a", bytes: encoder.encode("a") },
    { kind: "write", path: "/workspace/batch/a", offset: 1n, bytes: encoder.encode("b") },
    { kind: "hard-link", source: "/workspace/batch/a", destination: "/workspace/batch/b" },
  ]);
  assert(transaction.createdFileIds.length === 4 && transaction.createdFileIds[0]?.byteLength === 16 && transaction.createdFileIds[1]?.byteLength === 16, "atomic transaction result positions were incorrect");
  assert(new TextDecoder().decode((await checkout.readFileRange("/workspace/batch/b", 0n, 2n)).bytes) === "ab", "atomic transaction did not observe candidate state");
  const batchFileId = transaction.createdFileIds[1];
  if (batchFileId === undefined) throw new Error("batch file identity was absent");
  const batchMetadata = await checkout.readMetadata("/workspace/batch/a");
  await checkout.setAttributes("/workspace/batch/a", batchMetadata.canonicalBytes, 4n);
  await checkout.setAttributesById(batchFileId, batchMetadata.canonicalBytes, 3n);
  const resizedBatch = await checkout.readFileRange("/workspace/batch/b", 0n, 3n);
  assert(
    resizedBatch.bytes[0] === 97 && resizedBatch.bytes[1] === 98 && resizedBatch.bytes[2] === 0,
    "atomic metadata and logical-size replacement lost hard-link or sparse semantics",
  );
  const batchLookup = await checkout.lookupBatchNoFollow([
    "/workspace/batch/a",
    "/workspace/batch/missing",
    "/workspace/batch/a",
  ]);
  assert(batchLookup.entries.length === 3, "batch lookup did not preserve cardinality");
  assert(batchLookup.entries[0].exists && !batchLookup.entries[1].exists, "batch lookup existence was incorrect");
  assert(
    batchLookup.entries[0].fileId?.every((byte, index) => byte === batchLookup.entries[2].fileId?.[index]),
    "batch lookup did not preserve duplicate identity and order",
  );
  assert(batchLookup.retainedAllocationBytes > 0n, "batch lookup omitted retained-allocation evidence");
  const recordRead = await checkout.readFileRecordById(batchFileId);
  assert(
    recordRead.record.fileKind === "regular" && recordRead.record.logicalBytes === 3n,
    `identity record read was incorrect: ${recordRead.record.payloadKind}/${typeof recordRead.record.logicalBytes}/${String(recordRead.record.logicalBytes)}`,
  );
  recordRead.record.fileId.fill(0);
  recordRead.record.metadataObject.fill(0);
  recordRead.record.payloadObject?.fill(0);
  recordRead.record.inlineBytes?.fill(0);
  const isolatedRecordRead = await checkout.readFileRecordById(batchFileId);
  assert(
    isolatedRecordRead.record.fileId.some((byte) => byte !== 0)
      && isolatedRecordRead.record.metadataObject.some((byte) => byte !== 0)
      && (isolatedRecordRead.record.payloadObject?.some((byte) => byte !== 0)
        ?? isolatedRecordRead.record.inlineBytes?.some((byte) => byte !== 0)
        ?? false),
    "record result buffers aliased canonical checkout state",
  );
  const recordPage = await checkout.listDirectoryRecords("/workspace/batch", undefined, 16);
  assert(recordPage.entries.length === 2 && !recordPage.hasMore, "directory record page was incomplete");
  assert(
    recordPage.entries[0].record.fileId.every((byte, index) => byte === recordPage.entries[1].record.fileId[index]),
    "directory record page lost hard-link identity",
  );
  assert(recordPage.entries.every((entry) => entry.metadataCanonicalBytes.byteLength > 0), "directory record page omitted metadata");
  const wideLogicalBytes = 2n ** 53n + 1n;
  const wideOptions = portableVolumeOptions("ephemeral");
  const wideVolume = await source.createVolume({
    ...wideOptions,
    limits: {
      ...wideOptions.limits,
      maximumGenerationBytes: wideLogicalBytes + 1024n,
    },
  });
  const wideCheckout = await wideVolume.checkout({
    access: "read-write",
    consistency: "pinned",
    mutationMode: "private-cow",
  });
  const wideBefore = await wideCheckout.checkpoint();
  const wideCreate = await wideCheckout.createFile("/wide", new Uint8Array());
  if (wideCreate.fileId === undefined) throw new Error("wide sparse file identity was absent");
  await wideCheckout.resizeFileById(wideCreate.fileId, wideLogicalBytes);
  const wideRecord = await wideCheckout.readFileRecordById(wideCreate.fileId);
  assert(wideRecord.record.logicalBytes === wideLogicalBytes, "identity record lost exact 64-bit logical size");
  const wideAfter = await wideCheckout.checkpoint();
  const wideDiff = await wideVolume.diffGenerations(wideBefore.generationId, wideAfter.generationId, 8);
  assert(wideDiff.files[0]?.after?.logicalBytes === wideLogicalBytes, "generation diff lost exact 64-bit logical size");
  await checkout.createSpecial("/workspace/nested-volume", "mount-boundary");
  assert((await checkout.lookupNoFollow("/workspace/nested-volume")).fileKind === "mount-boundary", "special mount boundary was not preserved");
  const createdData = await checkout.createFile("/workspace/data.bin", encoder.encode("head"));
  assert(createdData.fileId?.byteLength === 16, "created file identity was malformed");
  const dataFileId = createdData.fileId;
  if (dataFileId === undefined) throw new Error("created file identity was absent");
  await checkout.writeFileById(dataFileId, 0n, encoder.encode("HEAD"));
  await checkout.resizeFile("/workspace/data.bin", 1024n * 1024n);
  await checkout.writeFile("/workspace/data.bin", 1024n * 1024n - 4n, encoder.encode("tail"));
  const extentPlan = await checkout.planFileExtents("/workspace/data.bin", 0n, 1024n * 1024n, 8);
  assert(extentPlan.kind === "sparse" && extentPlan.spans.length === 3, "sparse extent plan was incomplete");
  assert(extentPlan.kind === "sparse" && extentPlan.spans[0].kind === "content" && extentPlan.spans[1].kind === "hole" && extentPlan.spans[2].kind === "content", "sparse extent classes were incorrect");
  assert(extentPlan.kind === "sparse" && extentPlan.spans[1].offset === 4n && extentPlan.spans[1].sourceEnd === 1024n * 1024n - 4n, "sparse extent bounds were incorrect");
  const identityPlan = await checkout.planFileExtentsById(dataFileId, 0n, 1024n * 1024n, 8);
  assert(identityPlan.kind === "sparse" && identityPlan.spans.length === 3, "identity sparse plan was incorrect");
  assert(new TextDecoder().decode((await checkout.readFileRangeById(dataFileId, 0n, 4n)).bytes) === "HEAD", "identity read was incorrect");
  assert((await checkout.seekFileExtentById(dataFileId, 0n, "hole")).offset === 4n, "identity sparse seek was incorrect");
  assert((await checkout.seekFileExtent("/workspace/data.bin", 0n, "hole")).offset === 4n, "sparse hole seek was incorrect");
  assert((await checkout.seekFileExtent("/workspace/data.bin", 4n, "data")).offset === 1024n * 1024n - 4n, "sparse data seek was incorrect");
  await checkout.hardLink("/workspace/data.bin", "/workspace/alias.bin");
  await checkout.writeNamedAttribute(
    "/workspace/data.bin",
    "posix-xattr",
    encoder.encode("user.browser-smoke"),
    encoder.encode("present"),
    "create",
  );
  const scratch = await source.createVolume(portableVolumeOptions("ephemeral"));
  const scratchCheckout = await scratch.checkout({
    access: "read-write",
    consistency: "pinned",
    mutationMode: "private-cow",
  });
  const mounted = new MountedView([
    { mountId: new Uint8Array(16).fill(1), volumeId: volume.id, path: "/", checkout },
    { mountId: new Uint8Array(16).fill(2), volumeId: scratch.id, path: "/.scratch", checkout: scratchCheckout },
  ]);
  await mounted.createFile("/.scratch/temp.txt", encoder.encode("scratch"));
  assert((await scratchCheckout.lookupNoFollow("/temp.txt")).fileId !== undefined, "nested mount did not route to its volume");
  let crossVolumeRejected = false;
  try {
    await mounted.rename("/workspace/data.bin", "/.scratch/data.bin", false);
  } catch (error) {
    crossVolumeRejected = error instanceof CrossVolumeError && error.code === "EXDEV";
  }
  assert(crossVolumeRejected, "cross-volume rename did not fail as EXDEV");
  const snapshot = await mounted.checkpointSnapshot();
  assert(snapshot.mounts.length === 2, "mounted snapshot did not capture every volume");
  assert(snapshot.mounts.every((mount) => mount.generationId.byteLength === 32), "mounted snapshot identity was malformed");
  assert(snapshot.mounts.every((mount) => mount.volumeId.byteLength === 16), "mounted volume identity was malformed");
  const checkpoint = await checkout.checkpoint();
  assert(checkpoint.generationId.byteLength === 32, "browser checkpoint identity was malformed");
  const windowsVolume = await source.createVolume({
    ...portableVolumeOptions("ephemeral"),
    profile: "windows",
  });
  const windowsCheckout = await windowsVolume.checkout({
    access: "read-write",
    consistency: "pinned",
    mutationMode: "private-cow",
  });
  const reparsePayload = encoder.encode("opaque-reparse");
  await windowsCheckout.createReparsePoint("/junction", reparsePayload);
  assert(new TextDecoder().decode((await windowsCheckout.readReparsePoint("/junction")).bytes) === "opaque-reparse", "reparse payload was not exact");
  const reparseStat = await windowsCheckout.statNoFollow("/junction");
  assert(reparseStat.record?.fileKind === "reparse-point" && reparseStat.record.payloadKind === "reparse-point", "structured stat lost reparse identity");
  assert(reparseStat.metadataCanonicalBytes?.byteLength > 0, "structured stat omitted canonical metadata");
  const committed = await checkout.commit(new Uint8Array(16).fill(17));
  assert(committed.status === "committed", "browser commit did not publish");
  const manualCheckout = await volume.checkout({
    access: "read-only",
    consistency: "manual",
    mutationMode: "none",
  });
  await checkout.createFile("/workspace/refresh.txt", encoder.encode("fresh"));
  const advanced = await checkout.commit(new Uint8Array(16).fill(18));
  assert(advanced.status === "committed", "browser refresh fixture did not publish");
  assert(!(await manualCheckout.lookupNoFollow("/workspace/refresh.txt")).exists, "manual checkout advanced implicitly");
  await manualCheckout.refreshHead();
  assert((await manualCheckout.lookupNoFollow("/workspace/refresh.txt")).exists, "manual refresh did not advance");

  const manifest = await checkout.exportManifest();
  assert(manifest.objects.length > 0, "browser export had no closure objects");
  source.close();

  const reopened = await openBrowserFs({
    databaseName: `acyclic-fs-smoke-source-${suffix}`,
    maximumObjectBytes: 64 * 1024 * 1024,
    objectAcceleration: "opfs-if-available",
    objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
  });
  assert(reopened.capabilities.nativeWatch === false, "browser reported native watcher support");
  const reopenedVolume = await reopened.openVolume(volumeId);
  const reopenedCheckout = await reopenedVolume.checkout({
    access: "read-only",
    consistency: "pinned",
    mutationMode: "none",
  });
  const head = await reopenedCheckout.readFileRange("/workspace/alias.bin", 0n, 4n);
  const tail = await reopenedCheckout.readFileRange(
    "/workspace/data.bin",
    1024n * 1024n - 4n,
    4n,
  );
  assert(new TextDecoder().decode(head.bytes) === "HEAD", "reopened hard link lost content");
  assert(new TextDecoder().decode(tail.bytes) === "tail", "reopened sparse tail was incorrect");
  const attribute = await reopenedCheckout.readNamedAttribute(
    "/workspace/data.bin",
    "posix-xattr",
    encoder.encode("user.browser-smoke"),
  );
  assert(
    attribute.bytes !== undefined && new TextDecoder().decode(attribute.bytes) === "present",
    "reopened named attribute was incorrect",
  );

  const destination = await openBrowserFs({
    databaseName: `acyclic-fs-smoke-destination-${suffix}`,
    maximumObjectBytes: 64 * 1024 * 1024,
    objectAcceleration: "indexeddb",
    objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
  });
  for (const objectId of manifest.objects) {
    const object = await reopened.exportObject(objectId, 64n * 1024n * 1024n);
    await destination.importObject(objectId, object.bytes);
  }
  const restored = await destination.restoreVolume(manifest);
  const restoredCheckout = await restored.checkout({
    access: "read-only",
    consistency: "pinned",
    mutationMode: "none",
  });
  const restoredTail = await restoredCheckout.readFileRange(
    "/workspace/data.bin",
    1024n * 1024n - 4n,
    4n,
  );
  assert(new TextDecoder().decode(restoredTail.bytes) === "tail", "restored closure was incorrect");
  reopened.close();
  destination.close();

  result.dataset.status = "passed";
  result.textContent = JSON.stringify({
    status: "passed",
    sourceAuthority: reopened.capabilities.authority,
    sourceObjects: reopened.capabilities.immutableObjects,
    exportedObjects: manifest.objects.length,
  });
}

run().catch((error) => {
  result.dataset.status = "failed";
  result.textContent = error instanceof Error ? error.stack ?? error.message : String(error);
});
