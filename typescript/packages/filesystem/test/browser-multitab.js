import { DEFAULT_OBJECT_CACHE_OPTIONS, portableVolumeOptions } from "../dist/contracts.js";
import initialize, { openBrowserFs } from "../generated/wasm/acyclic_fs_wasm.js";
import { openBrowserFs as openWorkspaceFs } from "../dist/browser.js";
import { exerciseWorkspace } from "./workspace-composition.mjs";

await initialize();

const result = document.querySelector("#result");
const query = new URLSearchParams(location.search);
const runId = query.get("run");
const actor = query.get("actor");
const databaseName = query.get("database");
const workspaceProfiles = ["indexeddb", "opfs-required"];
const volumeIdentity = new Uint8Array(16).fill(71);
const checkoutOptions = {
  access: "read-write",
  consistency: "pinned",
  mutationMode: "private-cow",
};
const workFields = [
  "authorityRecordsRead",
  "authorityRecordsAppended",
  "authorityBytesRead",
  "authorityBytesWritten",
  "objectProbes",
  "backendReadOperations",
  "backendWriteOperations",
  "durabilityOperations",
  "pageReads",
  "pageWrites",
  "objectBytesRead",
  "objectBytesWritten",
  "bytesHashed",
  "bytesCopied",
  "bytesEncoded",
  "sourceBytesRead",
  "outputBytes",
  "itemsExamined",
  "itemsReturned",
  "allocationOperations",
  "peakAllocationBytes",
  "materializations",
];

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function errorText(error) {
  return error instanceof Error ? error.stack ?? error.message : String(error);
}

function databaseOptions(name) {
  return {
    databaseName: name,
    maximumObjectBytes: 64 * 1024 * 1024,
    objectAcceleration: "opfs-required",
    objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
  };
}

function post(channel, value) {
  channel.postMessage({ actor, ...value });
}

function emptyAccounting() {
  return {
    operations: 0,
    work: Object.fromEntries(workFields.map((field) => [field, 0])),
  };
}

function record(accounting, receipt) {
  recordWork(accounting, receipt.work);
  return receipt;
}

function recordWork(accounting, work) {
  accounting.operations += 1;
  for (const field of workFields) {
    const value = Number(work[field]);
    assert(Number.isSafeInteger(value) && value >= 0, `invalid work counter ${field}`);
    if (field === "peakAllocationBytes") {
      accounting.work[field] = Math.max(accounting.work[field], value);
    } else {
      const sum = accounting.work[field] + value;
      assert(Number.isSafeInteger(sum), `work counter ${field} overflowed`);
      accounting.work[field] = sum;
    }
  }
}

function recordControl(accounting, value) {
  accounting.operations += 1;
  assert(Number.isSafeInteger(accounting.operations), "operation count overflowed");
  return value;
}

function speculationOptions() {
  return {
    residency: {
      maximumActiveOperations: 2,
      maximumActiveBytes: 1024n * 1024n,
      outcomeWindow: 8,
      trafficWindow: 8,
      speculativeCostBasisPoints: 10_000,
      minimumUsefulnessSamples: 2,
      minimumUsefulnessBasisPoints: 1,
    },
    promotion: {
      maximumActiveOperations: 2,
      maximumActiveBytes: 1024n * 1024n,
      maximumActiveCostUnits: 1024n * 1024n,
      maximumResidencyFacts: 4,
      maximumDestinations: 4,
      maximumAcceptedTiers: 4,
      outcomeWindow: 8,
      minimumUsefulnessSamples: 2,
      minimumUsefulnessBasisPoints: 1,
    },
  };
}

async function exerciseSpeculation(fs, volume, checkout, operationSequence, accounting) {
  if (operationSequence === "create_write_read") {
    return { status: "not-applicable", verifiedBytes: 0 };
  }
  assert(
    operationSequence === "speculative_residency" || operationSequence === "speculative_promotion",
    `unsupported targeted browser operation sequence ${operationSequence}`,
  );
  const checkpoint = record(accounting, await checkout.checkpoint());
  const speculation = recordControl(
    accounting,
    fs.createSpeculation(volume.id, checkpoint.generationId, speculationOptions()),
  );
  const generationRoot = new Uint8Array(33);
  generationRoot[0] = 6;
  generationRoot.set(checkpoint.generationId, 1);
  const operationId = new Uint8Array(16).fill(
    operationSequence === "speculative_residency" ? 141 : 142,
  );
  const admission = recordControl(accounting, await speculation.observe({
    operationId,
    volumeId: volume.id,
    generationId: checkpoint.generationId,
    foregroundBytes: 1024n * 1024n,
    objectId: generationRoot,
    maximumBytes: 1024n * 1024n,
    reason: "consumer-hint",
  }));
  assert(admission.status === "admitted", "targeted browser residency was not admitted");
  const execution = record(accounting, await speculation.executeResidency(operationId));
  if (operationSequence === "speculative_promotion") {
    const promotion = recordControl(accounting, await speculation.planPromotion({
      operationId,
      acceptedTiers: ["node-local"],
      residency: [{
        objectId: generationRoot,
        locationId: new Uint8Array(16).fill(143),
        tier: "durable-origin",
        sourcePriority: 0,
      }],
      destinations: [{
        locationId: new Uint8Array(16).fill(144),
        tier: "node-local",
        writable: true,
        maximumObjectBytes: 1024n * 1024n,
        priority: 0,
        costUnitsPerByte: 1n,
      }],
    }));
    assert(promotion.status === "planned", "targeted browser promotion was not planned");
    recordControl(accounting, await speculation.finishPromotion(operationId, true));
  }
  recordControl(accounting, await speculation.finishResidency(operationId, true));
  const metrics = recordControl(accounting, await speculation.metrics());
  assert(metrics.residency.useful === "1", "targeted browser residency usefulness diverged");
  if (operationSequence === "speculative_promotion") {
    assert(metrics.promotion.useful === "1", "targeted browser promotion usefulness diverged");
  } else {
    assert(metrics.promotion.useful === "0", "residency-only case mutated promotion outcomes");
  }
  const objectBytes = Number(execution.objectBytes);
  assert(Number.isSafeInteger(objectBytes) && objectBytes > 0, "speculative object byte count is invalid");
  return {
    status: operationSequence === "speculative_promotion" ? "promotion-useful" : "residency-useful",
    verifiedBytes: objectBytes,
  };
}

function mergeAccounting(target, source) {
  target.operations += source.operations;
  assert(Number.isSafeInteger(target.operations), "operation count overflowed");
  for (const field of workFields) {
    if (field === "peakAllocationBytes") {
      target.work[field] = Math.max(target.work[field], source.work[field]);
    } else {
      const sum = target.work[field] + source.work[field];
      assert(Number.isSafeInteger(sum), `work counter ${field} overflowed`);
      target.work[field] = sum;
    }
  }
}

async function runActor() {
  assert(runId !== null && actor !== null && databaseName !== null, "actor URL is incomplete");
  const channel = new BroadcastChannel(`acyclic-fs-multitab-${runId}`);
  let fs;
  let checkout;
  let authoredPath;
  let targetedAccounting;
  let targetedVolume;
  channel.addEventListener("message", async (event) => {
    const message = event.data;
    if (message.target !== actor && message.target !== "all") return;
    try {
      if (message.command === "prepare") {
        fs = await openBrowserFs(databaseOptions(databaseName));
        assert(fs.capabilities.immutableObjects === "indexeddb-opfs", "OPFS acceleration was not selected");
        const volume = await fs.openVolume(volumeIdentity);
        checkout = await volume.checkout(checkoutOptions);
        authoredPath = `/${actor}.txt`;
        await checkout.createFile(authoredPath, new TextEncoder().encode(actor));
        post(channel, {
          event: "prepared",
          immutableObjects: fs.capabilities.immutableObjects,
        });
        return;
      }
      if (message.command === "targeted-prepare") {
        fs = await openBrowserFs(databaseOptions(message.database));
        assert(fs.capabilities.immutableObjects === "indexeddb-opfs", "targeted actor lost OPFS acceleration");
        const volume = await fs.openVolume(new Uint8Array(message.volumeIdentity));
        targetedVolume = volume;
        targetedAccounting = emptyAccounting();
        recordWork(targetedAccounting, volume.acquisitionWork);
        checkout = await volume.checkout({
          access: "read-only",
          consistency: message.consistency,
          mutationMode: "none",
        });
        recordWork(targetedAccounting, checkout.acquisitionWork);
        post(channel, { event: "targeted-prepared", consistency: message.consistency });
        return;
      }
      if (message.command === "targeted-observe") {
        assert(checkout !== undefined, "targeted observation arrived before preparation");
        assert(targetedAccounting !== undefined, "targeted acquisition accounting is absent");
        const accounting = targetedAccounting;
        let transition;
        if (message.consistency === "pinned") {
          let rejected = false;
          try {
            await checkout.refreshHead();
          } catch (error) {
            rejected = true;
            assert(errorText(error).toLowerCase().includes("refresh"), "pinned refresh returned an unrelated failure");
          }
          assert(rejected, "pinned targeted actor refreshed");
          transition = "pinned-old";
        } else if (message.consistency === "tracking-safe") {
          const rebase = record(accounting, await checkout.rebaseHead(8));
          assert(rebase.status === "safe", "tracking targeted actor did not safely rebase");
          transition = "tracking-new";
        } else if (message.consistency === "manual") {
          record(accounting, await checkout.refreshHead());
          transition = "manual-new";
        } else if (message.consistency === "live") {
          record(accounting, await checkout.refreshLive());
          transition = "live-new";
        } else {
          throw new Error(`unknown targeted consistency ${message.consistency}`);
        }
        const observed = record(accounting, await checkout.readFileRange("/targeted.bin", 0n, 6n));
        const text = new TextDecoder().decode(new Uint8Array(observed.bytes));
        const expected = message.consistency === "pinned" ? "before" : "after!";
        assert(text === expected, `targeted actor observed ${text}, expected ${expected}`);
        assert(targetedVolume !== undefined, "targeted volume was released before speculation");
        const speculation = await exerciseSpeculation(
          fs,
          targetedVolume,
          checkout,
          message.operationSequence,
          accounting,
        );
        fs.close();
        fs = undefined;
        checkout = undefined;
        targetedAccounting = undefined;
        targetedVolume = undefined;
        post(channel, {
          event: "targeted-observed",
          consistency: message.consistency,
          observed: text,
          transition,
          operationSequence: message.operationSequence,
          speculation: speculation.status,
          speculationVerifiedBytes: speculation.verifiedBytes,
          accounting,
        });
        return;
      }
      if (message.command === "commit") {
        assert(checkout !== undefined, "actor commit arrived before preparation");
        const operation = new Uint8Array(16).fill(actor === "left" ? 72 : 73);
        const committed = await checkout.commit(operation);
        post(channel, { event: "committed", status: committed.status, path: authoredPath });
        return;
      }
      if (message.command === "verify") {
        fs = await openBrowserFs(databaseOptions(databaseName));
        assert(fs.capabilities.immutableObjects === "indexeddb-opfs", "reopened actor lost OPFS acceleration");
        const volume = await fs.openVolume(volumeIdentity);
        const readOnly = await volume.checkout({
          access: "read-only",
          consistency: "pinned",
          mutationMode: "none",
        });
        const seed = await readOnly.readFileRange("/seed.txt", 0n, 4n);
        const winner = await readOnly.readFileRange(message.winnerPath, 0n, BigInt(message.winnerBytes));
        assert(new TextDecoder().decode(new Uint8Array(seed.bytes)) === "seed", "fresh tab lost the durable seed");
        assert(new TextDecoder().decode(new Uint8Array(winner.bytes)) === message.winnerActor, "fresh tab read the wrong winning bytes");
        for (const absentPath of message.absentPaths) {
          const absent = await readOnly.lookupNoFollow(absentPath);
          assert(!absent.exists, `${absentPath} became visible at the durable head`);
        }
        fs.close();
        fs = undefined;
        post(channel, { event: "verified", reloaded: query.get("reload") === "1" });
        return;
      }
      if (message.command === "reload") {
        fs?.close();
        const next = new URL(location.href);
        next.searchParams.set("reload", "1");
        location.replace(next);
        return;
      }
      if (message.command === "close") {
        fs?.close();
        channel.close();
        window.close();
      }
    } catch (error) {
      post(channel, { event: "failed", error: errorText(error) });
    }
  });
  post(channel, { event: "ready", reloaded: query.get("reload") === "1" });
}

async function runTargetedCase(
  channel,
  nextMessage,
  actorUrl,
  run,
  operationSequence,
  consistency,
  index,
) {
  const database = `acyclic-fs-targeted-${operationSequence}-${consistency}-${Date.now()}-${crypto.randomUUID()}`;
  const identity = new Uint8Array(16).fill(80 + index);
  const accounting = emptyAccounting();
  const initial = await openBrowserFs(databaseOptions(database));
  const options = {
    ...portableVolumeOptions("durable"),
    concurrency: "serialized-authority",
  };
  const volume = await initial.createVolumeWithId(identity, options);
  recordWork(accounting, volume.acquisitionWork);
  const seed = await volume.checkout(checkoutOptions);
  recordWork(accounting, seed.acquisitionWork);
  record(accounting, await seed.createFile("/targeted.bin", new TextEncoder().encode("before")));
  const seeded = record(accounting, await seed.commit(new Uint8Array(16).fill(90 + index)));
  assert(seeded.status === "committed", "targeted seed did not publish");
  initial.close();

  const name = `targeted-${operationSequence}-${consistency}`;
  actorUrl.searchParams.set("actor", name);
  actorUrl.searchParams.set("database", database);
  const actorWindow = window.open(actorUrl, `acyclic-fs-${name}-${run}`);
  assert(actorWindow !== null, `browser blocked ${name} actor window`);
  await nextMessage((message) => message.actor === name && message.event === "ready", `${name} ready`);
  channel.postMessage({
    target: name,
    command: "targeted-prepare",
    consistency,
    operationSequence,
    database,
    volumeIdentity: Array.from(identity),
  });
  await nextMessage(
    (message) => message.actor === name && message.event === "targeted-prepared",
    `${name} prepared`,
  );

  const writerFs = await openBrowserFs(databaseOptions(database));
  const writerVolume = await writerFs.openVolume(identity);
  recordWork(accounting, writerVolume.acquisitionWork);
  const writer = await writerVolume.checkout(checkoutOptions);
  recordWork(accounting, writer.acquisitionWork);
  record(accounting, await writer.writeFile("/targeted.bin", 0n, new TextEncoder().encode("after!")));
  const published = record(accounting, await writer.commit(new Uint8Array(16).fill(100 + index)));
  assert(published.status === "committed", "targeted parent writer did not publish");
  writerFs.close();

  channel.postMessage({
    target: name,
    command: "targeted-observe",
    consistency,
    operationSequence,
  });
  const observed = await nextMessage(
    (message) => message.actor === name && message.event === "targeted-observed",
    `${name} observation`,
  );
  mergeAccounting(accounting, observed.accounting);
  actorWindow.close();

  const verifierFs = await openBrowserFs(databaseOptions(database));
  const verifierVolume = await verifierFs.openVolume(identity);
  recordWork(accounting, verifierVolume.acquisitionWork);
  const verifier = await verifierVolume.checkout({
    access: "read-only",
    consistency: "pinned",
    mutationMode: "none",
  });
  recordWork(accounting, verifier.acquisitionWork);
  const finalRead = record(accounting, await verifier.readFileRange("/targeted.bin", 0n, 6n));
  const finalText = new TextDecoder().decode(new Uint8Array(finalRead.bytes));
  assert(finalText === "after!", "targeted fresh engine lost durable writer bytes");
  verifierFs.close();
  return {
    operationSequence,
    consistency,
    final: finalText,
    observed: observed.observed,
    transition: observed.transition,
    operations: accounting.operations,
    speculation: observed.speculation,
    verifiedBytes:
      observed.observed.length + finalText.length + observed.speculationVerifiedBytes,
    work: accounting.work,
  };
}

function messageQueue(channel) {
  const buffered = [];
  const waiters = [];
  let failure;
  channel.addEventListener("message", (event) => {
    const message = event.data;
    if (message.event === "failed") {
      failure = new Error(`${message.actor}: ${message.error}`);
      for (const waiter of waiters.splice(0)) {
        clearTimeout(waiter.timeout);
        waiter.reject(failure);
      }
      return;
    }
    const index = waiters.findIndex((waiter) => waiter.predicate(message));
    if (index === -1) {
      buffered.push(message);
      return;
    }
    const [waiter] = waiters.splice(index, 1);
    clearTimeout(waiter.timeout);
    waiter.resolve(message);
  });
  return (predicate, label) => {
    if (failure !== undefined) return Promise.reject(failure);
    const index = buffered.findIndex(predicate);
    if (index !== -1) return Promise.resolve(buffered.splice(index, 1)[0]);
    return new Promise((resolve, reject) => {
      const waiter = { predicate, resolve, reject, timeout: undefined };
      waiter.timeout = setTimeout(() => {
        const position = waiters.indexOf(waiter);
        if (position !== -1) waiters.splice(position, 1);
        reject(new Error(`timed out waiting for ${label}`));
      }, 30_000);
      waiters.push(waiter);
    });
  };
}

async function runCoordinator() {
  const suffix = `${Date.now()}-${crypto.randomUUID()}`;
  for (const objectAcceleration of workspaceProfiles) {
    const engine = await openWorkspaceFs({
      ...databaseOptions(`acyclic-fs-workspace-${objectAcceleration}-${suffix}`),
      objectAcceleration,
    });
    try {
      await exerciseWorkspace(engine);
    } finally {
      engine.close();
    }
  }
  const run = crypto.randomUUID();
  const database = `acyclic-fs-multitab-${suffix}`;
  const channel = new BroadcastChannel(`acyclic-fs-multitab-${run}`);
  const nextMessage = messageQueue(channel);
  const initial = await openBrowserFs(databaseOptions(database));
  assert(initial.capabilities.immutableObjects === "indexeddb-opfs", "coordinator could not require OPFS");
  const volume = await initial.createVolumeWithId(
    volumeIdentity,
    portableVolumeOptions("durable"),
  );
  const seedCheckout = await volume.checkout(checkoutOptions);
  await seedCheckout.createFile("/seed.txt", new TextEncoder().encode("seed"));
  const seeded = await seedCheckout.commit(new Uint8Array(16).fill(74));
  assert(seeded.status === "committed", "initial durable generation did not publish");
  initial.close();

  const actorUrl = new URL(location.href);
  actorUrl.search = "";
  actorUrl.searchParams.set("run", run);
  actorUrl.searchParams.set("database", database);
  actorUrl.searchParams.set("actor", "left");
  const left = window.open(actorUrl, `acyclic-fs-left-${run}`);
  actorUrl.searchParams.set("actor", "right");
  const right = window.open(actorUrl, `acyclic-fs-right-${run}`);
  assert(left !== null && right !== null, "browser blocked required top-level actor windows");

  for (const name of ["left", "right"]) {
    await nextMessage((message) => message.actor === name && message.event === "ready", `${name} ready`);
    channel.postMessage({ target: name, command: "prepare" });
  }
  const prepared = await Promise.all(["left", "right"].map((name) =>
    nextMessage((message) => message.actor === name && message.event === "prepared", `${name} prepared`)
  ));
  assert(prepared.every((message) => message.immutableObjects === "indexeddb-opfs"), "one tab bypassed OPFS");

  channel.postMessage({ target: "all", command: "commit" });
  const outcomes = await Promise.all(["left", "right"].map((name) =>
    nextMessage((message) => message.actor === name && message.event === "committed", `${name} commit`)
  ));
  const ordered = outcomes.map((outcome) => outcome.status).sort();
  assert(ordered[0] === "committed" && ordered[1] === "conflict", `unexpected authority race: ${ordered.join(",")}`);
  const winning = outcomes.find((outcome) => outcome.status === "committed");
  const losing = outcomes.find((outcome) => outcome.status === "conflict");
  assert(winning !== undefined && losing !== undefined, "authority race had no exact winner and loser");

  left.close();
  right.close();
  actorUrl.searchParams.set("actor", "abandoned");
  const abandoned = window.open(actorUrl, `acyclic-fs-abandoned-${run}`);
  assert(abandoned !== null, "browser blocked the abrupt-lifecycle actor window");
  await nextMessage((message) => message.actor === "abandoned" && message.event === "ready", "abandoned ready");
  channel.postMessage({ target: "abandoned", command: "prepare" });
  await nextMessage(
    (message) => message.actor === "abandoned" && message.event === "prepared",
    "abandoned prepared",
  );
  abandoned.close();
  assert(abandoned.closed, "abrupt-lifecycle actor did not close");
  actorUrl.searchParams.set("actor", "verifier");
  const verifier = window.open(actorUrl, `acyclic-fs-verifier-${run}`);
  assert(verifier !== null, "browser blocked the fresh verifier tab");
  await nextMessage((message) => message.actor === "verifier" && message.event === "ready" && !message.reloaded, "verifier ready");
  const verifyCommand = {
    target: "verifier",
    command: "verify",
    winnerPath: winning.path,
    absentPaths: [losing.path, "/abandoned.txt"],
    winnerActor: winning.actor,
    winnerBytes: winning.actor.length,
  };
  channel.postMessage(verifyCommand);
  await nextMessage((message) => message.actor === "verifier" && message.event === "verified" && !message.reloaded, "fresh-tab verification");
  channel.postMessage({ target: "verifier", command: "reload" });
  await nextMessage((message) => message.actor === "verifier" && message.event === "ready" && message.reloaded, "reloaded verifier ready");
  channel.postMessage(verifyCommand);
  await nextMessage((message) => message.actor === "verifier" && message.event === "verified" && message.reloaded, "reloaded verification");
  verifier.close();
  const targetedCases = [];
  let targetedIndex = 0;
  for (const operationSequence of [
    "create_write_read",
    "speculative_residency",
    "speculative_promotion",
  ]) {
    for (const consistency of ["pinned", "tracking-safe", "live", "manual"]) {
      targetedCases.push(
        await runTargetedCase(
          channel,
          nextMessage,
          actorUrl,
          run,
          operationSequence,
          consistency,
          targetedIndex,
        ),
      );
      targetedIndex += 1;
    }
  }
  channel.close();

  result.dataset.status = "passed";
  result.textContent = JSON.stringify({
    status: "passed",
    authorityOutcomes: ordered,
    winningActor: winning.actor,
    opfsActors: prepared.length,
    topLevelWindows: 17,
    freshContextVerified: true,
    reloadVerified: true,
    abruptTabRecoveryVerified: true,
    workspaceProfiles,
    targetedCases,
  });
}

(actor === null ? runCoordinator() : runActor()).catch((error) => {
  result.dataset.status = "failed";
  result.textContent = errorText(error);
});
