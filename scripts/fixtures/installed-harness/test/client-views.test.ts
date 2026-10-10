import { expect, test } from "bun:test";
import { ClientViews } from "@acyclic-labs/harness/client";
import { clientReadable } from "@acyclic-labs/harness/svelte";
import { receiveClientSnapshots } from "@acyclic-labs/harness/worker";

test("installed optional exports use the one Rust kernel without installing React", async () => {
  expect(typeof receiveClientSnapshots).toBe("function");
  await ClientViews.initialize();
  const client = new ClientViews<number, null, { value: number; revision: string }>({
    identity: "1",
    validate: () => ({ bytes: 128, work: 1 }),
    observe: evidence => ({ fact: { key: "a", basis: evidence.revision, value: evidence.value, bytes: 128 }, operation: null, work: 1 }),
    corresponds: (predicted, canonical) => ({ matches: predicted === canonical, work: 1 }),
  }, "1", 0n, { records: 1, branches: 1, edges: 1, bytes: 100_000, work: 32, retention: 10n, visible: 1 });
  try {
    client.observe("a", { value: 0, revision: "0" });
    const branch = client.begin({ key: "a", basis: "0", operation: "op", predicted: 1, assumption: null, dependencies: [], expires: 5n });
    const selected = client.select("a", [branch]);
    const snapshots: number[] = [];
    const detach = clientReadable(selected).subscribe(snapshot => snapshots.push(snapshot.value));
    expect(snapshots).toEqual([1]);
    client.discard(branch);
    expect(snapshots).toEqual([1, 0]);
    detach(); selected.dispose(); client.release("a");
    expect(client.residency()).toEqual([0, 0, 0, 0]);
  } finally { client.dispose(); }
});
