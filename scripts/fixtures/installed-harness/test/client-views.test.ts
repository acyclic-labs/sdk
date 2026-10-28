import { expect, test } from "bun:test";
import { SnapshotStore } from "@acyclic-labs/harness";
import { clientReadable } from "@acyclic-labs/harness/svelte";
import { receiveClientSnapshots } from "@acyclic-labs/harness/worker";

test("installed optional exports adapt one snapshot store without installing React", () => {
  expect(typeof receiveClientSnapshots).toBe("function");
  const store = new SnapshotStore(1);
  const snapshots: number[] = [];
  const detach = clientReadable(store).subscribe(snapshot => snapshots.push(snapshot));
  expect(snapshots).toEqual([1]);
  store.publish(0);
  expect(snapshots).toEqual([1, 0]);
  detach(); store.dispose();
});
