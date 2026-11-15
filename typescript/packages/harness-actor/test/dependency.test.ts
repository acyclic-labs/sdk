import { expect, test } from "bun:test";
import { DependencyUnavailableError } from "@acyclic-labs/actors/guest";
import { HARNESS_BINDINGS, MODEL_FETCH } from "../src/bindings.js";
import { manifest, onRecord } from "../src/harness-actor.js";

test("every delivery after a readiness without a binding names that binding", async () => {
  // Every selected binding except the Machines workspace.
  const env = {
    [HARNESS_BINDINGS.objects]: {}, [HARNESS_BINDINGS.streams]: {}, [HARNESS_BINDINGS.authority]: {},
    [MODEL_FETCH]: () => ({}),
  };
  const readiness = await manifest({ env } as unknown as Parameters<typeof manifest>[0])
    .then(() => undefined, (error: unknown) => error);
  expect(readiness).toBeInstanceOf(DependencyUnavailableError);
  expect(JSON.stringify({ type: (readiness as DependencyUnavailableError).type, detail: (readiness as DependencyUnavailableError).detail }))
    .toBe('{"type":"dependencyUnavailable","detail":{"dependency":"machinesWorkspace"}}');
  // The first delivery (an agent Bind) reports the same typed dependency, not an
  // unopened environment.
  expect(await onRecord(Uint8Array.of(0)).then(() => undefined, (error: unknown) => error)).toBe(readiness);
});
