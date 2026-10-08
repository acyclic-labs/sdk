import assert from "node:assert/strict";
import test from "node:test";
import { generationArgs } from "./generate-actors.mjs";

test("Actors generation forwards every CLI mode to the locked Rust entrypoint", () => {
  for (const mode of ["check", "write", "rust"]) {
    const args = generationArgs(mode);
    assert.deepEqual(args.slice(0, 10), ["run", "--offline", "--locked", "-p", "acyclic-actors", "--example", "actors-http-routes", "--", "--generate", mode]);
  }
  assert.equal(generationArgs()[9], "check");
  assert.throws(() => generationArgs("unknown"), /usage:/);
});