import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";

const source = readFileSync(new URL("../typescript/packages/actors/src/generated/actors-service.ts", import.meta.url), "utf8");
const names = [...new Set(ActorsService.methods.flatMap(method => [method.input.typeName, method.output.typeName]))];
const complete = text => {
  for (const name of names) assert.ok(text.includes(`"${name}":`), `missing semantic root: ${name}`);
};

test("Actors build emits every descriptor-bound semantic RPC root", () => {
  assert.ok(names.length > 0);
  complete(source);
  for (const name of names) assert.throws(() => complete(source.replace(`"${name}":`, '"removed":')));
  const marker = readFileSync(new URL("../typescript/packages/actors/src/generated/semantic/actors/CurrentHeadMarker.ts", import.meta.url), "utf8");
  assert.ok(marker.includes("CurrentHeadMarker"));
});
