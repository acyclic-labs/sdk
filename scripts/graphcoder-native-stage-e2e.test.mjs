import test from "node:test";
import assert from "node:assert/strict";
import { correlateResponses } from "./graphcoder-native-stage-e2e.mjs";

const response = requestId => JSON.stringify({ request_id: requestId, ok: true });

test("native stage correlation accepts independent responses in completion order", () => {
  const responses = correlateResponses([response("invalid-fixture"), response("list-1")]);
  assert.equal(responses.get("list-1").ok, true);
  assert.equal(responses.get("invalid-fixture").ok, true);
});

test("native stage correlation rejects duplicate response identities", () => {
  assert.throws(() => correlateResponses([response("list-1"), response("list-1")]), /duplicate request_id/);
});

test("native stage correlation rejects unknown response identities", () => {
  assert.throws(() => correlateResponses([response("list-1"), response("unexpected")]), /unknown request_id/);
});

test("native stage correlation rejects missing response identities", () => {
  assert.throws(() => correlateResponses([response("list-1")]), /omitted request_id/);
});
