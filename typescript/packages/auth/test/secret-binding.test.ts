import { test } from "node:test";
import assert from "node:assert/strict";
import { materializeSecretBinding, verifyHmac } from "../src/secret-binding.js";

test("protected guest surface cannot read host data through properties, replies or errors", async () => {
  const planted = "HOST_ONLY_NEGATIVE_CONTROL";
  const host = {
    secret: planted,
    read() { return this.secret; },
    async verifyHmac(): Promise<boolean> { throw new Error(this.secret); },
  };
  // Negative control proves the planted host value is actually readable there.
  assert.equal(host.read(), planted);
  const binding = materializeSecretBinding("WEBHOOK", host);
  assert.deepEqual(Reflect.ownKeys(binding), ["verifyHmac"]);
  assert.equal(JSON.stringify(binding), "{}");
  assert.equal(Reflect.get(binding, "secret"), undefined);
  assert.equal(Reflect.get(binding, "read"), undefined);
  const bytes = new Uint8Array();
  await assert.rejects(verifyHmac(binding, "HMAC-SHA256", bytes, bytes), error => {
    assert.equal(String(error).includes(planted), false);
    assert.equal(Reflect.get(error as object, "cause"), undefined);
    return true;
  });
  // Reject a compromised/miswired provider returning a key instead of a boolean.
  const malformed = materializeSecretBinding("WEBHOOK", {
    async verifyHmac() { return planted as unknown as boolean; },
  });
  await assert.rejects(malformed.verifyHmac("HMAC-SHA256", bytes, bytes), /verification unavailable/);
});
