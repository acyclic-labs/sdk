import assert from "node:assert/strict";
import test from "node:test";
import { sha256 } from "../authority.mjs";

export function registerArchiveTests(language, readPackage, fixture, { extraMember, metadata, metadataError }) {
  test(`${language}: archive admission verifies every payload byte`, async t => {
    const f = fixture(t), admitted = await readPackage(f.args);
    assert.equal(admitted.payload.size, f.payload.size);
    for (const [name, bytes] of admitted.payload) assert.deepEqual(bytes, f.payload.get(name));
  });

  test(`${language}: archive links, traversal, duplicates and unlisted members reject`, async t => {
    for (const mutate of [
      entries => { entries[0].type = "2"; },
      entries => { entries[0].name = "../outside"; },
      entries => { entries.push(entries[0]); },
      entries => { entries.push({ name: extraMember, bytes: Buffer.from("extra") }); },
    ]) {
      const f = fixture(t), entries = f.entries();
      mutate(entries); f.saveArchive(entries);
      await assert.rejects(async () => await readPackage(f.args));
    }
  });

  test(`${language}: missing, changed and self-consistently substituted metadata reject`, async t => {
    const missing = fixture(t); missing.saveArchive(missing.entries().slice(1));
    await assert.rejects(async () => await readPackage(missing.args), /inventory differs/);
    const altered = fixture(t), entries = altered.entries();
    entries[0].bytes = Buffer.from("changed"); altered.saveArchive(entries);
    await assert.rejects(async () => await readPackage(altered.args), /payload digest differs/);
    for (const name of metadata) {
      const f = fixture(t);
      f.payload.set(name, Buffer.from("substituted"));
      f.receipt.output_sha256[name] = sha256(f.payload.get(name));
      f.saveReceipt(); f.saveArchive();
      await assert.rejects(async () => await readPackage(f.args), metadataError);
    }
  });

  test(`${language}: caller archive digest is enforced`, async t => {
    const f = fixture(t); f.args.sha256 = "0".repeat(64);
    await assert.rejects(async () => await readPackage(f.args), /archive digest differs/);
  });
}
