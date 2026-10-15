import { expect, test } from "bun:test";
import { deserializeAttachment, serializeAttachment } from "../src/attachment.js";

test("structured attachments round trip within the UTF-8 byte bound", () => {
  const value = { name: "é", enabled: true, values: [1, null] };
  expect(deserializeAttachment(serializeAttachment(value))).toEqual(value);
  expect(serializeAttachment("é".repeat(1023)).byteLength).toBe(2048);
  expect(() => serializeAttachment("é".repeat(1024))).toThrow(RangeError);
  expect(() => serializeAttachment(undefined)).toThrow(TypeError);
  expect(() => deserializeAttachment(new Uint8Array(2049))).toThrow(RangeError);
  expect(() => deserializeAttachment(new Uint8Array([255]))).toThrow(TypeError);
});
