// Assertions used only by browser qualification and its shared lifecycle fixture.
function assert(value, message = "assertion failed") { if (!value) throw new Error(message); }
assert.ok = assert;
assert.equal = (actual, expected) => assert(Object.is(actual, expected), `expected ${String(expected)}, received ${String(actual)}`);
function same(actual, expected) {
  if (Object.is(actual, expected)) return true;
  if (actual === null || expected === null || typeof actual !== "object" || typeof expected !== "object") return false;
  const keys = Object.keys(actual); const other = Object.keys(expected);
  return keys.length === other.length && keys.every(key => Object.hasOwn(expected, key) && same(actual[key], expected[key]));
}
assert.deepEqual = (actual, expected) => assert(same(actual, expected), "deep equality assertion failed");
assert.rejects = async (promise, predicate) => {
  try { await (typeof promise === "function" ? promise() : promise); }
  catch (error) { if (predicate !== undefined) assert(predicate(error), `unexpected rejection: ${error}`); return; }
  throw new Error("expected rejection");
};
export default assert;
