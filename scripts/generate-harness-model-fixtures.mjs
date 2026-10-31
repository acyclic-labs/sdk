export const rust = [["acyclic-harness", "example", "model-contract-fixtures"], ["acyclic-harness-core", "example", "agent-record-corpus"]];
export function render([stdout, corpus]) {
  const fixture = JSON.parse(stdout);
  if (Object.keys(fixture).sort().join(",") !== "prefix,request"
      || typeof fixture.request !== "string" || typeof fixture.prefix !== "string") {
    throw new Error("Harness model fixture inventory drift");
  }
  return {
    "rust/crates/harness/fixtures/model-request.json": fixture.request,
    "rust/crates/harness/fixtures/model-prefix.json": fixture.prefix,
    ...Object.fromEntries(Object.entries(JSON.parse(corpus)).map(([name, bytes]) => [`rust/fuzz/corpus/inbox_record/${name}`, Buffer.from(bytes)])),
  };
}
