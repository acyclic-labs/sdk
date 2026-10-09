export const rust = [["acyclic-harness", "example", "model-contract-fixtures"]];
export function render([stdout]) {
  const fixture = JSON.parse(stdout);
  if (Object.keys(fixture).sort().join(",") !== "prefix,request"
      || typeof fixture.request !== "string" || typeof fixture.prefix !== "string") {
    throw new Error("Harness model fixture inventory drift");
  }
  return {
    "rust/crates/harness/fixtures/model-request.json": fixture.request,
    "rust/crates/harness/fixtures/model-prefix.json": fixture.prefix,
  };
}
