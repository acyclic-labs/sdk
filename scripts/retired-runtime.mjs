// Retirement applies to declared SDK surfaces; repository tooling may use Python.
const inventories = new Set([
  "languages/package-names.json",
  "release/npm-packages.json",
  "release/cargo-crates.json",
]);
const retiredRuntime = /python|pytest|boto3|botocore|\.py(?:i|c|o)?\b/i;

/** @param {Iterable<readonly [string, unknown]>} documents */
export function retiredRuntimeFindings(documents) {
  const findings = [];
  for (const [path, document] of documents) {
    if (inventories.has(path) && retiredRuntime.test(JSON.stringify(document))) {
      findings.push(`step=retired-runtime SDK inventory ${path}; recovery command: restore supported package declarations in ${path}, then node scripts/check-metadata.mjs`);
    }
  }
  return findings;
}
