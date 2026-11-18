import { execFileSync } from "node:child_process";

/** @param {string} cwd @param {string} [binary] */
export function conformanceCertificate(cwd, binary) {
  const options = { cwd, encoding: /** @type {const} */ ("utf8") };
  const json = binary === undefined
    ? execFileSync("bazel", ["run", "//rust/crates/actors:conformance-certificate_example"], options)
    : execFileSync(binary, [], options);
  return JSON.parse(json);
}
