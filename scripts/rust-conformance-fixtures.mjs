import { execFileSync } from "node:child_process";

/** @param {string} cwd */
export function actorsHttpRoutes(cwd) {
  const json = execFileSync("bazel", ["run", "//rust/crates/actors:actors-http-routes_example"], { cwd, encoding: "utf8" });
  return Object.fromEntries(JSON.parse(json));
}

/** @param {string} cwd @param {string} [binary] */
export function conformanceCertificate(cwd, binary) {
  const options = { cwd, encoding: /** @type {const} */ ("utf8") };
  const json = binary === undefined
    ? execFileSync("bazel", ["run", "//rust/crates/actors:conformance-certificate_example"], options)
    : execFileSync(binary, [], options);
  return JSON.parse(json);
}
