import { execFileSync } from "node:child_process";
import { bazelExecutable } from "./bazel-executables.mjs";

/** @param {string} cwd */
export function actorsHttpRoutes(cwd) {
  const binary = bazelExecutable(cwd, "//rust/crates/actors:actors-http-routes_example");
  const json = execFileSync(binary, [], { cwd, encoding: "utf8" });
  return Object.fromEntries(JSON.parse(json));
}

/** @param {string} cwd @param {string} [binary] */
export function conformanceCertificate(cwd, binary) {
  const executable = binary ?? bazelExecutable(cwd, "//rust/crates/actors:conformance-certificate_example");
  const json = execFileSync(executable, [], { cwd, encoding: "utf8" });
  return JSON.parse(json);
}
