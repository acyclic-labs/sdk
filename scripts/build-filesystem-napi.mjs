import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { runCommand } from "./run-command.mjs";

/** Build the filesystem binding and return its downloaded shared library. */
/** @param {string} [cwd] */
export async function buildFilesystemNapi(cwd) {
  const target = "//rust/crates/filesystem-napi:acyclic_fs_napi_cdylib";
  const download = "--remote_download_toplevel";
  if (await runCommand("bazel", ["build", download, target], { cwd }) !== 0) {
    throw new Error(`native binding build failed for ${target}`);
  }
  /** @param {...string} args */
  const bazelOutput = (...args) => execFileSync("bazel", args, { cwd, encoding: "utf8" }).trim();
  const files = bazelOutput("cquery", download, "--output=files", target).split(/\r?\n/);
  const libraries = files.filter((file) => /\.(?:dll|so|dylib)$/.test(file));
  if (libraries.length !== 1) {
    throw new Error(`expected one shared library from ${target}, got ${libraries.length}`);
  }
  return resolve(bazelOutput("info", "execution_root"), libraries[0]);
}
