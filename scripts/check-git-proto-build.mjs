import { execFileSync, spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const revision = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8", windowsHide: true }).trim();
const parent = process.env.SDK_TEMP_DIR ?? join(root, "target");
mkdirSync(parent, { recursive: true });
const consumer = mkdtempSync(join(parent, "git-proto-consumer-"));
const vendor = join(consumer, "vendor");
try {
  // Use the published Git revision, independently of checkout depth and partial-clone caches.
  const repository = execFileSync("git", ["remote", "get-url", "origin"], { cwd: root, encoding: "utf8", windowsHide: true }).trim();
  const environment = Object.fromEntries(Object.entries(process.env).filter(([name]) => !["PROTOC", "PROTOC_INCLUDE", "PATH"].includes(name.toUpperCase())));
  environment.PATH = process.env.PATH ?? "";
  environment.CARGO_TARGET_DIR = resolve(root, process.env.CARGO_TARGET_DIR ?? "target");
  environment.RUSTUP_TOOLCHAIN ??= execFileSync("rustup", ["show", "active-toolchain"], { cwd: root, encoding: "utf8", windowsHide: true }).trim().split(" ")[0];

  writeFileSync(join(consumer, "Cargo.toml"), `[package]\nname = "sdk-git-proto-consumer"\nversion = "0.0.0"\nedition = "2024"\n\n[workspace]\n\n[dependencies]\nacyclic-objects = { git = ${JSON.stringify(repository)}, rev = ${JSON.stringify(revision)} }\n\n[lib]\npath = "lib.rs"\n`);
  writeFileSync(join(consumer, "lib.rs"), "");
  function cargo(args, env, capture = false) {
    const result = spawnSync("cargo", args, { cwd: consumer, env, encoding: "utf8", windowsHide: true, stdio: ["ignore", capture ? "pipe" : "inherit", "inherit"] });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`cargo ${args[0]} failed: ${result.status ?? result.signal}`);
    return result.stdout;
  }
  const configuration = cargo(["vendor", "--versioned-dirs", vendor], environment, true);
  mkdirSync(join(consumer, ".cargo"));
  writeFileSync(join(consumer, ".cargo/config.toml"), configuration);

  // A PATH lookup must fail even on hosts that happen to install system protoc.
  const trap = join(consumer, "no-system-protoc");
  mkdirSync(trap);
  copyFileSync(process.execPath, join(trap, process.platform === "win32" ? "protoc.exe" : "protoc"));
  cargo(["build", "--locked", "--offline", "--lib"], { ...environment, CARGO_NET_OFFLINE: "true", PATH: `${trap}${delimiter}${environment.PATH}` });
  console.log(`Git dependency ${revision}: built offline from vendored sources with PROTOC unset and system protoc blocked.`);
} finally {
  rmSync(consumer, { recursive: true, force: true });
}
