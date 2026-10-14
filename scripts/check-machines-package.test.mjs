import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";

test("Machines source tests and packaging share one verified checkout and reject source mutations", () => {
  const source = readFileSync("scripts/check-machines-package.sh", "utf8").replaceAll("\r\n", "\n");
  const start = source.indexOf('package_root="$work/package-source"');
  const end = source.indexOf('crate="$package_target/package/', start);
  assert.ok(start >= 0 && end > start);
  const commands = source.slice(start, end);
  assert.doesNotMatch(source, /git archive|source_root|source_manifest|package_manifest/);
  const bash = process.platform === "win32"
    ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  mkdirSync(".tools", { recursive: true });
  for (const mutate of [false, true]) {
    const directory = mkdtempSync(".tools/machines-package-");
    try {
      const result = spawnSync(bash, ["-c", `set -euo pipefail
work="$SDK_FIXTURE"
root="$work/original"
head=recorded-source
mkdir -p "$root"
command() { return 1; }
git() {
  if [[ "$1" == clone ]]; then
    [[ "$#" == 6 && "$2" == --quiet && "$3" == --no-checkout && "$4" == -- && "$5" == "$root" ]]
    [[ ! -e "$work/clone-started" ]]
    touch "$work/clone-started"
    mkdir -p "$6"
    echo '[workspace]' > "$6/Cargo.toml"
  else
    [[ "$#" == 6 && "$1" == -C && "$2" == "$package_root" && "$3" == checkout && "$4" == --quiet && "$5" == --detach && "$6" == "$head" ]]
    touch "$work/checkout-verified"
  fi
}
clean_head() {
  [[ "$2" == "$head" && -f "$work/checkout-verified" ]]
  [[ "$1" == "$root" || "$1" == "$package_root" ]]
  [[ ! -f "$package_root/Cargo.toml.dirty" ]]
}
cargo() {
  [[ -f "$work/checkout-verified" ]]
  local action="$1" argument previous="" observed=""
  for argument in "$@"; do
    if [[ "$previous" == --manifest-path ]]; then observed="$argument"; fi
    previous="$argument"
  done
  [[ "$observed" == "$package_root/Cargo.toml" ]]
  printf '%s\\n' "$action" >> "$work/cargo-calls"
  case "$action" in
    metadata) echo '{"packages":[{"name":"acyclic-machines","version":"0.2.0"}]}' ;;
    test)
      [[ "$*" == "test --locked -p acyclic-machines -p acyclic-harness --manifest-path $manifest --target-dir $package_target_argument" ]]
      if [[ "$SDK_MUTATE_SOURCE" == true ]]; then touch "$package_root/Cargo.toml.dirty"; fi ;;
    package)
      [[ "$*" == "package --locked --no-verify -p acyclic-machines --manifest-path $manifest --target-dir $package_target_argument" ]] ;;
    *) return 33 ;;
  esac
}
${commands}
echo packaged-verified-source
`], {
        encoding: "utf8", timeout: 60000,
        env: { ...process.env, SDK_FIXTURE: directory.replaceAll("\\", "/"), SDK_MUTATE_SOURCE: String(mutate) },
      });
      assert.equal(result.error, undefined);
      assert.equal(result.status, mutate ? 1 : 0, result.stderr);
      assert.equal(result.stdout.includes("packaged-verified-source"), !mutate);
      assert.equal(readFileSync(join(directory, "cargo-calls"), "utf8"), mutate ? "metadata\ntest\n" : "metadata\ntest\npackage\n");
    } finally {
      assert.ok(resolve(directory).startsWith(resolve(".tools") + sep));
      rmSync(directory, { recursive: true, force: true });
    }
  }
});
