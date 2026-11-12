import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readdirSync } from "node:fs";

const [privateBody, projectBody] = process.argv.slice(2);
assert.ok(privateBody && projectBody);
const paths = readdirSync(".", { recursive: true });
for (const name of ["agent", "context", "children", "evals"]) {
  assert.ok(!paths.some(path => path === name || path.startsWith(`${name}/`) || path.startsWith(`${name}\\`)), name);
}
const run = (command, args) => execFileSync(command, args, { encoding: "utf8" });
const tracked = run("git", ["ls-files", "--cached", "--others", "--exclude-standard"]);
assert.ok(tracked.includes("ui/index.md"));
assert.ok(!tracked.includes("private/ui"));
const source = run("git", ["grep", "--no-index", "-n", "-e", projectBody, "--", "ui/index.md"]);
assert.ok(source.includes(projectBody));
try {
  run("git", ["grep", "--no-index", "-l", "-e", privateBody, "--", "."]);
  assert.fail("git grep reached the private view");
} catch (error) {
  assert.equal(error.status, 1);
}
if (process.platform !== "win32") {
  const listing = run("ls", ["-R", "."]);
  assert.ok(listing.includes("index.md"));
  assert.ok(!listing.includes("private/ui"));
  const matches = run("grep", ["-r", "-l", "--", projectBody, "."]);
  assert.ok(matches.includes("ui/index.md"));
  try {
    run("grep", ["-r", "-l", "--", privateBody, "."]);
    assert.fail("recursive grep reached the private view");
  } catch (error) {
    assert.equal(error.status, 1);
  }
}
process.stdout.write("CWD_ISOLATED");
