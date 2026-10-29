import { copyFile, cp, lstat, mkdir, readFile, rm } from "node:fs/promises";
import { join } from "node:path";

// Stage the production npm closure with its admitted native executable. Never
// invoke postinstall: acceptance must run the caller's exact shipping binary.
export async function stagePlugin(source, binary, destination) {
  await mkdir(destination);
  const manifest = JSON.parse(await readFile(join(source, "package.json"), "utf8"));
  for (const entry of ["package.json", ...manifest.files]) {
    // npm files may name unpublished certification. Only an absent top-level
    // member is optional; a failed copy of an existing member must propagate.
    try { await lstat(join(source, entry)); }
    catch (error) { if (error.code === "ENOENT") continue; throw error; }
    await cp(join(source, entry), join(destination, entry), { recursive: true, force: false, errorOnExist: true });
  }
  const name = process.platform === "win32" ? "acyclic.exe" : "acyclic";
  if (name.endsWith(".exe")) await rm(join(destination, "bin/acyclic"));
  await copyFile(binary, join(destination, "bin", name));
  return join(destination, "bin", name);
}
