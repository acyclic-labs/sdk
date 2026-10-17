export const rust = [["acyclic-fs", "example", "filesystem-descriptor-digest"]];

export function render([stdout]) {
  const digest = stdout.trim();
  if (!/^[0-9a-f]{64}$/.test(digest)) throw new Error("Rust filesystem descriptor digest is invalid");
  const header = "// Generated from the Rust filesystem descriptor; do not edit.\n";
  return {
    "typescript/packages/filesystem/generated/descriptor-digest.js": `${header}export const FILESYSTEM_DESCRIPTOR_DIGEST = "${digest}";\n`,
    "typescript/packages/filesystem/generated/descriptor-digest.d.ts": `${header}export declare const FILESYSTEM_DESCRIPTOR_DIGEST: "${digest}";\n`,
  };
}
