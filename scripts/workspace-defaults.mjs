// The root SDK set is explicit after side tools/proofs join the workspace.
export function workspaceDefaults(metadata) {
  const fail = () => {
    throw new Error("STEP_FAILED workspace-default-selection: metadata defaults are empty or incomplete; recovery: cargo metadata --locked --no-deps --format-version 1");
  };
  const ids = metadata.workspace_default_members;
  const members = metadata.workspace_members;
  if (!Array.isArray(ids) || !ids.length || !Array.isArray(members)
    || !Array.isArray(metadata.packages)) fail();
  const selected = new Set(ids);
  const all = new Set(members);
  if (selected.size !== ids.length || ids.some(id => !all.has(id))) fail();
  const packages = metadata.packages.filter(item => selected.has(item.id));
  if (packages.length !== selected.size
    || packages.some(item => typeof item.name !== "string" || !item.name)
    || new Set(packages.map(item => item.id)).size !== selected.size
    || new Set(packages.map(item => item.name)).size !== selected.size) fail();
  return packages.sort((a, b) => a.name.localeCompare(b.name));
}
