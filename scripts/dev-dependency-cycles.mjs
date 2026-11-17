// A crate's dev-dependency closure must not reach the crate itself. Cargo permits such a
// cycle, but builds the crate twice (test and dependency copies), so types from the two copies
// are distinct. The BUILD generator refuses to render a workspace that would do this.
export function devDependencyCycles(members, nodes) {
  const cycles = [];
  for (const id of members) {
    const start = nodes.get(id);
    if (!start) continue;
    const pending = start.deps
      .filter(dep => dep.dep_kinds.some(kind => kind.kind === "dev"))
      .map(dep => [dep.pkg]);
    const seen = new Set();
    while (pending.length) {
      const path = pending.pop();
      const current = path.at(-1);
      if (current === id) {
        cycles.push(path);
        continue;
      }
      if (seen.has(current)) continue;
      seen.add(current);
      for (const dep of nodes.get(current)?.deps ?? []) {
        // Only the first edge is a development edge. A dependency's own dev-dependencies
        // are not built when this crate's tests are built, so they cannot close the cycle.
        if (dep.dep_kinds.some(kind => kind.kind !== "dev")) pending.push([...path, dep.pkg]);
      }
    }
  }
  return cycles;
}
