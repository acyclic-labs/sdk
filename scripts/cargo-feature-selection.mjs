// Admit requested root features against Cargo's defining packages. This is a
// request identity, not a resolved dependency/cfg graph or a compiler receipt.
// Library, test, native and wasm invocations must still run independently.
export function selectCargoFeatures(items, names, flags) {
  const fail = detail => {
    throw new Error(`STEP_FAILED cargo-feature-selection: ${detail}; recovery: node scripts/check-feature-matrix.mjs --list`);
  };
  const packages = names.map(name => {
    const item = items.get(name);
    if (!item) fail(`unknown package ${name}`);
    return item;
  });
  if (!packages.length || new Set(names).size !== names.length) fail("empty or duplicate package selection");
  const requested = new Map(packages.map(item => [item.name, new Set()]));
  let defaults = true;
  let all = false;
  for (let index = 0; index < flags.length; index++) {
    const flag = flags[index];
    if (flag === "--no-default-features") defaults = false;
    else if (flag === "--all-features") all = true;
    else if (flag === "--features") {
      const value = flags[++index];
      if (!value || value.startsWith("--")) fail("missing feature names");
      for (const feature of value.split(/[\s,]+/u).filter(Boolean)) {
        const parts = feature.split("/");
        const roots = parts.length === 1 ? packages : packages.filter(item => item.name === parts[0]);
        const name = parts.at(-1);
        const defining = roots.filter(item => Object.hasOwn(item.features, name));
        if (parts.length > 2 || !defining.length) fail(`unknown selected-root feature ${feature}`);
        for (const item of defining) requested.get(item.name).add(name);
      }
    } else fail(`unsupported feature flag ${flag}`);
  }
  return {
    packages,
    flags: [...flags],
    // Retain each defining package, including `default` itself: cfg(feature =
    // "default") is valid. Dependency feature tokens remain Cargo's authority.
    roots: packages.map(item => ({
      id: item.id,
      features: declaredFeatures(item, [
        ...requested.get(item.name),
        ...(defaults && Object.hasOwn(item.features, "default") ? ["default"] : []),
        ...(all ? Object.keys(item.features) : []),
      ]),
    })),
  };
}

function declaredFeatures(item, features) {
  const seen = new Set();
  const pending = [...features];
  while (pending.length) {
    const entry = pending.pop();
    if (seen.has(entry)) continue;
    seen.add(entry);
    if (Object.hasOwn(item.features, entry)) pending.push(...item.features[entry]);
  }
  return [...seen].sort();
}

// Projection keeps the original requested feature policy. Never substitute
// the declared closure for flags: Cargo resolves dependencies and target cfg.
export function cargoFeatureArguments(selection, { manifest = false, target, targets } = {}) {
  if (manifest && selection.packages.length !== 1) {
    throw new Error("STEP_FAILED cargo-feature-projection: manifest selection requires one package; recovery: node scripts/check-feature-matrix.mjs --list");
  }
  return [
    ...(manifest ? ["--manifest-path", selection.packages[0].manifest_path]
      : selection.packages.flatMap(item => ["-p", item.name])),
    ...(target ? ["--target", target] : []),
    ...(targets ? [`--${targets}`] : []),
    ...selection.flags,
  ];
}

/** Declared package-local expansion used by the existing full matrix. This
 * does not resolve dependency, target or development-feature unification. */
export function enabled(item, features) {
  return declaredFeatures(item, features).join(" ");
}

export function featureSets(item, eachFeature = false) {
  if (!eachFeature) return [["check", "--locked", "--manifest-path", item.manifest_path, "--lib", "--all-features"]];
  const own = Object.keys(item.features).filter(name => name !== "default").sort();
  const defaults = Object.hasOwn(item.features, "default") ? ["default"] : [];
  const sets = [
    [["--no-default-features"], []],
    [[], defaults],
    [["--all-features"], Object.keys(item.features)],
    ...own.map(feature => [["--no-default-features", "--features", feature], [feature]]),
  ];
  const checked = new Set();
  return sets.filter(([, features]) => {
    const key = enabled(item, features);
    return !checked.has(key) && checked.add(key);
  }).map(([set]) => ["check", "--locked", "--manifest-path", item.manifest_path, "--lib", ...set]);
}
