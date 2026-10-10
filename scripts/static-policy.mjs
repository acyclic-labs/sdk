// Keep workflow Actions in ordinary block mappings so every reference is
// visible to this dependency-free static check. Unsupported YAML forms fail.
export function pinnedActions(file, source) {
  let scalarIndent = null;
  for (const line of source.split('\n')) {
    if (!line.trim()) continue;
    const indent = line.match(/^\s*/)[0].length;
    if (scalarIndent !== null) {
      if (indent > scalarIndent) continue;
      scalarIndent = null;
    }
    if (line.trimStart().startsWith('#')) continue;
    // Mask quoted values while retaining quoted mapping keys. Text in a
    // step name or environment value is not an action reference.
    const keys = line.replace(/"(?:\\.|[^"\\])*"|'(?:''|[^'])*'/g,
      (value, offset) => /^\s*:/.test(line.slice(offset + value.length)) ? value : ' '.repeat(value.length));
    if (/(?:^\s*(?:-\s*)?|:\s*|[{,]\s*)[&*!]/.test(keys) ||
        /(?:^\s*(?:-\s*)?|[{,]\s*)["'][^\n]*["']\s*:/.test(keys)) {
      throw new Error(`${file}: YAML anchors, aliases, tags and quoted mapping keys are unsupported; use ordinary block mappings`);
    }
    const key = /(?:^\s*(?:-\s*)?|[{,]\s*)(?:uses|["']uses["'])\s*:/.test(keys);
    if (!key && /^\s*(?:-\s*)?[^:]+:\s*[|>][0-9+-]*(?:\s+#.*)?\s*$/.test(line)) {
      scalarIndent = indent;
      continue;
    }
    if (!key) continue;
    const action = line.match(/^\s*(?:-\s*)?uses:\s*["']?([^\s"'#]+)/)?.[1];
    if (!action) throw new Error(`${file}: uses must be an unquoted block mapping key`);
    if (!action.startsWith('./') && !/^[^@]+@[0-9a-f]{40}$/.test(action)) {
      throw new Error(`${file}: action must be pinned: ${action}`);
    }
  }
}

export const rustFormatNeeded = changed => changed.some(file => file.endsWith('.rs') ||
  /(?:^|\/)\.?rustfmt\.toml$/.test(file) || file === 'rust-toolchain.toml');
