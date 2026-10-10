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
    if (/^\s*(?:-\s*)?[^:]+:\s*[|>][0-9+-]*(?:\s+#.*)?\s*$/.test(line)) {
      scalarIndent = indent;
      continue;
    }
    const key = /(?:\buses|["']uses["'])\s*:/.test(line);
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
