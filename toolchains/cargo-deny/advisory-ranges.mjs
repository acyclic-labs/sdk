// RustSec version arrays consist of TOML strings and comments. Remove comments
// only outside strings before interpreting the values.
export function ranges(section, key, name) {
  const match = new RegExp(`^\\s*${key}\\s*=\\s*\\[([^\\]]*)\\]`, 'm').exec(section);
  if (!match) {
    if (new RegExp(`^\\s*${key}\\s*=`, 'm').test(section)) throw Error(`invalid versions.${key}: ${name}`);
    return [];
  }
  const input = match[1].replace(/"[^"\\]*"|#[^\n]*/g, token => token.startsWith('#') ? '' : token);
  const values = [...input.matchAll(/"([^"\\]*)"/g)].map(item => item[1]);
  if (input.replace(/"[^"\\]*"|[\s,]/g, '') !== '') throw Error(`invalid versions.${key}: ${name}`);
  return values;
}
