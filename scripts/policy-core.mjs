import {execFileSync} from 'node:child_process';
import {existsSync, lstatSync, mkdtempSync, readFileSync, readdirSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';


export const repositoryUrl = new URL('../', import.meta.url).href;
export const repositoryRoot = resolve(fileURLToPath(repositoryUrl));

const utf8 = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true});
export function readUtf8(file) {
  const bytes = readFileSync(file);
  try { return utf8.decode(bytes); }
  catch (cause) { throw new Error(`${file}: invalid UTF-8`, {cause}); }
}

// Git paths are NUL-delimited, including names with newlines or quoting characters.
// Each policy owns its pathspecs; discovery never silently drops missing files.
/** @param {string} [root] @param {string[]} [paths] @param {import('node:child_process').ExecFileSyncOptions & {untracked?: boolean}} [options] */
export function trackedFiles(root = process.cwd(), paths = [], {untracked = false, ...options} = {}) {
  return execFileSync('git', ['ls-files', '-z', ...(untracked ? ['--cached', '--others', '--exclude-standard'] : []), '--', ...paths],
    {...options, cwd: root, encoding: 'utf8', maxBuffer: Infinity}).split('\0').filter(Boolean);
}

const manifests = new Map();
// Parsed tables are dictionaries: undeclared names never resolve through the
// JavaScript object prototype, including implicit Cargo feature identities.
export const parseJson = text => JSON.parse(text, (_, value) => value && typeof value === 'object' && !Array.isArray(value)
  ? Object.setPrototypeOf(value, null) : value);
function pythonJson(source, input) {
  try {
    return parseJson(execFileSync(process.platform === 'win32' ? 'python' : 'python3', ['-I', '-c', source],
      {input: JSON.stringify(input), encoding: 'utf8', maxBuffer: Infinity, stdio: ['pipe', 'pipe', 'pipe']}));
  } catch (error) {
    if (error.stderr?.length) throw new Error(error.stderr.toString().trim());
    throw error;
  }
}

export function parseToml(texts) {
  return pythonJson(`
import datetime, json, math, sys, tomllib
def encode(value):
    if isinstance(value, dict):
        return {key: encode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode(item) for item in value]
    if isinstance(value, (datetime.date, datetime.time)):
        return {"toml_type": type(value).__name__, "value": value.isoformat()}
    if isinstance(value, float) and not math.isfinite(value):
        return {"toml_type": "float", "value": str(value)}
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 9007199254740991:
        return {"toml_type": "integer", "value": str(value)}
    return value
documents = {}
for path, text in json.load(sys.stdin).items():
    try:
        documents[path] = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        sys.exit(f"{path}: {error}")
json.dump(encode(documents), sys.stdout, allow_nan=False)
`, texts);
}

export function tomlFiles(root, files) {
  const missing = files.filter(file => !manifests.has(resolve(root, file)));
  if (missing.length) {
    const parsed = parseToml(Object.fromEntries(missing.map(file => [file, readUtf8(resolve(root, file))])));
    for (const [file, manifest] of Object.entries(parsed)) manifests.set(resolve(root, file), manifest);
  }
  return Object.fromEntries(files.map(file => [file, manifests.get(resolve(root, file))]));
}

export const toml = (root, file) => tomlFiles(root, [file])[file];

// Expectations name actual parsed fields, independently of manifest formatting.
export function checkManifestFields(root, expectations) {
  const files = [...new Set(expectations.map(([file]) => file))];
  const parsed = tomlFiles(root, files.filter(file => file.endsWith('.toml')));
  for (const file of files.filter(file => !file.endsWith('.toml'))) parsed[file] = parseJson(readUtf8(resolve(root, file)));
  for (const [file, keys, expected] of expectations) {
    const actual = keys.reduce((value, key) => value?.[key], parsed[file]);
    if (actual !== expected) throw new Error(`${file}: ${keys.join('.')} must be ${JSON.stringify(expected)}; found ${JSON.stringify(actual)}`);
  }
}

// Evaluate the positive cfg formula directly, without expanding its Cartesian
// product of alternatives. A negated feature cannot be expressed by Cargo's
// positive required-features list, even inside a currently satisfied branch.
export function testGateEnabled(gate, enabled) {
  const tokens = gate.match(/"[^"\\]*(?:\\.[^"\\]*)*"|[A-Za-z_][A-Za-z_0-9]*|[(),=]/g) ?? [];
  let at = 0;
  const invalid = () => { throw new Error(`unsupported cfg gate: ${gate}`); };
  const parse = () => {
    const name = tokens[at++];
    if (!/^[A-Za-z_][A-Za-z_0-9]*$/.test(name ?? '')) invalid();
    let value, children;
    if (tokens[at] === '=') {
      at++;
      if (!tokens[at]?.startsWith('"')) invalid();
      value = tokens[at++].slice(1, -1);
    } else if (tokens[at] === '(') {
      at++;
      children = [];
      while (tokens[at] !== ')') {
        children.push(parse());
        if (tokens[at] === ',') at++;
        else if (tokens[at] !== ')') invalid();
      }
      at++;
    }
    const feature = name === 'feature' || Boolean(children?.some(child => child.feature));
    if (name === 'not' && feature) throw new Error('negated feature gate cannot be represented by required-features; move this gate inside the test target');
    if (['all', 'any', 'not'].includes(name) && !children) invalid();
    return {feature, enabled: name === 'feature' ? enabled.has(value)
      : ['loom', 'kani'].includes(name) ? enabled.has('model-checking')
      : name === 'all' ? children.every(child => child.enabled)
      : name === 'any' ? children.some(child => child.enabled) : true};
  };
  const result = parse();
  if (at !== tokens.length) invalid();
  return result.enabled;
}

export function testFeatureFindings(root) {
  const errors = [];
  const files = trackedFiles(root, ['*Cargo.toml', ':(exclude)scripts/tests/fixtures/**', ':(exclude)rust/vendor/**'])
    .filter(file => existsSync(resolve(root, file)));
  let documents;
  try { documents = tomlFiles(root, files); }
  catch (error) { return [error.message]; }
  for (const manifest of files) {
    try {
      const data = documents[manifest], directory = dirname(resolve(root, manifest));
      if (data.package === undefined) continue;
      if (!data.package || typeof data.package !== 'object' || Array.isArray(data.package)) throw new Error('invalid package table');
      const targets = new Map((data.test ?? []).map(target => {
        if (!target || typeof target.name !== 'string') throw new Error('invalid test target');
        return [target.path ?? `tests/${target.name}.rs`, target];
      }));
      if (data.package.autotests !== false && existsSync(join(directory, 'tests'))) {
        for (const name of readdirSync(join(directory, 'tests'))) {
          const path = `tests/${name}`;
          if (name.endsWith('.rs') && !targets.has(path)) targets.set(path, {});
          try {
            lstatSync(join(directory, path, 'main.rs'));
            if (!targets.has(`${path}/main.rs`)) targets.set(`${path}/main.rs`, {});
          } catch (error) { if (!['ENOENT', 'ENOTDIR'].includes(error.code)) throw error; }
        }
      }
      for (const [path, target] of targets) {
        const source = join(directory, path), text = readUtf8(source);
        const enabled = new Set(target['required-features'] ?? []), pending = [...enabled];
        while (pending.length) {
          const next = pending.pop();
          for (const feature of data.features?.[next] ?? []) {
            if (!enabled.has(feature)) { enabled.add(feature); pending.push(feature); }
          }
        }
        for (const [, gate] of text.matchAll(/#!\[cfg\((.*?)\)\]/gs)) {
          try {
            if (!testGateEnabled(gate, enabled)) errors.push(`${source}: add required-features satisfying (${gate}) to its [[test]] in ${manifest}`);
          } catch (error) { errors.push(`${source}: ${error.message}`); }
        }
      }
    } catch (error) { errors.push(`${manifest}: ${error.message}`); }
  }
  return errors;
}

// Starlark is a Python syntax subset. Parse syntax, never execute repository code.
// Literal pins and direct calls are mandatory: indirection cannot hide a fetch.
export function bazelGitPins(root) {
  const files = trackedFiles(root).filter(file => file.endsWith('MODULE.bazel') || file.endsWith('.bzl'));
  return pythonJson(`
import ast, json, re, sys
pins = []
repository_rules = {"git_repository", "new_git_repository"}
for file, source in json.load(sys.stdin).items():
    tree = ast.parse(source, filename=file)
    names = {"git_override"} | repository_rules
    bindings = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "load":
            for keyword in node.keywords:
                if isinstance(keyword.value, ast.Constant) and keyword.value.value in names:
                    names.add(keyword.arg)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Name) and call.func.id == "use_repo_rule":
                if len(call.args) != 2 or not isinstance(call.args[1], ast.Constant) or not isinstance(call.args[1].value, str):
                    sys.exit(f"{file}:{node.lineno}: Git rules require literal use_repo_rule names")
                if isinstance(call.args[1], ast.Constant) and call.args[1].value in repository_rules:
                    for target in node.targets:
                        if not isinstance(target, ast.Name):
                            sys.exit(f"{file}:{node.lineno}: Git rule binding must be a name")
                        names.add(target.id)
                        bindings.add(target)
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    for node in ast.walk(tree):
        name = node.id if isinstance(node, ast.Name) else node.attr if isinstance(node, ast.Attribute) else None
        if name not in names | {"use_repo_rule"} or node in bindings:
            continue
        call = parents.get(node)
        location = f"{file}:{node.lineno}"
        if not isinstance(call, ast.Call) or call.func is not node:
            sys.exit(f"{location}: Git rules must be called directly, not passed or aliased")
        if name == "use_repo_rule":
            if len(call.args) != 2 or not isinstance(call.args[1], ast.Constant) or not isinstance(call.args[1].value, str):
                sys.exit(f"{location}: Git rules require literal use_repo_rule names")
            if call.args[1].value in repository_rules and not isinstance(parents.get(call), ast.Assign):
                sys.exit(f"{location}: Git rules require a named use_repo_rule binding")
            continue
        fields = {keyword.arg: keyword.value for keyword in call.keywords}
        if call.args or None in fields or len(fields) != len(call.keywords):
            sys.exit(f"{location}: Git rules require explicit keyword arguments")
        pin = {"file": file, "line": node.lineno}
        for key in ("remote", "commit"):
            value = fields.get(key)
            if not isinstance(value, ast.Constant) or not isinstance(value.value, str) or not value.value:
                sys.exit(f"{location}: Git {key} must be a nonempty literal string")
            pin[key] = value.value
        if not re.fullmatch(r"[0-9a-fA-F]{40}", pin["commit"]):
            sys.exit(f"{location}: Git commit must be a full SHA-1")
        pins.append(pin)
json.dump(pins, sys.stdout)
`, Object.fromEntries(files.map(file => [file, readUtf8(resolve(root, file))])));
}

export function checkFetchableGitPins(root) {
  const pins = bazelGitPins(root), checked = new Set();
  const directory = mkdtempSync(resolve(tmpdir(), 'git-pins-'));
  try {
    for (const pin of pins) {
      const key = JSON.stringify([pin.remote, pin.commit]);
      if (checked.has(key)) continue;
      // No object from another declared remote may satisfy this fetch.
      const repository = mkdtempSync(resolve(directory, 'pin-'));
      const git = args => execFileSync('git', args, {cwd: repository, encoding: 'utf8',
        env: {...process.env, GIT_TERMINAL_PROMPT: '0', GCM_INTERACTIVE: 'never'}, stdio: ['ignore', 'pipe', 'pipe']}).trim();
      git(['init', '--bare', '-q']);
      try {
        git(['fetch', '--depth=1', '--no-tags', '--no-recurse-submodules', '--', pin.remote, pin.commit]);
        if (git(['rev-parse', 'FETCH_HEAD^{commit}']).toLowerCase() !== pin.commit.toLowerCase()) throw new Error('wrong commit');
      } catch {
        throw new Error(`${pin.file}:${pin.line}: Git commit ${pin.commit} is not fetchable from its declared remote`);
      }
      checked.add(key);
    }
  } finally {
    rmSync(directory, {recursive: true, force: true});
  }
  return checked.size;
}
