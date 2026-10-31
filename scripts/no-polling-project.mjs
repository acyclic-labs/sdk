import {createHash} from 'node:crypto';
import {posix} from 'node:path';
import ts from 'typescript-parser';
import {pollingUnit, temporalModule, validateDispositions} from './no-polling.mjs';

/** @typedef {{id: string, unit: string, edges: Set<Dependency>, hazardous: boolean, callable: boolean}} Dependency */
/** @typedef {{members: Dependency[], edges: Set<Component>, parents: Set<Component>, remaining: number, hazardous: boolean, fingerprint: string}} Component */
/** @typedef {{node: import('typescript-parser').Node, target?: Dependency, unresolved?: string}} Reference */
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const script = file => /\.(?:[cm]?[jt]s|[jt]sx)$/.test(file);

/** @template K, V @param {Map<K, V>} map @param {K} key @returns {V} */
function required(map, key) {
 const value = map.get(key);
 if (value === undefined) throw new Error('missing source dependency: ' + String(key));
 return value;
}

// Collapse recursion before hashing dependencies. Each edge is visited a fixed
// number of times; neither recursion depth nor a readiness iteration is needed.
/** @param {Dependency[]} nodes */
function dependencyComponents(nodes) {
 /** @type {Map<Dependency, Set<Dependency>>} */
 const reverse = new Map();
 for (const node of nodes) reverse.set(node, new Set());
 for (const node of nodes) for (const target of node.edges) required(reverse, target).add(node);
 const seen = new Set();
 const ordered = [];
 for (const root of nodes) {
  if (seen.has(root)) continue;
  seen.add(root);
  const stack = [{node: root, edges: root.edges.values()}];
  while (stack.length) {
   const frame = stack[stack.length - 1];
   const edge = frame.edges.next();
   if (edge.done) { ordered.push(frame.node); stack.pop(); }
   else if (!seen.has(edge.value)) {
    seen.add(edge.value);
    stack.push({node: edge.value, edges: edge.value.edges.values()});
   }
  }
 }
 /** @type {Map<Dependency, Component>} */
 const owner = new Map();
 /** @type {Component[]} */
 const components = [];
 for (const root of ordered.reverse()) {
  if (owner.has(root)) continue;
  /** @type {Component} */
  const component = {members: [], edges: new Set(), parents: new Set(), remaining: 0, hazardous: false, fingerprint: ''};
  components.push(component);
  const stack = [root];
  owner.set(root, component);
  while (stack.length) {
   const node = /** @type {Dependency} */ (stack.pop());
   component.members.push(node);
   for (const parent of required(reverse, node)) {
    if (!owner.has(parent)) { owner.set(parent, component); stack.push(parent); }
   }
  }
 }
 for (const node of nodes) {
  const component = required(owner, node);
  for (const target of node.edges) {
   const dependency = required(owner, target);
   if (component !== dependency) { component.edges.add(dependency); dependency.parents.add(component); }
  }
 }
 const ready = [];
 for (const component of components) {
  component.remaining = component.edges.size;
  if (!component.remaining) ready.push(component);
 }
 for (let at = 0; at < ready.length; at++) {
  const component = ready[at];
  component.hazardous = component.members.some(node => node.hazardous || (node.callable && (component.members.length > 1 || node.edges.has(node))))
   || [...component.edges].some(child => child.hazardous);
  component.fingerprint = digest([
   component.members.map(node => [node.id, node.unit]).sort((a, b) => a[0].localeCompare(b[0])),
   [...component.edges].map(child => child.fingerprint).sort(),
  ]);
  for (const parent of component.parents) if (!--parent.remaining) ready.push(parent);
 }
 if (ready.length !== components.length) throw new Error('source dependency components retain a cycle');
 return owner;
}

/** @param {Map<string, string>} sources */
export function projectPollingSites(sources) {
 /** @type {Map<string, ReturnType<typeof pollingUnit>>} */
 const units = new Map();
 /** @type {ReturnType<typeof pollingUnit>['sites']} */
 const sites = [];
 for (const [file, source] of sources) {
  const unit = pollingUnit(file, source);
  for (const site of unit.sites) sites.push(site);
  if (script(file)) units.set(file, unit);
 }
 const scripts = new Map([...sources].filter(([file]) => script(file)).map(([file, source]) => ['/' + file, source]));
 const resolveModule = (name, containingFile) => {
  if (!name.startsWith('.')) return undefined;
  const base = posix.resolve(posix.dirname(containingFile), name);
  const stem = base.replace(/\.(?:[cm]?js|jsx)$/, '');
  for (const candidate of [base, stem + '.ts', stem + '.tsx', stem + '.mts', stem + '.cts', base + '.ts', base + '.js', base + '.mjs', base + '.cjs', base + '/index.ts', base + '/index.js', base + '/index.mjs']) {
   if (scripts.has(candidate)) return {resolvedFileName: candidate};
  }
  return undefined;
 };
 /** @type {import('typescript-parser').CompilerHost} */
 const host = {
  getSourceFile: file => units.get(file.slice(1))?.parsed,
  getDefaultLibFileName: () => '/__no_default_lib.d.ts',
  writeFile() { throw new Error('source analysis must not emit files'); },
  getCurrentDirectory: () => '/', getDirectories: () => [],
  fileExists: file => scripts.has(file), readFile: file => scripts.get(file),
  getCanonicalFileName: file => file, useCaseSensitiveFileNames: () => true, getNewLine: () => '\n',
  resolveModuleNames: (names, containingFile) => names.map(name => resolveModule(name, containingFile)),
 };
 const program = ts.createProgram([...scripts.keys()], {
  allowJs: true, noLib: true, noResolve: false, noEmit: true,
  target: ts.ScriptTarget.Latest, module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Bundler, moduleDetection: ts.ModuleDetectionKind.Force,
 }, host);
 const checker = program.getTypeChecker();
 const workspaceNames = new Set();
 for (const [file, source] of sources) {
  if (!/^typescript\/packages\/[^/]+\/package\.json$/.test(file)) continue;
  const name = JSON.parse(source).name;
  if (typeof name === 'string') workspaceNames.add(name);
 }
 const localModule = name => name.startsWith('.') || workspaceNames.has(name.split('/').slice(0, name.startsWith('@') ? 2 : 1).join('/'));
 /** @type {Map<string, Dependency>} */
 const dependencies = new Map();
 /** @type {import('typescript-parser').Symbol[]} */
 const symbolQueue = [];
 /** @type {Map<import('typescript-parser').Symbol, Dependency>} */
 const symbols = new Map();
 const indices = new Map([...units].map(([file, unit]) => [file, new Map(unit.stream.map((token, index) => [token.start, index]))]));
 /** @param {import('typescript-parser').Node} node */
 const nodePosition = node => {
  const file = node.getSourceFile().fileName.slice(1);
  const index = indices.get(file)?.get(node.getStart());
  if (index === undefined) throw new Error('missing executable token position in ' + file);
  return {file, index};
 };
 const dependency = (id, unit) => {
  const previous = dependencies.get(id);
  if (previous) return previous;
  /** @type {Dependency} */
  const value = {id, unit, edges: new Set(), hazardous: false, callable: false};
  dependencies.set(id, value);
  return value;
 };
 /** @param {import('typescript-parser').Symbol | undefined} symbol */
 const canonical = symbol => symbol && (symbol.flags & ts.SymbolFlags.Alias) ? checker.getAliasedSymbol(symbol) : symbol;
 /** @param {import('typescript-parser').Symbol | undefined} symbol */
 const aliasModule = symbol => {
  if (!symbol || !(symbol.flags & ts.SymbolFlags.Alias)) return undefined;
  for (const declaration of symbol.declarations ?? []) {
   /** @type {import('typescript-parser').Node} */
   let original = declaration;
   while (original.parent && !ts.isImportDeclaration(original) && !ts.isExportDeclaration(original)) original = original.parent;
   if ((ts.isImportDeclaration(original) || ts.isExportDeclaration(original))
    && original.moduleSpecifier && ts.isStringLiteralLike(original.moduleSpecifier)) return original.moduleSpecifier.text;
  }
  return undefined;
 };
 /** @param {import('typescript-parser').Symbol | undefined} symbol @returns {Dependency | undefined} */
 const symbolDependency = symbol => {
  const module = aliasModule(symbol);
  const resolved = canonical(symbol);
  if (resolved?.declarations?.length) symbol = resolved;
  else if (!module || !temporalModule(module)) return undefined;
  if (!symbol || !symbol.declarations?.length) return undefined;
  if (symbols.has(symbol)) return required(symbols, symbol);
  const declarations = symbol.declarations.filter(node => units.has(node.getSourceFile().fileName.slice(1)));
  if (!declarations.length) return undefined;
  const positions = declarations.map(node => {
   const file = node.getSourceFile().fileName.slice(1);
   const index = indices.get(file)?.get(node.getStart());
   // Type-only JSDoc bindings and empty module declarations have no executable
   // token. Keep their identity in the graph rather than dropping the binding.
   return file + ':' + node.kind + ':' + (index ?? digest(node.getText()));
  }).sort();
  const unit = digest(declarations.map(node => required(units, node.getSourceFile().fileName.slice(1)).fingerprint).sort());
  // A named default export and its local spelling can be distinct symbols for
  // the same declaration. Declaration identity joins them without joining names
  // in unrelated lexical scopes or source files.
  const value = dependency(positions.join('|'), unit);
  if (module && temporalModule(module)) value.hazardous = true;
  symbols.set(symbol, value);
  symbolQueue.push(symbol);
  return value;
 };
 const roots = new Map([...scripts.keys()].map(file => [file, dependency(file + ':initialization', required(units, file.slice(1)).fingerprint)]));
 /** @type {Reference[]} */
 const references = [];
 /** @type {{node: import('typescript-parser').Node, owner: Dependency, target: Dependency}[]} */
 const initializers = [];
 for (const source of program.getSourceFiles()) {
  const file = source.fileName.slice(1);
  const unit = required(units, file);
  const hazardPositions = new Set(unit.sites.map(site => site.start));
  /** @type {{node: import('typescript-parser').Node, owner: Dependency}[]} */
  const stack = [{node: source, owner: required(roots, source.fileName)}];
  while (stack.length) {
   const {node, owner: parentOwner} = /** @type {{node: import('typescript-parser').Node, owner: Dependency}} */ (stack.pop());
   if (ts.isTypeNode(node)) continue;
   let owner = parentOwner;
   if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
    if (node.moduleSpecifier && ts.isStringLiteralLike(node.moduleSpecifier)
     && !(ts.isImportDeclaration(node) ? node.importClause?.isTypeOnly : node.isTypeOnly)) {
     const resolved = resolveModule(node.moduleSpecifier.text, source.fileName);
     if (resolved) initializers.push({node: node.moduleSpecifier, owner, target: required(roots, resolved.resolvedFileName)});
     else if (localModule(node.moduleSpecifier.text)) references.push({node: node.moduleSpecifier, unresolved: node.moduleSpecifier.text});
    }
    continue;
   }
   if ((ts.isFunctionDeclaration(node) || ts.isFunctionExpression(node) || ts.isMethodDeclaration(node)
    || ts.isGetAccessor(node) || ts.isSetAccessor(node) || ts.isClassDeclaration(node) || ts.isClassExpression(node)) && node.name) {
    owner = symbolDependency(checker.getSymbolAtLocation(node.name)) ?? owner;
    if (!ts.isClassDeclaration(node) && !ts.isClassExpression(node)) owner.callable = true;
    if (ts.isFunctionExpression(node) || ts.isClassExpression(node) || ts.isMethodDeclaration(node)
     || ts.isGetAccessor(node) || ts.isSetAccessor(node)) parentOwner.edges.add(owner);
   } else if (ts.isVariableDeclaration(node)) {
    if (ts.isIdentifier(node.name)) owner = symbolDependency(checker.getSymbolAtLocation(node.name)) ?? owner;
    else {
     // Escaping/destructured namespace values retain all possible dependencies.
     // A precise property reference uses its own resolved member symbol below.
     owner = dependency(file + ':pattern:' + nodePosition(node).index, unit.fingerprint);
     /** @type {import('typescript-parser').BindingName[]} */
     const names = [node.name];
     while (names.length) {
      const name = /** @type {import('typescript-parser').BindingName} */ (names.pop());
      if (ts.isIdentifier(name)) {
       const bound = symbolDependency(checker.getSymbolAtLocation(name));
       if (bound) bound.edges.add(owner);
      } else for (const element of name.elements) if (ts.isBindingElement(element)) names.push(element.name);
     }
    }
    if (node.initializer && !ts.isArrowFunction(node.initializer) && !ts.isFunctionExpression(node.initializer)) parentOwner.edges.add(owner);
   } else if (ts.isExportAssignment(node) || (ts.isFunctionDeclaration(node) && !node.name)) {
    const module = checker.getSymbolAtLocation(source);
    owner = module && symbolDependency(checker.getExportsOfModule(module).find(symbol => symbol.getName() === 'default')) || owner;
    if (ts.isFunctionDeclaration(node)) owner.callable = true;
    if (ts.isExportAssignment(node) && !ts.isArrowFunction(node.expression) && !ts.isFunctionExpression(node.expression)) parentOwner.edges.add(owner);
   }
   if (ts.isArrowFunction(node) || ts.isFunctionExpression(node) || ts.isConstructorDeclaration(node)) owner.callable = true;
   if (hazardPositions.has(node.getStart(source))) owner.hazardous = true;
   if (ts.isIdentifier(node)) {
    const parent = node.parent;
    const declarationName = 'name' in parent && parent.name === node && !ts.isPropertyAccessExpression(parent) && !ts.isShorthandPropertyAssignment(parent);
    const receiver = canonical(checker.getSymbolAtLocation(node));
    const resolvedReceiver = ts.isPropertyAccessExpression(parent) && parent.expression === node
     && receiver && (receiver.flags & ts.SymbolFlags.Module) && checker.getSymbolAtLocation(parent.name);
    if (!declarationName && !resolvedReceiver) {
     const symbol = ts.isShorthandPropertyAssignment(parent)
      ? checker.getShorthandAssignmentValueSymbol(parent) : checker.getSymbolAtLocation(node);
     const target = symbolDependency(symbol);
     if (target) { owner.edges.add(target); references.push({node, target}); }
     else if (symbol && (symbol.flags & ts.SymbolFlags.Alias)) {
      const original = aliasModule(symbol);
      if (original && localModule(original)) references.push({node, unresolved: original});
     }
    }
   }
   ts.forEachChild(node, child => { stack.push({node: child, owner}); });
  }
 }
 for (const item of initializers) item.owner.edges.add(item.target);
 for (let at = 0; at < symbolQueue.length; at++) {
  const symbol = symbolQueue[at];
  if (symbol.flags & ts.SymbolFlags.Module) {
   for (const exported of checker.getExportsOfModule(symbol)) {
    const target = symbolDependency(exported);
    if (target) required(symbols, symbol).edges.add(target);
   }
  }
 }
 const components = dependencyComponents([...dependencies.values()]);
 for (const {node, target, unresolved} of references) {
  const component = target && required(components, target);
  if (!unresolved && !component?.hazardous) continue;
  const {file, index} = nodePosition(node);
  sites.push({file, line: node.getSourceFile().getLineAndCharacterOfPosition(node.getStart()).line + 1,
   kind: unresolved ? 'unresolved-local-helper' : 'polling-helper-reference',
   fingerprint: digest([index, required(units, file).fingerprint, unresolved ?? component?.fingerprint]), start: node.getStart()});
 }
 for (const {node, target} of initializers) {
  const component = required(components, target);
  if (!component.hazardous) continue;
  const {file, index} = nodePosition(node);
  sites.push({file, line: node.getSourceFile().getLineAndCharacterOfPosition(node.getStart()).line + 1,
   kind: 'polling-module-initialization', fingerprint: digest([index, required(units, file).fingerprint, component.fingerprint]), start: node.getStart()});
 }
 return sites;
}

/** @param {Map<string, string>} before @param {Map<string, string>} after */
export function newProjectPollingSites(before, after, dispositions = []) {
 validateDispositions(dispositions);
 const retained = new Map();
 for (const site of projectPollingSites(before)) retained.set(site.file + ':' + site.fingerprint, (retained.get(site.file + ':' + site.fingerprint) ?? 0) + 1);
 for (const item of dispositions) retained.set(item.file + ':' + item.fingerprint, Math.max(1, retained.get(item.file + ':' + item.fingerprint) ?? 0));
 return projectPollingSites(after).filter(site => {
  const key = site.file + ':' + site.fingerprint;
  const count = retained.get(key) ?? 0;
  if (count) { retained.set(key, count - 1); return false; }
  return true;
 });
}
