import {createHash} from 'node:crypto';
import {posix} from 'node:path';
import ts from 'typescript-parser';
import {pollingUnit, sameRustImportLayout, SourceSyntaxError, temporalModule, validateDispositions} from './index.mjs';

/** @typedef {{id: string, unit: string, edges: Set<Dependency>, hazardous: boolean, callable: boolean}} Dependency */
/** @typedef {{members: Dependency[], edges: Set<Component>, parents: Set<Component>, remaining: number, hazardous: boolean, fingerprint: string}} Component */
/** @typedef {{node: import('typescript-parser').Node, target?: Dependency, unresolved?: string}} Reference */
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const script = file => /\.(?:[cm]?[jt]s|[jt]sx)$/.test(file);

/** @param {import('typescript-parser').Node} node */
function runtimeModuleSpecifier(node) {
 let specifier;
 if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node))
  && !(ts.isImportDeclaration(node) ? node.importClause?.isTypeOnly : node.isTypeOnly)) specifier = node.moduleSpecifier;
 else if (ts.isImportEqualsDeclaration(node) && !node.isTypeOnly
  && ts.isExternalModuleReference(node.moduleReference)) specifier = node.moduleReference.expression;
 else if (ts.isCallExpression(node) && (node.expression.kind === ts.SyntaxKind.ImportKeyword
  || (ts.isIdentifier(node.expression) && node.expression.text === 'require'))) specifier = node.arguments[0];
 return specifier && ts.isStringLiteralLike(specifier) ? specifier : undefined;
}

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
   component.members.map(node => [node.id, node.unit]).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0),
   [...component.edges].map(child => child.fingerprint).sort(),
  ]);
  for (const parent of component.parents) if (!--parent.remaining) ready.push(parent);
 }
 if (ready.length !== components.length) throw new Error('source dependency components retain a cycle');
 return owner;
}

/** @param {(from: string, to: string) => void} [recordImport] */
function scriptProgram(files, units, recordImport = () => {}) {
 const scripts = [...units.keys()].map(file => '/' + file);
 const options = {
  allowJs: true, noLib: true, noResolve: false, noEmit: true,
  target: ts.ScriptTarget.Latest, module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Bundler, moduleDetection: ts.ModuleDetectionKind.Force,
 };
 const moduleCache = ts.createModuleResolutionCache('/', file => file, options);
 const resolveModule = (name, containingFile) => {
  if (!name.startsWith('.')) return undefined;
  const base = posix.resolve(posix.dirname(containingFile), name);
  if (units.has(base.slice(1))) return {resolvedFileName: base};
  const resolved = ts.resolveModuleName(name, containingFile, options, host, moduleCache).resolvedModule;
  return resolved && units.has(resolved.resolvedFileName.slice(1)) ? resolved : undefined;
 };
 /** @type {import('typescript-parser').CompilerHost} */
 const host = {
  getSourceFile: file => units.get(file.slice(1))?.parsed,
  getDefaultLibFileName: () => '/__no_default_lib.d.ts',
  writeFile() { throw new Error('source analysis must not emit files'); },
  getCurrentDirectory: () => '/', getDirectories: () => [],
  fileExists: file => files.has(file), readFile: file => files.get(file),
  getCanonicalFileName: file => file, useCaseSensitiveFileNames: () => true, getNewLine: () => '\n',
  resolveModuleNames: (names, containingFile) => names.map(name => {
   const resolved = resolveModule(name, containingFile);
   if (resolved) recordImport(containingFile.slice(1), resolved.resolvedFileName.slice(1));
   return resolved;
  }),
 };
 const program = ts.createProgram(scripts, options, host);
 return {program, resolveModule};
}

/** Resolve the input closure of each source group with the same virtual
 * compiler host as the polling graph. Group roots also include malformed and
 * declaration-only source, so their owning check still validates them.
 * @param {Map<string, string>} sources @param {Map<string, string[]>} partitions
 */
export function projectPollingInputClosures(sources, partitions) {
 const units = new Map(), files = new Map(), manifests = [];
 for (const [file, source] of sources) {
  if (/(?:^|\/)package\.json$/.test(file)) {
   manifests.push(file);
   // An invalid manifest is still an input of the owning check. It cannot
   // supply module resolution data to an earlier, historical snapshot.
   try { JSON.parse(source); }
   catch (error) { if (error instanceof SyntaxError) continue; throw error; }
   files.set('/' + file, source);
  } else if (script(file)) {
   const parsed = ts.createSourceFile('/' + file, source, ts.ScriptTarget.Latest, true);
   const diagnostics = /** @type {typeof parsed & {parseDiagnostics: readonly import('typescript-parser').Diagnostic[]}} */ (parsed).parseDiagnostics;
   if (parsed.isDeclarationFile || diagnostics.length) continue;
   units.set(file, {parsed});
   files.set('/' + file, source);
  }
 }
 const imports = new Map([...units.keys()].map(file => [file, new Set()]));
 const recordImport = (from, to) => imports.get(from)?.add(to);
 const {program, resolveModule} = scriptProgram(files, units, recordImport);
 // This pinned compiler API reads the result recorded by this program,
 // including each directive's resolution mode.
 const compiler = /** @type {typeof program & {
  getResolvedTypeReferenceDirectiveFromTypeReferenceDirective(reference: import('typescript-parser').FileReference,
   source: import('typescript-parser').SourceFile): import('typescript-parser').ResolvedTypeReferenceDirectiveWithFailedLookupLocations | undefined
 }} */ (program);
 const augmentations = new Set();
 let sharedScope = false;
 for (const source of program.getSourceFiles()) {
  const file = source.fileName.slice(1);
  // Ambient augmentations and explicit compiler references can affect symbol
  // resolution outside the ordinary module import graph.
  if (source.referencedFiles.length || source.moduleName || source.typeReferenceDirectives.some(reference =>
   compiler.getResolvedTypeReferenceDirectiveFromTypeReferenceDirective(reference, source)?.resolvedTypeReferenceDirective))
   sharedScope = true;
  const pending = [source];
  while (pending.length) {
   const node = pending.pop();
   if (ts.isModuleDeclaration(node) &&
    ((node.flags & ts.NodeFlags.GlobalAugmentation) || ts.isStringLiteral(node.name))) augmentations.add(file);
   const specifier = runtimeModuleSpecifier(node);
   const target = specifier && resolveModule(specifier.text, source.fileName);
   if (target) recordImport(file, target.resolvedFileName.slice(1));
   ts.forEachChild(node, child => { pending.push(child); });
  }
 }
 return new Map([...partitions].map(([name, roots]) => {
  if (sharedScope) return [name, [...sources.keys()]];
  const selected = new Set(manifests), pending = [...roots, ...augmentations];
  while (pending.length) {
   const file = pending.pop();
   if (selected.has(file)) continue;
   selected.add(file);
   for (const dependency of imports.get(file) ?? []) pending.push(dependency);
  }
  // Retain the original source order for identical analyzer inputs.
  return [name, [...sources.keys()].filter(file => selected.has(file))];
 }));
}

/** @param {import('typescript-parser').Node} call */
function awaitedExportNames(call) {
 let value = call;
 while (ts.isParenthesizedExpression(value.parent)) value = value.parent;
 if (!ts.isAwaitExpression(value.parent)) return undefined;
 value = value.parent;
 while (ts.isParenthesizedExpression(value.parent)) value = value.parent;
 const use = value.parent;
 if (ts.isExpressionStatement(use)) return [];
 if (ts.isPropertyAccessExpression(use) && use.expression === value) return [use.name.text];
 if (ts.isElementAccessExpression(use) && use.expression === value && ts.isStringLiteralLike(use.argumentExpression))
  return [use.argumentExpression.text];
 if (!ts.isVariableDeclaration(use) || use.initializer !== value || !ts.isObjectBindingPattern(use.name)) return undefined;
 const names = [];
 for (const element of use.name.elements) {
  const name = element.propertyName ?? element.name;
  if (element.dotDotDotToken || !(ts.isIdentifier(name) || ts.isStringLiteralLike(name))) return undefined;
  names.push(name.text);
 }
 return names;
}

/** @param {Map<string, string>} sources */
export function projectPollingSites(sources, {historical = false} = {}) {
 /** @type {Map<string, ReturnType<typeof pollingUnit>>} */
 const units = new Map();
 /** @type {ReturnType<typeof pollingUnit>['sites']} */
 const sites = [];
 const errors = [];
 const workspaceNames = new Set();
 const files = new Map();
 for (const [file, source] of sources) {
  let unit;
  try {
   unit = pollingUnit(file, source, ts);
   if (/(?:^|\/)package\.json$/.test(file)) {
    let manifest;
    try { manifest = JSON.parse(source); }
    catch (error) {
     if (!(error instanceof SyntaxError)) throw error;
     throw new SourceSyntaxError(`${file}: ${error.message}`);
    }
    if (typeof manifest?.name === 'string') workspaceNames.add(manifest.name);
    files.set('/' + file, source);
   }
  }
  catch (error) {
   // An unparseable predecessor cannot authorize any current executable site.
   // Current source is always strict, including unchanged malformed files.
   if (historical && error instanceof SourceSyntaxError) continue;
   errors.push(error);
   continue;
  }
  for (const site of unit.sites) sites.push(site);
  if (script(file) && !unit.parsed?.isDeclarationFile) { units.set(file, unit); files.set('/' + file, source); }
 }
 if (errors.length === 1) throw errors[0];
 if (errors.length) throw new AggregateError(errors, errors.map(error => error.message).join('\n'));
 if (!units.size) return sites;
 const {program, resolveModule} = scriptProgram(files, units);
 const checker = program.getTypeChecker();
 const localModule = name => name.startsWith('.') || workspaceNames.has(name.split('/').slice(0, name.startsWith('@') ? 2 : 1).join('/'));
 /** @type {Map<string, Dependency>} */
 const dependencies = new Map();
 /** @type {import('typescript-parser').Symbol[]} */
 const symbolQueue = [];
 /** @type {Map<import('typescript-parser').Symbol, Dependency>} */
 const symbols = new Map();
 const indices = new Map([...units].map(([file, unit]) => [file, new Map(unit.stream.map((token, index) => [token.start, index]))]));

 /** @type {Map<string, Map<number, import('typescript-parser').Symbol>>} */
 const moduleLoads = new Map();
 for (const [file, unit] of units) {
  for (const token of unit.stream) {
   if (token.kind !== 'module-load') continue;
   const resolved = resolveModule(token.value, '/' + file);
   const imported = resolved && program.getSourceFile(resolved.resolvedFileName);
   if (!imported || !/\.m[jt]s$/.test(imported.fileName)) continue;
   const symbol = checker.getSymbolAtLocation(imported);
   if (!symbol || !(symbol.flags & ts.SymbolFlags.Module)) continue;
   if (!moduleLoads.has(file)) moduleLoads.set(file, new Map());
   required(moduleLoads, file).set(token.start, symbol);
  }
 }
 const modeled = site => site.kind === 'indirect-timer-or-call' && moduleLoads.get(site.file)?.has(site.start);

 // A top-level function owns its lexical context, including all local bindings.
 // Compute its token identity once even when many symbols refer into that scope.
 const opaqueFiles = new Set([...units].filter(([file, unit]) => unit.stream.some(token =>
  token.kind === 'indirection' || (token.kind === 'module-load' && !moduleLoads.get(file)?.has(token.start))
  || (token.kind === 'word'
   && ['globalThis', 'global', 'window', 'self'].includes(token.value)))).map(([file]) => file));
 const functions = new Map();
 for (const source of program.getSourceFiles()) {
  const file = source.fileName.slice(1), unit = required(units, file), index = required(indices, file);
  if (opaqueFiles.has(file)) continue;
  for (const node of source.statements) {
   if (!ts.isFunctionDeclaration(node)) continue;
   const start = index.get(node.getStart(source));
   if (start === undefined) continue;
   let end = start;
   while (end < unit.stream.length && unit.stream[end].start < node.end) end++;
   functions.set(node, {start, position: file + ':function:' + (node.name?.text ?? 'default'),
    unit: digest(unit.stream.slice(start, end).map(token => [token.kind, token.value]))});
  }
 }
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
   while (original.parent && !ts.isImportDeclaration(original) && !ts.isExportDeclaration(original)
    && !ts.isImportEqualsDeclaration(original)) original = original.parent;
   const specifier = runtimeModuleSpecifier(original);
   if (specifier) return specifier.text;
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
  const identities = declarations.map(node => {
   const file = node.getSourceFile().fileName.slice(1);
   const source = required(units, file);
   const index = required(indices, file).get(node.getStart());
   for (let parent = /** @type {import('typescript-parser').Node} */ (node); parent; parent = parent.parent) {
    const scope = functions.get(parent);
    if (!scope) continue;
    if (parent === node) return scope;
    const local = index === undefined ? digest(node.getText()) : index - scope.start;
    return {position: scope.position + ':local:' + node.kind + ':' + local, unit: scope.unit};
   }
   if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name) && node.initializer
    && ts.isVariableDeclarationList(node.parent) && (node.parent.flags & ts.NodeFlags.Const)
    && ts.isVariableStatement(node.parent.parent) && ts.isSourceFile(node.parent.parent.parent)) {
    const value = node.initializer;
    // A primitive const cannot be mutated through an alias. Other initializers
    // retain their file context until the graph models their writes and effects.
    if (ts.isStringLiteralLike(value) || ts.isNumericLiteral(value) || ts.isBigIntLiteral(value)
     || value.kind === ts.SyntaxKind.TrueKeyword || value.kind === ts.SyntaxKind.FalseKeyword
     || value.kind === ts.SyntaxKind.NullKeyword)
     return {position: file + ':const:' + node.name.text, unit: digest([value.kind, value.getText()])};
   }
   // Type-only JSDoc bindings and empty module declarations have no executable token.
   return {position: file + ':' + node.kind + ':' + (index ?? digest(node.getText())), unit: source.fingerprint};
  });
  // A named default export and its local spelling can be distinct symbols for
  // the same declaration. Declaration identity joins them without joining names
  // in unrelated lexical scopes or source files.
  const value = dependency(identities.map(item => item.position).sort().join('|'),
   digest(identities.map(item => item.unit).sort()));
  if (module && temporalModule(module)) value.hazardous = true;
  symbols.set(symbol, value);
  symbolQueue.push(symbol);
  return value;
 };
 const roots = new Map(program.getSourceFiles().map(source => {
  const file = source.fileName.slice(1);
  const unit = required(units, file);
  const bodies = source.statements.flatMap(statement =>
   ts.isFunctionDeclaration(statement) && statement.body
    ? [{start: statement.body.getStart(source), end: statement.body.end}] : []);
  let fingerprint = unit.fingerprint;
  if (bodies.length && !opaqueFiles.has(file)) {
   // Initialization calls carry function bodies through their dependency edges.
   let body = 0;
   const initialized = unit.stream.filter(token => {
    while (body < bodies.length && token.start >= bodies[body].end) body++;
    return body === bodies.length || token.start < bodies[body].start;
   });
   fingerprint = digest(initialized.map(token => [token.kind, token.value]));
  }
  return [source.fileName, dependency(source.fileName + ':initialization', fingerprint)];
 }));
 /** @type {Reference[]} */
 const references = [];
 /** @type {{node: import('typescript-parser').Node, owner: Dependency, target: Dependency}[]} */
 const initializers = [];
 for (const source of program.getSourceFiles()) {
  const file = source.fileName.slice(1);
  const unit = required(units, file);
  const hazardPositions = new Set(unit.sites.filter(site => !modeled(site)).map(site => site.start));
  /** @type {{node: import('typescript-parser').Node, owner: Dependency}[]} */
  const stack = [{node: source, owner: required(roots, source.fileName)}];
  while (stack.length) {
   const {node, owner: parentOwner} = /** @type {{node: import('typescript-parser').Node, owner: Dependency}} */ (stack.pop());
   if (node.kind >= ts.SyntaxKind.FirstTypeNode && node.kind <= ts.SyntaxKind.LastTypeNode
    || ts.isTypeAliasDeclaration(node) || ts.isInterfaceDeclaration(node)) continue;
   let owner = parentOwner;
   const specifier = runtimeModuleSpecifier(node);
   if (specifier) {
    const resolved = resolveModule(specifier.text, source.fileName);
    if (resolved) initializers.push({node: specifier, owner, target: required(roots, resolved.resolvedFileName)});
    else if (localModule(specifier.text)) references.push({node: specifier, unresolved: specifier.text});
    const namespace = moduleLoads.get(file)?.get(node.getStart(source));
    if (namespace) {
     // A returned namespace can escape. Only a direct selection proves which
     // exports are used, and an exported `then` can run on promise resolution.
     const members = checker.getExportsOfModule(namespace);
     const names = members.some(member => member.getName() === 'then') ? undefined : awaitedExportNames(node);
     const selected = names?.map(name => symbolDependency(members.find(member => member.getName() === name)));
     const loaded = selected?.every(Boolean) ? selected : [symbolDependency(namespace)];
     for (const target of loaded) {
      if (!target) throw new Error('missing loaded module dependency: ' + specifier.text);
      owner.edges.add(target);
      references.push({node: specifier, target});
     }
    }
   }
   if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node) || ts.isImportEqualsDeclaration(node)) continue;
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
   fingerprint: digest([index, required(units, file).fingerprint, unresolved ?? component?.fingerprint]),
   dependency: unresolved ?? component?.fingerprint, start: node.getStart()});
 }
 for (const {node, target} of initializers) {
  const component = required(components, target);
  if (!component.hazardous) continue;
  const {file, index} = nodePosition(node);
  sites.push({file, line: node.getSourceFile().getLineAndCharacterOfPosition(node.getStart()).line + 1,
   kind: 'polling-module-initialization', fingerprint: digest([index, required(units, file).fingerprint, component.fingerprint]),
   dependency: component.fingerprint, start: node.getStart()});
 }
 return sites.filter(site => !modeled(site));
}

/** @param {Map<string, string>} before @param {Map<string, string>} after */
export function newProjectPollingSites(before, after, dispositions = [], policy = {}) {
 validateDispositions(dispositions, policy);
 const candidates = projectPollingSites(after);
 if (!candidates.length) return candidates;
 const retained = new Map();
 const historical = new Map(before);
 for (const file of new Set(candidates.map(site => site.file))) {
  const previous = before.get(file), current = after.get(file);
  if (previous !== undefined && current !== undefined && sameRustImportLayout(file, previous, current))
   historical.set(file, current);
 }
 for (const site of projectPollingSites(historical, {historical: true})) retained.set(site.file + ':' + site.fingerprint, (retained.get(site.file + ':' + site.fingerprint) ?? 0) + 1);
 for (const item of dispositions) retained.set(item.file + ':' + item.fingerprint, Math.max(1, retained.get(item.file + ':' + item.fingerprint) ?? 0));
 return candidates.filter(site => {
  const key = site.file + ':' + site.fingerprint;
  const count = retained.get(key) ?? 0;
  if (count) { retained.set(key, count - 1); return false; }
  return true;
 });
}
