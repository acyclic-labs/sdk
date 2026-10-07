const pkg = await import('@acyclic-labs/actors');
const publicKeys = Object.keys(pkg).sort();
const missingFactories = ['ActorId', 'CodeSha256', 'PositiveU64'].filter((key) => !(key in pkg));
let deepImport;
try {
  await import('@acyclic-labs/actors/generated/native/binding.cjs');
  deepImport = { ok: true };
} catch (error) {
  deepImport = { ok: false, code: error?.code, message: String(error?.message ?? error) };
}
console.log(JSON.stringify({ publicKeys, missingFactories, deepImport }));
