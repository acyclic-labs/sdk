Object.defineProperty(process, "platform", { value: "linux" });
try {
  await import("../../../typescript/packages/actors/generated/native/binding.cjs");
  console.log(JSON.stringify({ unexpectedlyLoaded: true }));
  process.exitCode = 1;
} catch (error) {
  const chain = [];
  let current = error;
  for (let depth = 0; current && depth < 4; depth++) {
    chain.push({ name: current.name, code: current.code ?? null, firstLine: String(current.message).split(/\r?\n/)[0] });
    current = current.cause;
  }
  console.log(JSON.stringify({ scope: "actual generated loader with simulated Linux selector; not OS runtime qualification", chain }));
}