// Bazel's runfiles place the generated wasm beside the source tree, not inside it. Redirect the
// package's relative wasm imports to the runfiles copy so the model runs the declared output.
const directory = process.env.ACYCLIC_STREAM_WASM_DIR;
Bun.plugin({
  name: "stream-wasm-runfiles",
  setup(build) {
    build.onResolve({ filter: /generated\/wasm\/(?:index|acyclic_stream_wasm)\.js$/ }, args => ({
      path: `${directory}/${args.path.slice(args.path.lastIndexOf("/") + 1)}`,
    }));
  },
});
