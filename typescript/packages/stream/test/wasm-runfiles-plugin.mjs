// Bazel's runfiles place workspace packages and generated wasm beside the source tree, not inside
// it. Redirect the package's relative wasm imports to the runfiles copy so the model runs the
// declared output.
const directory = process.env.ACYCLIC_STREAM_WASM_DIR;
const authWasm = process.env.ACYCLIC_AUTH_WASM_DIR;
Bun.plugin({
  name: "stream-wasm-runfiles",
  setup(build) {
    build.onResolve({ filter: /generated\/wasm\/(?:index|acyclic_stream_wasm)\.js$/ }, args => ({
      path: `${directory}/${args.path.slice(args.path.lastIndexOf("/") + 1)}`,
    }));
    build.onLoad({ filter: /typescript[\\/]packages[\\/]auth[\\/]src[\\/]wasm\.ts$/ }, async args => {
      const source = await Bun.file(args.path).text();
      return {
        contents: source
          .replaceAll("../generated/wasm/acyclic_auth_wasm.js", `${authWasm}/acyclic_auth_wasm.js`)
          .replaceAll("../generated/wasm/acyclic_auth_wasm_bg.wasm", `${authWasm}/acyclic_auth_wasm_bg.wasm`),
        loader: "ts",
      };
    });
  },
});
