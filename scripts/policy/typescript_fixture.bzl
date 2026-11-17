"""Stage declared TypeScript, protobuf and Wasm inputs for the shared Node test."""

def _fixture_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name)
    sources = ctx.files.srcs
    npm = ctx.files.npm
    roots = [f.path.split("/node_modules/")[0] for f in npm if "/node_modules/" in f.path]
    if not roots:
        fail("npm package inputs are empty")
    commands = [
        'set -eu',
        'root="$PWD"',
        'out="$root/%s"' % output.path,
        'mkdir -p "$out/node_modules"',
        'cp -R "$root/%s/node_modules/." "$out/node_modules/"' % roots[0],
    ]
    for f in sources:
        commands.extend([
            'mkdir -p "$out/$(dirname \'%s\')"' % f.short_path,
            'cp "$root/%s" "$out/%s"' % (f.path, f.short_path),
        ])
    for package, wasm in [("auth", ctx.file.auth), ("machines", ctx.file.machines)]:
        commands.extend([
            'mkdir -p "$out/typescript/packages/%s/generated/wasm"' % package,
            'cp -R "$root/%s/." "$out/typescript/packages/%s/generated/wasm/"' % (wasm.path, package),
        ])
    commands.extend([
        'mkdir -p "$out/node_modules/@acyclic-labs"',
        'ln -s ../../typescript/packages/auth "$out/node_modules/@acyclic-labs/auth"',
        'printf \'version: v2\\nmodules:\\n  - path: compatibility/proto\\n  - path: rust/crates/machines/proto\\n\' > "$out/buf.yaml"',
        'printf \'version: v2\\nplugins:\\n  - local: ["%s", "%s"]\\n    out: generated/typescript\\n    strategy: all\\n    opt: [target=js+dts, import_extension=js]\\n\' "$root/%s" "$out/node_modules/@bufbuild/protoc-gen-es/bin/protoc-gen-es" > "$out/buf.gen.yaml"' % ("%s", "%s", ctx.executable.node.path),
        'cd "$out"',
        '"$out/node_modules/@bufbuild/buf-linux-x64/bin/buf" generate --path compatibility/proto/validation/v1/options.proto --path compatibility/proto/harness/v2/resources.proto --path rust/crates/machines/proto/machines/v1/machines.proto',
        'mkdir -p typescript/packages/machines/generated/proto',
        'cp -R generated/typescript/. typescript/packages/machines/generated/proto/',
        '"$root/%s" build --target=node --format=esm --external=\'*\' --outdir typescript/packages/auth/dist typescript/packages/auth/src/credential.ts typescript/packages/auth/src/credential-connect.ts typescript/packages/auth/src/wasm.ts' % ctx.executable.bun.path,
        '"$root/%s" build --target=node --format=esm --external=\'*\' --outdir typescript/packages/machines/dist typescript/packages/machines/src/grpc.ts typescript/packages/machines/src/transport.ts typescript/packages/machines/src/rpc-transport.ts typescript/packages/machines/src/wasm-runtime.ts typescript/packages/machines/src/owned-stream.ts' % ctx.executable.bun.path,
    ])
    ctx.actions.run_shell(
        inputs = depset(sources + npm + [ctx.file.auth, ctx.file.machines]),
        tools = [ctx.executable.node, ctx.executable.bun],
        outputs = [output],
        command = "\n".join(commands),
        mnemonic = "TypeScriptFixture",
    )
    return [DefaultInfo(files = depset([output]))]

typescript_fixture = rule(
    implementation = _fixture_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "npm": attr.label(allow_files = True, mandatory = True),
        "auth": attr.label(allow_single_file = True, mandatory = True),
        "machines": attr.label(allow_single_file = True, mandatory = True),
        "node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
        "bun": attr.label(default = "@bunjs//:bun", executable = True, cfg = "exec"),
    },
)
