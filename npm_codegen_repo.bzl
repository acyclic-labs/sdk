"""Pinned npm inputs for Bazel JavaScript generation, read from the workspace lock."""

_PACKAGES = {
    "@bufbuild/protobuf": "@bufbuild/protobuf",
    "@bufbuild/protoc-gen-es": "@bufbuild/protoc-gen-es",
    "@bufbuild/protoplugin": "@bufbuild/protoplugin",
    "@bufbuild/protoplugin/typescript": "@bufbuild/protoplugin/node_modules/typescript",
    "@connectrpc/connect": "@connectrpc/connect",
    "@connectrpc/connect-node": "@connectrpc/connect-node",
    "@typescript/vfs": "@typescript/vfs",
    "@types/node": "@types/node",
    "debug": "debug",
    "ignore": "ignore",
    "ms": "ms",
    "typescript": "typescript",
    "undici-types": "undici-types",
}

def _npm_codegen_repository_impl(ctx):
    lines = ctx.read(ctx.attr.lock).splitlines()
    for key, destination in _PACKAGES.items():
        matches = [line.strip()[:-1] for line in lines if line.strip().startswith('"%s": [' % key)]
        if len(matches) != 1:
            fail("expected exactly one lock entry for %s" % key)
        record = json.decode("{" + matches[0] + "}")[key]
        name, version = record[0].rsplit("@", 1)
        filename = name.split("/")[-1]
        ctx.download_and_extract(
            url = "https://registry.npmjs.org/%s/-/%s-%s.tgz" % (name, filename, version),
            integrity = record[3],
            stripPrefix = filename if name.startswith("@types/") else "package",
            output = "node_modules/" + destination,
        )
    patch = ctx.read(ctx.attr.connect_patch)
    prefix = "node_modules/@connectrpc/connect/dist/"
    patch = patch.replace("a/dist/", "a/" + prefix).replace("b/dist/", "b/" + prefix)
    ctx.file("connect.patch", patch)
    ctx.patch("connect.patch", strip = 1)
    ctx.file("BUILD.bazel", 'filegroup(name = "packages", srcs = glob(["node_modules/**"]), visibility = ["//visibility:public"])\n')
    return ctx.repo_metadata(reproducible = True)

npm_codegen_repository = repository_rule(
    implementation = _npm_codegen_repository_impl,
    attrs = {
        "lock": attr.label(default = Label("//:bun.lock"), allow_single_file = True),
        "connect_patch": attr.label(default = Label("//:patches/connect-2.3.0.patch"), allow_single_file = True),
    },
)
