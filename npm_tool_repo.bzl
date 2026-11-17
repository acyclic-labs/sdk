"""Resolve an executable npm archive from the workspace lock."""

def _npm_tool_impl(ctx):
    matches = [line.strip()[:-1] for line in ctx.read(ctx.attr.lock).splitlines() if line.strip().startswith('"%s": [' % ctx.attr.package)]
    if len(matches) != 1:
        fail("expected exactly one lock entry for %s" % ctx.attr.package)
    record = json.decode("{" + matches[0] + "}")[ctx.attr.package]
    name, version = record[0].rsplit("@", 1)
    ctx.download_and_extract(
        url = "https://registry.npmjs.org/%s/-/%s-%s.tgz" % (name, name.split("/")[-1], version),
        integrity = record[3],
        stripPrefix = "package",
    )
    ctx.file("BUILD.bazel", 'filegroup(name = "files", srcs = glob(["**"], exclude = ["BUILD.bazel"]), visibility = ["//visibility:public"])\n')

npm_tool_repository = repository_rule(
    implementation = _npm_tool_impl,
    attrs = {
        "package": attr.string(mandatory = True),
        "lock": attr.label(default = Label("//:bun.lock"), allow_single_file = True),
    },
)
