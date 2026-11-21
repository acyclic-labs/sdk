"""Write the checkout paths of the sources declared for a build script."""

def _source_manifest_impl(ctx):
    manifest = ctx.actions.declare_file(ctx.label.name + ".txt")
    paths = sorted({file.short_path: None for file in ctx.files.srcs})
    ctx.actions.write(manifest, "".join([path + "\n" for path in paths]))
    return [DefaultInfo(files = depset([manifest]))]

source_manifest = rule(
    implementation = _source_manifest_impl,
    attrs = {"srcs": attr.label_list()},
)
