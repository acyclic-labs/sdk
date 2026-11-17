"""Cache real Rustdoc JSON as declared, independently keyed fixture actions."""

def _rustdoc_fixture_impl(ctx):
    output = ctx.actions.declare_file(ctx.label.name + ".json")
    metadata = ctx.actions.declare_file(ctx.label.name + ".fixture.json")
    ctx.actions.write(metadata, json.encode({
        "fixture_root": ctx.attr.fixture_root,
        "package": ctx.attr.package,
        "cargo_flags": ctx.attr.cargo_flags,
        "sources": [file.short_path for file in ctx.files.srcs],
        "output": output.short_path,
    }))
    args = ctx.actions.args()
    args.add(output)
    args.add(ctx.file._rustup)
    args.add(ctx.attr.fixture_root)
    args.add(ctx.attr.package)
    args.add_all(ctx.files.srcs)
    args.add("--")
    args.add_all(ctx.attr.cargo_flags)
    compiler = ctx.attr._rustup[DefaultInfo]
    ctx.actions.run(
        executable = ctx.executable._producer,
        arguments = [args],
        inputs = depset(ctx.files.srcs + [ctx.file._rustup], transitive = [compiler.data_runfiles.files]),
        outputs = [output],
        mnemonic = "FixtureRustdoc",
        progress_message = "Building Rustdoc fixture %{label}",
    )
    return [DefaultInfo(files = depset([output, metadata]))]

rustdoc_fixture = rule(
    implementation = _rustdoc_fixture_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True, mandatory = True),
        "fixture_root": attr.string(mandatory = True),
        "package": attr.string(mandatory = True),
        "cargo_flags": attr.string_list(),
        "_producer": attr.label(default = ":producer", executable = True, cfg = "exec"),
        "_rustup": attr.label(default = "@sdk_compilers//:rustup", allow_single_file = True, cfg = "exec"),
    },
)

def rustdoc_fixture_set(srcs, fixture_root, package, profiles):
    for name, flags in profiles.items():
        rustdoc_fixture(
            name = name,
            srcs = srcs,
            fixture_root = fixture_root,
            package = package,
            cargo_flags = flags,
        )
