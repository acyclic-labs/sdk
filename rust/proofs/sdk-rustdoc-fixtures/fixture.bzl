"""Cache real Rustdoc JSON as declared, independently keyed fixture actions."""

def _rustdoc_fixture_impl(ctx):
    output = ctx.actions.declare_file(ctx.label.name + ".json")
    args = ctx.actions.args()
    args.add(output)
    args.add(ctx.file._rustup)
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
    return [DefaultInfo(files = depset([output]))]

rustdoc_fixture = rule(
    implementation = _rustdoc_fixture_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True, mandatory = True),
        "cargo_flags": attr.string_list(),
        "_producer": attr.label(default = ":producer", executable = True, cfg = "exec"),
        "_rustup": attr.label(default = "@sdk_compilers//:rustup", allow_single_file = True, cfg = "exec"),
    },
)
