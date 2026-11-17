"""Cache real Rustdoc JSON as declared, independently keyed fixture actions."""

def _repository_path(path):
    return path.split("/", 2)[2] if path.startswith("../") else path

def _declared_toolchain(target):
    versions = {}
    for file in target[DefaultInfo].data_runfiles.files.to_list():
        parts = file.short_path.split("/rustup/toolchains/", 1)
        if len(parts) != 2:
            continue
        name = parts[1].split("/", 1)[0]
        if name.startswith("nightly-"):
            version = "-".join(name.split("-")[:4])
            versions[version] = True
    if len(versions) != 1:
        fail("the declared Rustup target must contain exactly one dated nightly")
    return versions.keys()[0]

def _rustdoc_fixture_impl(ctx):
    output = ctx.actions.declare_file(ctx.label.name + ".json")
    metadata = ctx.actions.declare_file(ctx.label.name + ".fixture.json")
    toolchain = _declared_toolchain(ctx.attr._rustup)
    ctx.actions.write(metadata, json.encode({
        "fixture_root": ctx.attr.fixture_root,
        "package": ctx.attr.package,
        "toolchain": toolchain,
        "cargo_flags": ctx.attr.cargo_flags,
        "sources": [_repository_path(file.short_path) for file in ctx.files.srcs],
        "output": _repository_path(output.short_path),
    }))
    args = ctx.actions.args()
    args.add(output)
    args.add(ctx.file._rustup)
    args.add(toolchain)
    args.add(ctx.attr.package)
    for file in ctx.files.srcs:
        path = _repository_path(file.short_path)
        if not path.startswith(ctx.attr.fixture_root):
            fail("fixture input is outside its declared root: " + path)
        args.add(file)
        args.add(path[len(ctx.attr.fixture_root):])
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
