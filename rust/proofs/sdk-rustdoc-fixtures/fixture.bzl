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
    sources = []
    for file in ctx.files.srcs:
        path = _repository_path(file.short_path)
        if not path.startswith(ctx.attr.fixture_root):
            fail("fixture input is outside its declared root: " + path)
        sources.append((file, path[len(ctx.attr.fixture_root):]))
    if ctx.file.override:
        sources = [(file, relative) for file, relative in sources if relative != ctx.attr.override_path]
        sources.append((ctx.file.override, ctx.attr.override_path))
    ctx.actions.write(metadata, json.encode({
        "package": ctx.attr.package,
        "toolchain": toolchain,
        "cargo_flags": ctx.attr.cargo_flags,
        "sources": [{"file": _repository_path(file.short_path), "relative": relative} for file, relative in sources],
        "output": _repository_path(output.short_path),
    }))
    args = ctx.actions.args()
    args.add(output)
    args.add(ctx.file._rustup)
    args.add(toolchain)
    args.add(ctx.attr.package)
    for file, relative in sources:
        args.add(file)
        args.add(relative)
    args.add("--")
    args.add_all(ctx.attr.cargo_flags)
    compiler = ctx.attr._rustup[DefaultInfo]
    ctx.actions.run(
        executable = ctx.executable._producer,
        arguments = [args],
        inputs = depset([file for file, _ in sources] + [ctx.file._rustup], transitive = [compiler.data_runfiles.files]),
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
        "override": attr.label(allow_single_file = True),
        "override_path": attr.string(),
        "package": attr.string(mandatory = True),
        "cargo_flags": attr.string_list(),
        "_producer": attr.label(default = ":producer", executable = True, cfg = "exec"),
        "_rustup": attr.label(default = "@sdk_compilers//:rustup", allow_single_file = True, cfg = "exec"),
    },
)

def rustdoc_fixture_set(srcs, fixture_root, package, profiles, override = None, override_path = ""):
    for name, flags in profiles.items():
        rustdoc_fixture(
            name = name,
            srcs = srcs,
            fixture_root = fixture_root,
            package = package,
            override = override,
            override_path = override_path,
            cargo_flags = flags,
        )
