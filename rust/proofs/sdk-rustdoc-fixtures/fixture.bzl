"""Cache real Rustdoc JSON as declared, independently keyed fixture actions."""

load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain", "use_cpp_toolchain")
load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")

RustdocFixtureInfo = provider(fields = ["metadata"])

def _bundle_impl(ctx):
    index = ctx.actions.declare_file(ctx.label.name + ".index.json")
    ctx.actions.write(index, json.encode([fixture[RustdocFixtureInfo].metadata for fixture in ctx.attr.fixtures]))
    return [DefaultInfo(files = depset([index], transitive = [fixture[DefaultInfo].files for fixture in ctx.attr.fixtures]))]

rustdoc_fixture_bundle = rule(
    implementation = _bundle_impl,
    attrs = {"fixtures": attr.label_list(providers = [RustdocFixtureInfo])},
)

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
    artifacts = ctx.actions.declare_directory(ctx.label.name + ".artifacts") if ctx.attr.retain_artifacts else None
    toolchain = _declared_toolchain(ctx.attr._rustup)
    binary_name = ctx.attr.binary_name
    target_args = ["--bin", binary_name] if binary_name else ["--lib"]
    receipt_name = (binary_name or ctx.attr.library_name or ctx.attr.package).replace("-", "_")
    sources = []
    for file in ctx.files.srcs:
        path = _repository_path(file.short_path)
        if not path.startswith(ctx.attr.fixture_root):
            fail("fixture input is outside its declared root: " + path)
        sources.append((file, path[len(ctx.attr.fixture_root):]))
    for target, relative in ctx.attr.mapped_sources.items():
        files = target[DefaultInfo].files.to_list()
        if len(files) != 1:
            fail("mapped fixture input must name exactly one file")
        sources = [(file, existing) for file, existing in sources if existing != relative]
        sources.append((files[0], relative))
    metadata_value = {
        "package": ctx.attr.package,
        "toolchain": toolchain,
        "manifest_path": ctx.attr.manifest_path,
        "owner_path": ctx.attr.owner_path,
        "selected_lock": ctx.attr.selected_lock,
        "receipt_name": receipt_name,
        "target_args": target_args,
        "entrypoint": "src/main.rs" if binary_name else "src/lib.rs",
        "proc_macro": ctx.attr.proc_macro,
        "cargo_flags": ctx.attr.cargo_flags,
        "sources": [{"file": _repository_path(file.short_path), "relative": relative} for file, relative in sources],
        "output": _repository_path(output.short_path),
        "artifacts": _repository_path(artifacts.short_path) if artifacts else "",
    }
    args = ctx.actions.args()
    args.add(output)
    args.add(artifacts.path if artifacts else "")
    args.add(ctx.file._rustup)
    args.add(toolchain)
    args.add(ctx.attr.package)
    args.add(ctx.attr.manifest_path)
    args.add(ctx.attr.selected_lock)
    args.add(receipt_name)
    args.add(json.encode(target_args))
    args.add("proc-macro" if ctx.attr.proc_macro else "library")
    tool_inputs = [ctx.attr._rustup[DefaultInfo].data_runfiles.files]
    linker_env = {}
    if ctx.attr.needs_linker:
        cc = find_cpp_toolchain(ctx)
        features = cc_common.configure_features(ctx = ctx, cc_toolchain = cc, requested_features = ctx.features, unsupported_features = ctx.disabled_features)
        action = ACTION_NAMES.cpp_link_executable
        variables = cc_common.create_link_variables(feature_configuration = features, cc_toolchain = cc, output_file = "fixture-link-output")
        linker_env = cc_common.get_environment_variables(feature_configuration = features, action_name = action, variables = variables)
        args.add(cc_common.get_tool_for_action(feature_configuration = features, action_name = action))
        tool_inputs.append(cc.all_files)
    else:
        args.add("")
    for file, relative in sources:
        args.add(file)
        args.add(relative)
    args.add("--")
    args.add_all(ctx.attr.cargo_flags)
    ctx.actions.run(
        executable = ctx.executable._producer,
        arguments = [args],
        inputs = depset([file for file, _ in sources] + [ctx.file._rustup], transitive = tool_inputs),
        env = linker_env,
        use_default_shell_env = ctx.attr.needs_linker,
        outputs = [output] + ([artifacts] if artifacts else []),
        mnemonic = "FixtureRustdoc",
        progress_message = "Building Rustdoc fixture %{label}",
    )
    return [
        DefaultInfo(files = depset([output] + ([artifacts] if artifacts else []))),
        RustdocFixtureInfo(metadata = metadata_value),
    ]

rustdoc_fixture = rule(
    implementation = _rustdoc_fixture_impl,
    fragments = ["cpp"],
    toolchains = use_cpp_toolchain(),
    attrs = {
        "srcs": attr.label_list(allow_files = True, mandatory = True),
        "fixture_root": attr.string(mandatory = True),
        "mapped_sources": attr.label_keyed_string_dict(allow_files = True),
        "manifest_path": attr.string(default = "Cargo.toml"),
        "owner_path": attr.string(),
        "selected_lock": attr.string(),
        "library_name": attr.string(),
        "binary_name": attr.string(),
        "proc_macro": attr.bool(),
        "needs_linker": attr.bool(),
        "retain_artifacts": attr.bool(),
        "package": attr.string(mandatory = True),
        "cargo_flags": attr.string_list(),
        "_producer": attr.label(default = ":producer", executable = True, cfg = "exec"),
        "_rustup": attr.label(default = "@sdk_compilers//:rustup", allow_single_file = True, cfg = "exec"),
    },
)

def rustdoc_fixture_set(srcs, fixture_root, package, profiles, mapped_sources = {}, manifest_path = "Cargo.toml", owner_path = "", selected_lock = "", library_name = "", proc_macro = False, needs_linker = False, retain_artifacts = False, binary_name = ""):
    for name, flags in profiles.items():
        rustdoc_fixture(
            name = name,
            srcs = srcs,
            fixture_root = fixture_root,
            package = package,
            mapped_sources = mapped_sources,
            manifest_path = manifest_path,
            owner_path = owner_path,
            selected_lock = selected_lock,
            library_name = library_name,
            binary_name = binary_name,
            proc_macro = proc_macro,
            needs_linker = needs_linker,
            retain_artifacts = retain_artifacts,
            cargo_flags = flags,
        )
    return srcs + mapped_sources.keys() + [":" + name for name in profiles]
