"""Build workspace JavaScript packages with their canonical contract generators."""

load("//:typescript_tooling.bzl", "stage_typescript")

def _quote(value):
    return "'" + value.replace("'", "'\\''") + "'"

def _relative(file):
    path = file.short_path
    return "/".join(path.split("/")[2:]) if path.startswith("../") else path

def _packages_impl(ctx):
    outputs = {name: ctx.actions.declare_directory(ctx.label.name + "/" + name) for name in ctx.attr.packages}
    compiler = ctx.toolchains["//toolchains/javascript:toolchain_type"]
    manifest = ctx.actions.declare_file(ctx.label.name + ".tools.json")
    tools = {label: target[DefaultInfo].files_to_run for target, label in ctx.attr.tools.items()}
    ctx.actions.write(manifest, json.encode({label: value.executable.path for label, value in tools.items()}))
    roots = [f.path.split("/node_modules/")[0] for f in ctx.files._npm if "/node_modules/" in f.path]
    commands = [
        "set -eu",
        'root="$PWD"',
        'tmp="$(mktemp -d)"',
        'trap \'rm -rf "$tmp"\' EXIT',
        'cp -R "$root/%s/node_modules" "$tmp/"' % roots[0],
    ]
    for file in ctx.files.srcs:
        path = _relative(file)
        commands.extend([
            'mkdir -p "$tmp/$(dirname %s)"' % _quote(path),
            'cp "$root/%s" "$tmp/"%s' % (file.path, _quote(path)),
        ])
    for target, name in ctx.attr.protos.items():
        files = target[DefaultInfo].files.to_list()
        if len(files) != 1 or not files[0].is_directory:
            fail("protobuf binding must be a directory")
        commands.extend([
            'mkdir -p "$tmp/typescript/packages/%s/generated/proto"' % name,
            'cp -R "$root/%s/." "$tmp/typescript/packages/%s/generated/proto/"' % (files[0].path, name),
        ])
    for target, name in ctx.attr.wasms.items():
        files = target[DefaultInfo].files.to_list()
        if len(files) != 1 or not files[0].is_directory:
            fail("Wasm binding must be a directory")
        commands.extend([
            'mkdir -p "$tmp/typescript/packages/%s/generated/wasm"' % name,
            'cp -R "$root/%s/." "$tmp/typescript/packages/%s/generated/wasm/"' % (files[0].path, name),
        ])
    stage_typescript(commands, compiler)
    commands.extend([
        'cd "$tmp"',
        'SDK_EXEC_ROOT="$root" "$root/%s" scripts/project-bazel-packages.mjs %s "$root/%s"' % (ctx.executable._node.path, _quote(json.encode(ctx.attr.packages)), manifest.path),
        'mkdir -p node_modules/@acyclic-labs',
    ])
    for name in ctx.attr.packages:
        commands.append('ln -s "../../typescript/packages/%s" "node_modules/@acyclic-labs/%s"' % (name, name))
    commands.append('"$root/%s" node_modules/typescript/bin/tsc -b %s' % (
        ctx.executable._node.path,
        " ".join([_quote("typescript/packages/" + name) for name in ctx.attr.packages]),
    ))
    for name, output in outputs.items():
        commands.extend([
            'mkdir -p "$root/%s"' % output.path,
            'cp -R "$tmp/typescript/packages/%s/." "$root/%s/"' % (name, output.path),
        ])
    ctx.actions.run_shell(
        inputs = depset(ctx.files.srcs + ctx.files.protos + ctx.files.wasms + ctx.files._npm + compiler.typescript + [manifest]),
        tools = [ctx.attr._node[DefaultInfo].files_to_run] + tools.values(),
        outputs = outputs.values(),
        command = "\n".join(commands),
        mnemonic = "TypeScriptPackages",
    )
    return [
        DefaultInfo(files = depset(outputs.values())),
        OutputGroupInfo(**{name: depset([output]) for name, output in outputs.items()}),
    ]

typescript_packages = rule(
    implementation = _packages_impl,
    toolchains = ["//toolchains/javascript:toolchain_type"],
    attrs = {
        "packages": attr.string_list(mandatory = True),
        "srcs": attr.label_list(allow_files = True),
        "protos": attr.label_keyed_string_dict(allow_files = True),
        "wasms": attr.label_keyed_string_dict(allow_files = True),
        "tools": attr.label_keyed_string_dict(allow_files = True, cfg = "exec"),
        "_npm": attr.label(default = "@npm_codegen_packages//:packages", allow_files = True),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
    },
)
