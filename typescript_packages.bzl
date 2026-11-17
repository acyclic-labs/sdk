"""Build workspace JavaScript packages with their canonical contract generators."""

def _quote(value):
    return "'" + value.replace("'", "'\\''") + "'"

def _relative(file):
    path = file.short_path
    return "/".join(path.split("/")[2:]) if path.startswith("../") else path

def _packages_impl(ctx):
    outputs = {name: ctx.actions.declare_directory(ctx.label.name + "/" + name) for name in ctx.attr.packages}
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
    commands.extend([
        'cd "$tmp"',
        'SDK_EXEC_ROOT="$root" "$root/%s" scripts/project-bazel-packages.mjs %s "$root/%s"' % (ctx.executable._node.path, _quote(json.encode(ctx.attr.packages)), manifest.path),
    ])
    for name, output in outputs.items():
        commands.extend([
            'src="$tmp/typescript/packages/%s/src"' % name,
            'dist="$tmp/typescript/packages/%s/dist"' % name,
            'find "$src" -type f -name "*.ts" ! -name "*.d.ts" | while IFS= read -r file; do',
            '  relative="${file#"$src"/}"',
            '  "$root/%s" build --target=node --format=esm --external=\'*\' --outdir "$dist/$(dirname "$relative")" "$file"' % ctx.executable._bun.path,
            'done',
            'mkdir -p "$root/%s"' % output.path,
            'cp -R "$tmp/typescript/packages/%s/." "$root/%s/"' % (name, output.path),
        ])
    ctx.actions.run_shell(
        inputs = depset(ctx.files.srcs + ctx.files.protos + ctx.files._npm + [manifest]),
        tools = [ctx.attr._node[DefaultInfo].files_to_run, ctx.attr._bun[DefaultInfo].files_to_run] + tools.values(),
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
    attrs = {
        "packages": attr.string_list(mandatory = True),
        "srcs": attr.label_list(allow_files = True),
        "protos": attr.label_keyed_string_dict(allow_files = True),
        "tools": attr.label_keyed_string_dict(allow_files = True),
        "_npm": attr.label(default = "@npm_codegen_packages//:packages", allow_files = True),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
        "_bun": attr.label(default = "@bunjs//:bun", executable = True, allow_files = True, cfg = "exec"),
    },
)
