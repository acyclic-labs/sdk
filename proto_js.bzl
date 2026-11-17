"""Generate JavaScript protobuf bindings from declared schemas and pinned tools."""

def _quote(value):
    return "'" + value.replace("'", "'\\''") + "'"

def _proto_js_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name)
    npm = ctx.files._npm
    roots = [f.path.split("/node_modules/")[0] for f in npm if "/node_modules/" in f.path]
    if not roots:
        fail("protobuf generator dependencies are empty")
    modules = {}
    paths = []
    commands = [
        "set -eu",
        'root="$PWD"',
        'tmp="$(mktemp -d)"',
        'trap \'rm -rf "$tmp"\' EXIT',
        'cp -R "$root/%s/node_modules" "$tmp/"' % roots[0],
    ]
    for target, destination in ctx.attr.schemas.items():
        files = target[DefaultInfo].files.to_list()
        if len(files) != 1:
            fail("each schema must be a single file")
        module = destination.split("/proto/", 1)[0] + "/proto"
        modules[module] = True
        paths.append(destination)
        commands.extend([
            'mkdir -p "$tmp/$(dirname %s)"' % _quote(destination),
            'cp "$root/%s" "$tmp/"%s' % (files[0].path, _quote(destination)),
        ])
    yaml = "version: v2\nmodules:\n" + "".join(["  - path: %s\n" % module for module in modules])
    commands.extend([
        'printf %%s %s > "$tmp/buf.yaml"' % _quote(yaml),
        'printf \'version: v2\\nplugins:\\n  - local: ["%s", "%s"]\\n    out: generated\\n    strategy: all\\n    opt: [target=js+dts, import_extension=js]\\n\' "$root/%s" "$tmp/node_modules/@bufbuild/protoc-gen-es/bin/protoc-gen-es" > "$tmp/buf.gen.yaml"' % ("%s", "%s", ctx.executable._node.path),
        'cd "$tmp"',
        '"$tmp/node_modules/@bufbuild/buf-linux-x64/bin/buf" generate %s' % " ".join(["--path " + _quote(path) for path in paths]),
        'mkdir -p "$root/%s"' % output.path,
        'cp -R generated/. "$root/%s/"' % output.path,
    ])
    ctx.actions.run_shell(
        inputs = depset(ctx.files.schemas + npm),
        tools = [ctx.executable._node],
        outputs = [output],
        command = "\n".join(commands),
        mnemonic = "ProtoJavaScript",
    )
    return [DefaultInfo(files = depset([output]))]

proto_js = rule(
    implementation = _proto_js_impl,
    attrs = {
        "schemas": attr.label_keyed_string_dict(allow_files = True, mandatory = True),
        "_npm": attr.label(default = "@npm_codegen_packages//:packages", allow_files = True),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
    },
)
