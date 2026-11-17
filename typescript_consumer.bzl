"""Type-check consumers against the declared staged package trees."""

load("//:typescript_tooling.bzl", "stage_typescript")

def _impl(ctx):
    compiler = ctx.toolchains["//toolchains/javascript:toolchain_type"]
    output = ctx.actions.declare_file(ctx.label.name + ".checked")
    roots = [f.path.split("/node_modules/")[0] for f in ctx.files._npm if "/node_modules/" in f.path]
    commands = [
        "set -eu",
        'root="$PWD"',
        'tmp="$(mktemp -d)"',
        'trap \'rm -rf "$tmp"\' EXIT',
        'cp -RL "$root/%s/node_modules" "$tmp/"' % roots[0],
    ]
    stage_typescript(commands, compiler)
    commands.extend([
        'cd "$tmp"',
        'printf \'{"type":"module"}\\n\' > package.json',
    ])
    for target, name in ctx.attr.packages.items():
        files = target[DefaultInfo].files.to_list()
        if len(files) != 1 or not files[0].is_directory:
            fail("package tree must be a directory")
        commands.extend([
            'rm -rf node_modules/@acyclic-labs',
            'cp -R "$root/%s/." node_modules/' % files[0].path,
            'printf \'import * as api from "%s"; void api;\\n\' > consumer.ts' % name,
            '"$root/%s" node_modules/typescript/bin/tsc --noEmit --strict --skipLibCheck --module NodeNext --moduleResolution NodeNext --target ES2022 consumer.ts' % ctx.executable._node.path,
        ])
    commands.append('touch "$root/%s"' % output.path)
    ctx.actions.run_shell(
        inputs = depset(ctx.files.packages + ctx.files._npm + compiler.typescript),
        tools = [ctx.attr._node[DefaultInfo].files_to_run],
        outputs = [output],
        command = "\n".join(commands),
        mnemonic = "TypeScriptConsumer",
    )
    return [DefaultInfo(files = depset([output]))]

typescript_consumer = rule(
    implementation = _impl,
    toolchains = ["//toolchains/javascript:toolchain_type"],
    attrs = {
        "packages": attr.label_keyed_string_dict(allow_files = True),
        "_npm": attr.label(default = "@npm_codegen_packages//:packages", allow_files = True),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
    },
)
