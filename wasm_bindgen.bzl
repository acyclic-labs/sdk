"""The pinned Linux wasm-bindgen executable for the browser toolchain."""

def _wasm_bindgen_impl(ctx):
    manifest = ctx.read(Label("//:Cargo.toml"))
    pins = [line.split('"')[1] for line in manifest.splitlines() if line.startswith('wasm-bindgen = "=')]
    if pins != ["=0.2.117"]:
        fail("the wasm-bindgen archive checksum must match the exact Cargo.toml pin")
    url = "https://github.com/wasm-bindgen/wasm-bindgen/releases/download/0.2.117/wasm-bindgen-0.2.117-x86_64-unknown-linux-musl.tar.gz"
    ctx.download_and_extract(
        url = url,
        sha256 = "97f527f7c7956f69a88a4bdb5176142ebc4e255c2dbe3805ec4f373421028240",
        stripPrefix = "wasm-bindgen-0.2.117-x86_64-unknown-linux-musl",
    )
    ctx.file("defs.bzl", '''
def _tool_impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(output = executable, target_file = ctx.file.binary, is_executable = True)
    return [DefaultInfo(executable = executable, runfiles = ctx.runfiles(files = [ctx.file.binary]))]

tool = rule(implementation = _tool_impl, executable = True,
            attrs = {"binary": attr.label(allow_single_file = True, mandatory = True)})
''')
    ctx.file("BUILD.bazel", '''
load(":defs.bzl", "tool")
tool(name = "wasm_bindgen_cli", binary = "wasm-bindgen", visibility = ["//visibility:public"])
''')

wasm_bindgen_cli = repository_rule(implementation = _wasm_bindgen_impl)
