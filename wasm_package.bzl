"""Places a wasm-bindgen target's output where its TypeScript package imports it."""

load("@rules_rust_wasm_bindgen//:providers.bzl", "RustWasmBindgenInfo")

def _wasm_package_impl(ctx):
    bindgen = ctx.attr.bindgen[RustWasmBindgenInfo]
    output = ctx.actions.declare_directory(ctx.attr.out_dir)
    inputs = ctx.attr.bindgen[DefaultInfo].files
    arguments = [output.path, bindgen.root]
    command = "mkdir -p \"$1\" && cp -r \"$2\"/. \"$1\"/"
    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [output],
        arguments = arguments,
        command = command,
        mnemonic = "WasmPackage",
    )
    return [DefaultInfo(files = depset([output]))]

wasm_package = rule(
    implementation = _wasm_package_impl,
    attrs = {
        "bindgen": attr.label(mandatory = True, providers = [RustWasmBindgenInfo]),
        "out_dir": attr.string(mandatory = True),
    },
)
