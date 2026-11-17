"""Places a wasm-bindgen target's output where its TypeScript package imports it."""

load("@rules_rust_wasm_bindgen//:providers.bzl", "RustWasmBindgenInfo")

def _wasm_package_impl(ctx):
    bindgen = ctx.attr.bindgen[RustWasmBindgenInfo]
    output = ctx.actions.declare_directory(ctx.attr.out_dir)
    inputs = ctx.attr.bindgen[DefaultInfo].files
    arguments = [output.path, bindgen.root]
    command = "mkdir -p \"$1\" && cp -r \"$2\"/. \"$1\"/"
    if ctx.attr.package_name:
        package_json = ctx.actions.declare_file(ctx.label.name + ".package.json")
        index_js = ctx.actions.declare_file(ctx.label.name + ".index.js")
        index_dts = ctx.actions.declare_file(ctx.label.name + ".index.d.ts")
        ctx.actions.write(package_json, json.encode({
            "name": ctx.attr.package_name,
            "version": "0.2.0",
            "private": True,
            "type": "module",
            "main": "index.js",
            "types": "index.d.ts",
            "exports": {".": {"types": "./index.d.ts", "default": "./index.js"}},
        }) + "\n")
        ctx.actions.write(index_js, "\n".join([
            "export { default } from \"./%s.js\";" % ctx.attr.entry,
            "export * from \"./%s.js\";" % ctx.attr.entry,
            "export const wasmUrl = new URL(\"./%s_bg.wasm\", import.meta.url);" % ctx.attr.entry,
            "",
        ]))
        ctx.actions.write(index_dts, "\n".join([
            "export { default } from \"./%s.js\";" % ctx.attr.entry,
            "export * from \"./%s.js\";" % ctx.attr.entry,
            "export const wasmUrl: URL;",
            "",
        ]))
        inputs = depset(transitive = [inputs, depset([package_json, index_js, index_dts])])
        arguments.extend([package_json.path, index_js.path, index_dts.path])
        command += " && cp \"$3\" \"$1/package.json\" && cp \"$4\" \"$1/index.js\" && cp \"$5\" \"$1/index.d.ts\""
    if ctx.attr.strip_closure_shims:
        # wasm-bindgen's closure shims are not part of the public surface. Drop the declarations
        # exactly as the Cargo build did, so the package is identical whichever path produced it.
        command += " && for f in \"$1/%s.d.ts\" \"$1/%s_bg.wasm.d.ts\"; do sed -E '/^[[:space:]]*(readonly |export const )?wasm_bindgen_([0-9a-f]*)_+convert__closures_+invoke_[^:[:space:]]*:.*$/d' \"$f\" > \"$f.tmp\" && mv \"$f.tmp\" \"$f\"; done" % (ctx.attr.entry, ctx.attr.entry)
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
        "package_name": attr.string(),
        "entry": attr.string(),
        "strip_closure_shims": attr.bool(default = False),
    },
)
