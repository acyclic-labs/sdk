"""Browser modules and their bindings are ordinary, independently cached actions."""

def _wasm_configuration_impl(_settings, _attr):
    return {
        "//command_line_option:platforms": [str(Label("//:wasm32"))],
        "//command_line_option:compilation_mode": "opt",
    }

_wasm_configuration = transition(
    implementation = _wasm_configuration_impl,
    inputs = [],
    outputs = ["//command_line_option:platforms", "//command_line_option:compilation_mode"],
)

def _wasm_bindings_impl(ctx):
    if len(ctx.files.module) != 1:
        fail("a wasm module must supply exactly one linked output")
    output = ctx.actions.declare_directory(ctx.label.name)
    ctx.actions.run(
        executable = ctx.toolchains["@rules_rust_wasm_bindgen//:toolchain_type"].wasm_bindgen_cli,
        arguments = [ctx.files.module[0].path, "--target", "web", "--out-dir", output.path,
                     "--out-name", ctx.attr.out_name, "--remove-name-section", "--remove-producers-section"],
        inputs = ctx.files.module,
        outputs = [output],
        mnemonic = "WasmBindings",
        toolchain = "@rules_rust_wasm_bindgen//:toolchain_type",
    )
    return [DefaultInfo(files = depset([output]))]

wasm_bindings = rule(
    implementation = _wasm_bindings_impl,
    attrs = {
        "module": attr.label(cfg = _wasm_configuration, mandatory = True),
        "out_name": attr.string(mandatory = True),
        "_allowlist_function_transition": attr.label(default = "@bazel_tools//tools/allowlists/function_transition_allowlist"),
    },
    toolchains = ["@rules_rust_wasm_bindgen//:toolchain_type"],
)
