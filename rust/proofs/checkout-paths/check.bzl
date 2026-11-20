"""Compiles one crate from two checkouts with the registered Rust toolchain's clippy-driver."""

def _dirname(file):
    return file.dirname

def _checkout_paths_check_impl(ctx):
    toolchain = ctx.toolchains["@rules_rust//rust:toolchain_type"]
    out = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    args.add(ctx.executable._process_wrapper)
    args.add(toolchain.clippy_driver)
    args.add_all([toolchain.sysroot_anchor], map_each = _dirname)
    args.add(ctx.file.src)
    args.add(ctx.file.clippy_config)
    args.add_all([out], expand_directories = False)
    ctx.actions.run(
        executable = ctx.executable._checker,
        arguments = [args],
        inputs = depset([ctx.file.src, ctx.file.clippy_config], transitive = [toolchain.all_files]),
        tools = [ctx.executable._process_wrapper],
        outputs = [out],
        mnemonic = "CheckoutPaths",
        progress_message = "Compiling %{label} from two checkouts",
    )
    return [DefaultInfo(files = depset([out]))]

checkout_paths_check = rule(
    implementation = _checkout_paths_check_impl,
    attrs = {
        "clippy_config": attr.label(allow_single_file = True, mandatory = True),
        "src": attr.label(allow_single_file = [".rs"], mandatory = True),
        "_checker": attr.label(default = ":checker", executable = True, cfg = "exec"),
        "_process_wrapper": attr.label(default = "@rules_rust//util/process_wrapper", executable = True, cfg = "exec"),
    },
    toolchains = ["@rules_rust//rust:toolchain_type"],
)
