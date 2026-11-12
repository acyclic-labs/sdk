"""GNU toolchains supplied by the content-pinned REAPI execution image.

The worker enforces the image property and mounts its filesystem read-only.
Compiler binaries, headers and runtimes are image inputs, rather than host tools.
"""
load("@rules_cc//cc:defs.bzl", "CcToolchainConfigInfo", "cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl", "feature", "tool_path")

def _config(ctx):
    windows = ctx.attr.windows
    prefix = "/usr/bin/x86_64-w64-mingw32-" if windows else "/usr/bin/"
    tools = {name: prefix + name for name in ["ar", "ld", "nm", "objcopy", "objdump", "strip", "gcov"]}
    tools["gcc"] = ctx.attr.compiler
    tools["cpp"] = tools["gcc"]
    tools["dwp"] = "/usr/bin/dwp"
    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = ctx.label.name,
        host_system_name = "x86_64-linux-gnu",
        target_system_name = "x86_64-w64-mingw32" if windows else "x86_64-linux-gnu",
        target_cpu = "x64_windows" if windows else "k8",
        target_libc = "msvcrt" if windows else "glibc",
        compiler = "gcc",
        abi_version = "gcc",
        abi_libc_version = "msvcrt" if windows else "glibc",
        cxx_builtin_include_directories = ["/usr/include", "/usr/lib/gcc", "/usr/x86_64-w64-mingw32/include"],
        tool_paths = [tool_path(name = name, path = path) for name, path in tools.items()],
        features = [feature(name = "supports_pic", enabled = True)] + ([] if windows else [feature(name = "supports_dynamic_linker", enabled = True)]),
    )

image_cc_config = rule(
    implementation = _config,
    attrs = {"windows": attr.bool(), "compiler": attr.string(mandatory = True)},
    provides = [CcToolchainConfigInfo],
)

def image_cc_toolchain(name, windows = False, compiler = "/usr/bin/gcc"):
    image_cc_config(name = name + "_config", windows = windows, compiler = compiler)
    native.filegroup(name = name + "_image_files", srcs = [])
    files = ":" + name + "_image_files"
    cc_toolchain(
        name = name + "_compiler",
        toolchain_identifier = name,
        toolchain_config = ":" + name + "_config",
        all_files = files,
        ar_files = files,
        as_files = files,
        compiler_files = files,
        dwp_files = files,
        linker_files = files,
        objcopy_files = files,
        strip_files = files,
        supports_param_files = 1,
    )
    native.toolchain(
        name = name,
        toolchain = ":" + name + "_compiler",
        toolchain_type = "@bazel_tools//tools/cpp:toolchain_type",
        exec_compatible_with = ["@platforms//cpu:x86_64", "@platforms//os:linux"],
        target_compatible_with = ["@platforms//cpu:x86_64", "@platforms//os:windows" if windows else "@platforms//os:linux"],
    )

def image_execution_platform(name, compile_image, test_image):
    native.platform(
        name = name,
        constraint_values = ["@platforms//os:linux", "@platforms//cpu:x86_64"],
        exec_properties = {"container-image": compile_image, "test.container-image": test_image},
    )
