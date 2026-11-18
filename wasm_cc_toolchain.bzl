"""The existing pinned LLVM distribution compiling freestanding wasm C."""

load("@rules_cc//cc:defs.bzl", "cc_toolchain")
load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:cc_toolchain_config_lib.bzl", "action_config", "feature", "flag_group", "flag_set", "tool")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/toolchains:cc_toolchain_config_info.bzl", "CcToolchainConfigInfo")

_COMPILE = [
    ACTION_NAMES.assemble,
    ACTION_NAMES.preprocess_assemble,
    ACTION_NAMES.c_compile,
    ACTION_NAMES.cpp_compile,
]
_LINK = [
    ACTION_NAMES.cpp_link_executable,
    ACTION_NAMES.cpp_link_dynamic_library,
    ACTION_NAMES.cpp_link_nodeps_dynamic_library,
]

def _default_flags(name, actions, flags):
    return feature(name = name, enabled = True, flag_sets = [
        flag_set(actions = actions, flag_groups = [flag_group(flags = flags)]),
    ])

def _wasm_config(ctx):
    clang = tool(tool = ctx.file.clang)
    archive = tool(tool = ctx.file.archive)
    target = "--target=wasm32-unknown-unknown"
    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = "llvm-wasm32",
        host_system_name = "local",
        target_system_name = "wasm32-unknown-unknown",
        target_cpu = "wasm32",
        target_libc = "unknown",
        compiler = "clang",
        abi_version = "unknown",
        abi_libc_version = "unknown",
        action_configs = [
            action_config(action_name = action, enabled = True, tools = [clang])
            for action in _COMPILE + _LINK
        ] + [
            action_config(action_name = ACTION_NAMES.cpp_link_static_library, enabled = True, tools = [archive]),
        ],
        features = [
            _default_flags("default_compile_flags", _COMPILE, [target]),
            _default_flags("default_link_flags", _LINK, [target, "-nostdlib"]),
        ],
        cxx_builtin_include_directories = ["%workspace%/" + ctx.attr.clang.label.workspace_root + "/lib/clang"],
    )

_wasm_cc_config = rule(
    implementation = _wasm_config,
    attrs = {
        "clang": attr.label(allow_single_file = True, mandatory = True),
        "archive": attr.label(allow_single_file = True, mandatory = True),
    },
    provides = [CcToolchainConfigInfo],
)

def wasm_cc_toolchain():
    """Declare the browser C toolchain for Linux execution."""
    clang = Label("@wasm_llvm_distribution//:bin/clang")
    archive = Label("@wasm_llvm_distribution//:bin/llvm-ar")
    _wasm_cc_config(
        name = "wasm_cc_config",
        clang = clang,
        archive = archive,
    )

    native.filegroup(name = "wasm_cc_files", srcs = [Label(target) for target in [
        "@wasm_llvm_distribution//:clang", "@wasm_llvm_distribution//:ar",
        "@wasm_llvm_distribution//:ld", "@wasm_llvm_distribution//:include",
        "@wasm_llvm_distribution//:runtime_shared_libraries",
    ]] + [clang, archive])
    cc_toolchain(
        name = "wasm_cc",
        toolchain_identifier = "llvm-wasm32",
        toolchain_config = ":wasm_cc_config",
        all_files = ":wasm_cc_files", ar_files = ":wasm_cc_files", as_files = ":wasm_cc_files",
        compiler_files = ":wasm_cc_files", dwp_files = ":wasm_cc_files", linker_files = ":wasm_cc_files", objcopy_files = ":wasm_cc_files", strip_files = ":wasm_cc_files",
        supports_param_files = 1,
    )
    native.toolchain(
        name = "wasm_cc_toolchain",
        toolchain = ":wasm_cc",
        toolchain_type = Label("@bazel_tools//tools/cpp:toolchain_type"),
        exec_compatible_with = [Label("@platforms//os:linux"), Label("@platforms//cpu:x86_64")],
        target_compatible_with = [Label("@platforms//os:none"), Label("@platforms//cpu:wasm32")],
    )
