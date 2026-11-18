"""The existing pinned LLVM distribution compiling freestanding wasm C."""

load("@rules_cc//cc:defs.bzl", "cc_toolchain")
load("@rules_cc//cc/private/toolchain:unix_cc_toolchain_config.bzl", "cc_toolchain_config")

def wasm_cc_toolchain():
    """Declare the browser C toolchain for Linux execution."""
    root = Label("@wasm_llvm_distribution//:bin/clang").workspace_root
    # C++ actions start at the execution root. Cargo's build-script runner
    # resolves this anchor before changing to the crate's manifest directory.
    tool_root = "/proc/self/cwd/" + root
    binaries = {
        "gcc": "clang", "cpp": "clang-cpp", "ar": "llvm-ar", "ld": "wasm-ld",
        "nm": "llvm-nm", "objdump": "llvm-objdump", "strip": "llvm-strip", "gcov": "llvm-cov",
    }
    tools = {name: tool_root + "/bin/" + binary for name, binary in binaries.items()}
    cc_toolchain_config(
        name = "wasm_cc_config",
        toolchain_identifier = "llvm-wasm32",
        host_system_name = "local",
        target_system_name = "wasm32-unknown-unknown",
        target_libc = "unknown",
        abi_version = "unknown",
        abi_libc_version = "unknown",
        cpu = "wasm32",
        compiler = "clang",
        tool_paths = tools,
        compile_flags = ["--target=wasm32-unknown-unknown"],
        link_flags = ["--target=wasm32-unknown-unknown", "-nostdlib"],
        cxx_builtin_include_directories = ["%workspace%/" + root + "/lib/clang"],
    )

    native.filegroup(name = "wasm_cc_files", srcs = [Label(target) for target in [
        "@wasm_llvm_distribution//:clang", "@wasm_llvm_distribution//:ar",
        "@wasm_llvm_distribution//:ld", "@wasm_llvm_distribution//:include",
        "@wasm_llvm_distribution//:runtime_shared_libraries",
    ] + ["@wasm_llvm_distribution//:bin/" + binary for binary in binaries.values()]])
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
