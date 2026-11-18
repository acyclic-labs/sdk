"""The Windows MSVC sysroot Bazel compiles against.

The sysroot is a host path, so neither this repository nor any generated file
names one: the path comes from --repo_env=XWIN_SYSROOT, which the host's Bazel
configuration supplies from the same MSVC environment the Cargo Windows build
uses. This rule exposes only the real leaf directories the toolchain reads, so
the SDK's version symlinks (which point at their own directory) are never
globbed.

Without XWIN_SYSROOT the repository still loads, with no symlinks, and its one
target is incompatible with every platform: pass
--repo_env=XWIN_SYSROOT=<xwin sysroot> to build for Windows.
"""

_LEAVES = [
    "crt/include",
    "crt/lib/x86_64",
    "sdk/include/cppwinrt",
    "sdk/include/shared",
    "sdk/include/ucrt",
    "sdk/include/um",
    "sdk/include/winrt",
    "sdk/lib/ucrt/x86_64",
    "sdk/lib/um/x86_64",
]

def _xwin_repository_impl(ctx):
    root = ctx.os.environ.get("XWIN_SYSROOT")
    for leaf in (_LEAVES if root else []):
        ctx.symlink(root + "/" + leaf, leaf)
    ctx.file("WORKSPACE.bazel", "workspace(name = \"xwin\")\n")
    # The toolchain compiles against the headers and links against the import libraries, so they are
    # separate filegroups: a Rust link action reads only the libraries and never the 5000 headers.
    incompatible = "" if root else '\n    target_compatible_with = ["@platforms//:incompatible"],'
    ctx.file("BUILD.bazel", """package(default_visibility = ["//visibility:public"])

filegroup(
    name = "includes",
    srcs = glob(["crt/include/**", "sdk/include/**"], allow_empty = True),%s
)

filegroup(
    name = "libs",
    srcs = glob(["crt/lib/**", "sdk/lib/**"], allow_empty = True),%s
)
""" % (incompatible, incompatible))

xwin_repository = repository_rule(
    implementation = _xwin_repository_impl,
    environ = ["XWIN_SYSROOT"],
)
