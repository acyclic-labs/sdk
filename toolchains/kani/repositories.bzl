"""Pinned Kani and the exact Rust compiler used to build its release."""

_VERSION = "0.68.0"
_TRIPLE = "x86_64-unknown-linux-gnu"
_NIGHTLY = "2026-08-21"

def _repository_impl(ctx):
    ctx.download_and_extract(
        url = "https://github.com/model-checking/kani/releases/download/kani-%s/kani-%s-%s.tar.gz" % (_VERSION, _VERSION, _TRIPLE),
        sha256 = "32e2b484d73ede0bbf64a2cf0879c4259422497d8aec4ee67d448de9ae7843d3",
        stripPrefix = "kani-" + _VERSION,
    )
    for component, prefix, digest in [
        ("rustc", "rustc", "e531815fa551362b5a485e0d286b6c96555312d49359c0b8fe9e8f34c45594b4"),
        ("rust-std", "rust-std-" + _TRIPLE, "52cd12570c6f967c3d04173e061fdab1cd44ab9613f53df8e04f7ce3b52ea3a3"),
    ]:
        ctx.download_and_extract(
            url = "https://static.rust-lang.org/dist/%s/%s-nightly-%s.tar.xz" % (_NIGHTLY, component, _TRIPLE),
            sha256 = digest,
            output = "toolchain",
            stripPrefix = "%s-nightly-%s/%s" % (component, _TRIPLE, prefix),
        )
    expected = "nightly-%s-%s" % (_NIGHTLY, _TRIPLE)
    if ctx.read("rust-toolchain-version").strip() != expected:
        fail("Kani release and Rust compiler pins disagree")
    ctx.file("BUILD.bazel", """\
load(%r, "kani_tool")
kani_tool(
    name = "kani",
    driver = "bin/kani-driver",
    data = glob(["**"], exclude = ["BUILD.bazel"]),
    visibility = ["//visibility:public"],
)
""" % str(ctx.attr._defs))

_repository = repository_rule(
    implementation = _repository_impl,
    attrs = {"_defs": attr.label(default = Label("//toolchains/kani:defs.bzl"))},
)

def _extension_impl(_ctx):
    _repository(name = "kani")

kani = module_extension(implementation = _extension_impl)
