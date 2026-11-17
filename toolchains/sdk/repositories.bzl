"""The actual pinned Cargo/Rustdoc tools used by SDK artifact conformance."""

_RUSTUP = "1.28.2"
_HOSTS = {
    "linux-x86_64": ("x86_64-unknown-linux-gnu", "20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c"),
    "macos-aarch64": ("aarch64-apple-darwin", "20ef5516c31b1ac2290084199ba77dbbcaa1406c45c1d978ca68558ef5964ef5"),
    "windows-x86_64": ("x86_64-pc-windows-msvc", "88d8258dcf6ae4f7a80c7d1088e1f36fa7025a1cfd1343731b4ee6f385121fc0"),
}

def _repository_impl(ctx):
    os = {"mac os x": "macos"}.get(ctx.os.name, ctx.os.name)
    if os.startswith("windows"):
        os = "windows"
    cpu = {"arm64": "aarch64", "amd64": "x86_64"}.get(ctx.os.arch, ctx.os.arch)
    host, digest = _HOSTS["%s-%s" % (os, cpu)]
    suffix = ".exe" if os == "windows" else ""
    installer = "rustup-init" + suffix
    ctx.download(
        url = "https://static.rust-lang.org/rustup/archive/%s/%s/%s" % (_RUSTUP, host, installer),
        output = installer,
        sha256 = digest,
        executable = True,
    )
    environment = {
        "CARGO_HOME": str(ctx.path("tools")),
        "RUSTUP_HOME": str(ctx.path("rustup")),
        "RUSTUP_INIT_SKIP_PATH_CHECK": "yes",
        "RUSTUP_DIST_SERVER": "https://static.rust-lang.org",
        "RUSTUP_UPDATE_ROOT": "https://static.rust-lang.org/rustup",
    }
    result = ctx.execute(
        [str(ctx.path(installer)), "-y", "--no-modify-path", "--default-toolchain", "none"],
        environment = environment,
    )
    if result.return_code:
        fail("private rustup initialization failed: " + result.stderr)
    rustup = "tools/bin/rustup" + suffix
    for toolchain in ["1.98.1", "nightly-2026-07-01"]:
        arguments = [str(ctx.path(rustup)), "toolchain", "install", toolchain, "--profile", "minimal", "--no-self-update"]
        if toolchain.startswith("nightly"):
            arguments.extend(["--target", "wasm32-unknown-unknown"])
        result = ctx.execute(arguments, environment = environment)
        if result.return_code:
            fail("SDK compiler provisioning failed: " + result.stderr)
    ctx.delete(installer)
    ctx.delete("rustup/downloads")
    ctx.delete("rustup/tmp")
    ctx.file("BUILD.bazel", """\
package(default_visibility = ["//visibility:public"])
filegroup(
    name = "rustup",
    srcs = [%r],
    data = glob(["tools/**", "rustup/**"]),
)
""" % rustup)

_repository = repository_rule(implementation = _repository_impl)

def _extension_impl(_ctx):
    _repository(name = "sdk_compilers")

sdk_compilers = module_extension(implementation = _extension_impl)
