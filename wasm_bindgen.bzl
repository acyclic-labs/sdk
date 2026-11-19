"""The pinned wasm-bindgen executable for the browser toolchain, for the host it runs on."""

# Each host's release archive of the pinned version, by Bazel's host OS and CPU names, with the SHA-256 the
# release publishes for it.
_ARCHIVES = {
    ("linux", "amd64"): ("x86_64-unknown-linux-musl", "97f527f7c7956f69a88a4bdb5176142ebc4e255c2dbe3805ec4f373421028240"),
    ("linux", "aarch64"): ("aarch64-unknown-linux-musl", "a9e2c56bf58a95bdfd8f2a67f726a3d11fa873cfe984468e5bbf0e48cb7dd923"),
    ("mac os x", "aarch64"): ("aarch64-apple-darwin", "d7691b221163d68884ef0a893cb57d3372f1100789dbf0cc2c9260a7f253a497"),
    ("windows", "amd64"): ("x86_64-pc-windows-msvc", "54d52650adcf93e332d1d02c94c81925dfbda1f1292cecf276a0507263a4936f"),
}

def _wasm_bindgen_impl(ctx):
    manifest = ctx.read(Label("//:Cargo.toml"))
    pins = [line.split('"')[1] for line in manifest.splitlines() if line.startswith('wasm-bindgen = "=')]
    if pins != ["=0.2.117"]:
        fail("the wasm-bindgen archive checksum must match the exact Cargo.toml pin")
    host = ("windows" if ctx.os.name.startswith("windows") else ctx.os.name, {"x86_64": "amd64", "arm64": "aarch64"}.get(ctx.os.arch, ctx.os.arch))
    if host not in _ARCHIVES:
        fail("wasm-bindgen 0.2.117 publishes no archive for host %s %s" % host)
    triple, sha256 = _ARCHIVES[host]
    ctx.download_and_extract(
        url = "https://github.com/wasm-bindgen/wasm-bindgen/releases/download/0.2.117/wasm-bindgen-0.2.117-%s.tar.gz" % triple,
        sha256 = sha256,
        stripPrefix = "wasm-bindgen-0.2.117-%s" % triple,
    )
    ctx.file("defs.bzl", '''
def _tool_impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name + (".exe" if ctx.file.binary.extension == "exe" else ""))
    ctx.actions.symlink(output = executable, target_file = ctx.file.binary, is_executable = True)
    return [DefaultInfo(executable = executable, runfiles = ctx.runfiles(files = [ctx.file.binary]))]

tool = rule(implementation = _tool_impl, executable = True,
            attrs = {"binary": attr.label(allow_single_file = True, mandatory = True)})
''')
    binary = "wasm-bindgen.exe" if host[0] == "windows" else "wasm-bindgen"
    ctx.file("BUILD.bazel", '''
load(":defs.bzl", "tool")
tool(name = "wasm_bindgen_cli", binary = "%s", visibility = ["//visibility:public"])
''' % binary)
    return ctx.repo_metadata(reproducible = True)

wasm_bindgen_cli = repository_rule(implementation = _wasm_bindgen_impl)
