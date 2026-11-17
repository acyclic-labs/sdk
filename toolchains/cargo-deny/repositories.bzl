"""Pinned cargo-deny binaries and RustSec data for the offline policy test."""

_VERSION = "0.19.9"
_RUSTSEC = "233155b4025b854c68a4b199c0e32d7fd53291a2"
_RUSTSEC_SHA256 = "638f3bde5819e3301e324bc2cdfc33c2047e6aa94ed84f53c37057782e91d0b4"
_HOSTS = {
    "linux-x86_64": ("x86_64-unknown-linux-musl", "f1f8eedc2a3ac297c540873f93785d4104b102c0079506b2a6b3221b7ec956af"),
    "linux-aarch64": ("aarch64-unknown-linux-musl", "32580dcc2bc13fbeeb7c50edc38ef99c3b9ce9569a1d07a96e11b3b941f3e72e"),
    "macos-x86_64": ("x86_64-apple-darwin", "3336724665a3aef124a9e4c79cb59968df36d21bfcda5ae596abe2a7874b1938"),
    "macos-aarch64": ("aarch64-apple-darwin", "be6fd555e910ac360e25cbef4f16ead47d87d2545fa07aa27223ef0f9af1a02c"),
    "windows-x86_64": ("x86_64-pc-windows-msvc", "a0f2b3da55444f1a354fcbd20ce4b94981ab8193c1b674362d4761905f31ffd0"),
}

def _binary_impl(ctx):
    os = {"mac os x": "macos"}.get(ctx.os.name, ctx.os.name)
    if os.startswith("windows"):
        os = "windows"
    cpu = {"arm64": "aarch64", "amd64": "x86_64"}.get(ctx.os.arch, ctx.os.arch)
    key = "%s-%s" % (os, cpu)
    if key not in _HOSTS:
        fail("cargo-deny has no pinned release for %s" % key)
    triple, sha = _HOSTS[key]
    release = "cargo-deny-%s-%s" % (_VERSION, triple)
    ctx.download_and_extract(
        url = "https://github.com/EmbarkStudios/cargo-deny/releases/download/%s/%s.tar.gz" % (_VERSION, release),
        sha256 = sha,
        stripPrefix = release,
    )
    binary = "cargo-deny.exe" if os == "windows" else "cargo-deny"
    ctx.file("BUILD.bazel", "exports_files([%r], visibility = [\"//visibility:public\"])\n" % binary)

def _advisories_impl(ctx):
    ctx.download_and_extract(
        url = "https://codeload.github.com/RustSec/advisory-db/tar.gz/%s" % _RUSTSEC,
        sha256 = _RUSTSEC_SHA256,
        type = "tar.gz",
        stripPrefix = "advisory-db-%s" % _RUSTSEC,
    )
    ctx.file("BUILD.bazel", "filegroup(name = \"data\", srcs = glob([\"**\"]), visibility = [\"//visibility:public\"])\n")

_binary = repository_rule(implementation = _binary_impl)
_advisories = repository_rule(implementation = _advisories_impl)

def _extension_impl(_ctx):
    _binary(name = "cargo_deny_binary")
    _advisories(name = "cargo_deny_advisories")

cargo_deny = module_extension(implementation = _extension_impl)
