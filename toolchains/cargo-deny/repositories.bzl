"""Pinned RustSec archive for the index action."""

def _archive_impl(ctx):
    ctx.download(
        url = "https://codeload.github.com/RustSec/advisory-db/tar.gz/233155b4025b854c68a4b199c0e32d7fd53291a2",
        output = "advisories.tar.gz",
        sha256 = "638f3bde5819e3301e324bc2cdfc33c2047e6aa94ed84f53c37057782e91d0b4",
    )
    ctx.file("BUILD.bazel", "exports_files([\"advisories.tar.gz\"], visibility = [\"//visibility:public\"])\n")

_archive = repository_rule(implementation = _archive_impl)

def _extension_impl(_ctx):
    _archive(name = "cargo_deny_advisories")

cargo_deny = module_extension(implementation = _extension_impl)
