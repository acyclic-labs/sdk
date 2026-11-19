"""Bun for Bazel tests, using the repository's maintained bootstrap pins."""

_CASES = {
    "linux-x86_64": "Linux:x86_64)",
    "linux-aarch64": "Linux:aarch64|Linux:arm64)",
    "macos-x86_64": "Darwin:x86_64)",
    "macos-aarch64": "Darwin:arm64)",
}

def _bun_repository_impl(ctx):
    os = ctx.os.name
    os = "windows" if os.startswith("windows") else {"mac os x": "macos"}.get(os, os)
    cpu = {"arm64": "aarch64", "amd64": "x86_64"}.get(ctx.os.arch, ctx.os.arch)
    key = "%s-%s" % (os, cpu)
    shell = ctx.read(ctx.attr._shell)
    version = shell.split("version=", 1)[1].split("\n", 1)[0].strip()
    if key == "windows-x86_64":
        powershell = ctx.read(ctx.attr._powershell)
        if ('$version = "' + version + '"') not in powershell:
            fail("Bun bootstrap versions disagree")
        target = powershell.split('$target = "', 1)[1].split('"', 1)[0]
        sha256 = powershell.split('$expected = "', 1)[1].split('"', 1)[0]
    elif key in _CASES:
        section = shell.split(_CASES[key], 1)[1].split(";;", 1)[0]
        target = section.split("target=", 1)[1].split("\n", 1)[0].strip()
        sha256 = section.split("expected=", 1)[1].split("\n", 1)[0].strip()
    else:
        fail("no pinned Bun release for %s" % key)
    ctx.download_and_extract(
        url = "https://github.com/oven-sh/bun/releases/download/bun-v%s/%s.zip" % (version, target),
        sha256 = sha256,
        stripPrefix = target,
    )
    executable = "bun.exe" if os == "windows" else "bun"
    build = 'exports_files(["%s"], visibility = ["//visibility:public"])\n' % executable
    build += 'filegroup(name = "bun_tool", srcs = ["%s"], visibility = ["//visibility:public"])\n' % executable
    if os == "windows":
        build += 'alias(name = "bun", actual = ":bun.exe", visibility = ["//visibility:public"])\n'
    ctx.file("BUILD.bazel", build)

_bun_repository = repository_rule(
    implementation = _bun_repository_impl,
    attrs = {
        "_shell": attr.label(default = Label("//:scripts/ensure-bun.sh"), allow_single_file = True),
        "_powershell": attr.label(default = Label("//:scripts/ensure-bun.ps1"), allow_single_file = True),
    },
)

def _bun_extension_impl(_ctx):
    _bun_repository(name = "bunjs")

bun = module_extension(implementation = _bun_extension_impl)
