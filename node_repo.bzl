"""The Node.js runtime Bazel tests start, from the official release for the host platform.

The version and its archive digests are pinned here, so a test's node comes from runfiles
and its action key names no host tool directory.
"""

_VERSION = "24.22.2"

# platform -> (release name, archive suffix, sha256)
_RELEASES = {
    "linux-x86_64": ("linux-x64", "tar.xz", "48cc0e21e048790e54c0cf5749c2725005313d652651c40b7a21feb9697aaf75"),
    "macos-aarch64": ("darwin-arm64", "tar.gz", "aa9549349e47dffaa42dcf95d6e06ade8c4bdafc980979883b88767670df385a"),
    "macos-x86_64": ("darwin-x64", "tar.gz", "d5e336d588c45be0c37dbd52cc29895593009ca4997a77a1fd02815131ed0e52"),
    "windows-x86_64": ("win-x64", "zip", "ca1bff5d8ead13c9f2ea404a176db46bd430704d5b5113734049fa365e7799e2"),
}

_DEFS = """\
def _tool_impl(ctx):
    node = ctx.file.node
    executable = ctx.actions.declare_file(ctx.label.name + ("." + node.extension if node.extension else ""))
    ctx.actions.symlink(output = executable, target_file = node, is_executable = True)
    return [DefaultInfo(
        executable = executable,
        files = depset([executable]),
        runfiles = ctx.runfiles(files = [executable, node]),
    )]

tool = rule(
    implementation = _tool_impl,
    attrs = {"node": attr.label(allow_single_file = True, mandatory = True)},
    executable = True,
)
"""

_BUILD = """\
load("//:defs.bzl", "tool")

package(default_visibility = ["//visibility:public"])

tool(
    name = "node",
    node = "%s",
)
"""

def _node_repository_impl(ctx):
    os = ctx.os.name
    if os.startswith("windows"):
        os = "windows"
    else:
        os = {"mac os x": "macos"}.get(os, os)
    cpu = {"arm64": "aarch64", "amd64": "x86_64"}.get(ctx.os.arch, ctx.os.arch)
    key = "%s-%s" % (os, cpu)
    if key not in _RELEASES:
        fail("node_repository: no Node.js %s release for %s" % (ctx.attr.version, key))
    release, suffix, sha256 = _RELEASES[key]
    stem = "node-v%s-%s" % (ctx.attr.version, release)
    ctx.download_and_extract(
        url = "https://nodejs.org/dist/v%s/%s.%s" % (ctx.attr.version, stem, suffix),
        sha256 = sha256,
        stripPrefix = stem,
    )
    node = "node.exe" if os == "windows" else "bin/node"
    ctx.file("defs.bzl", _DEFS)
    ctx.file("BUILD.bazel", _BUILD % node)

_node_repository = repository_rule(
    implementation = _node_repository_impl,
    attrs = {"version": attr.string(mandatory = True)},
)

_node = tag_class(attrs = {"version": attr.string(default = _VERSION)})

def _node_extension_impl(ctx):
    version = _VERSION
    for module in ctx.modules:
        for tag in module.tags.node:
            version = tag.version
    _node_repository(name = "nodejs", version = version)

node = module_extension(
    implementation = _node_extension_impl,
    tag_classes = {"node": _node},
)
