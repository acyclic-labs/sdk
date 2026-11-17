"""Execution-platform JavaScript generation tools."""

def _impl(ctx):
    buf_files = ctx.files.buf
    binary = [file for file in buf_files if file.basename in ["buf", "buf.exe"]]
    if len(binary) != 1:
        fail("Buf archive must contain one executable")
    return [platform_common.ToolchainInfo(
        buf = binary[0],
        typescript = ctx.files.typescript,
        typescript_root = ctx.attr.typescript.label.workspace_root,
        typescript_package = ctx.attr.typescript_package,
    )]

javascript_tools = rule(
    implementation = _impl,
    attrs = {
        "buf": attr.label(mandatory = True, allow_files = True),
        "typescript": attr.label(mandatory = True, allow_files = True),
        "typescript_package": attr.string(mandatory = True),
    },
)
