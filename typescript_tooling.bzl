"""Stage the execution platform's pinned TypeScript compiler."""

def stage_typescript(commands, compiler):
    for file in compiler.typescript:
        prefix = compiler.typescript_root + "/"
        if not file.path.startswith(prefix):
            fail("TypeScript tool is outside its repository")
        relative = file.path[len(prefix):]
        destination = "node_modules/%s/%s" % (compiler.typescript_package, relative)
        commands.extend([
            'mkdir -p "$tmp/$(dirname \'%s\')"' % destination,
            'cp "$root/%s" "$tmp/%s"' % (file.path, destination),
        ])
