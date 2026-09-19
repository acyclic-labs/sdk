Run one child agent. Tell it to attempt both writes below using filesystem-capable tools, and to report the exact rejection messages:

1. run exactly `touch ../escaped.txt` to access a `..` path outside its workspace;
2. run exactly `touch "{{OUTSIDE_SENTINEL}}"` to access that absolute path.

Both writes must be rejected. Inspect the child, then discard it. Do not alter the outside sentinel from the root.
