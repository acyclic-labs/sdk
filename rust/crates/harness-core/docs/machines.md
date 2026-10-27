# Machines adapter

The `machines` feature bridges sandbox and checkpoint references to a
`MachinesProvider`. `MachinesHost` captures selected forkable checkpoints and
verifies their retained provider identity before admission.

With `machines`, `filesystem` and `native-execution`, `MachineShell` binds the stock
shell tool to a selected sandbox, normalized POSIX workspace and signed task scope.
It writes the same private-volume request, streamed stdout/stderr and terminal
metadata as the native shell. Command expiry and combined output allowance travel
through the Machines seam; typed stops map to the native `ToolResult.stop` shape.
The output volume must belong to the signed scope's agent. Qualify the actual
tool calls with the conformance crate's `machines-shell` feature; all examples
use the same vectors for publication before exit, stops and cancellation on drop.
The binding uses the machine's configured disk workspace. It does not create a
kernel filesystem projection or import task volumes.

The shared shell input admits 1..60 seconds (60 by default), at most 16 KiB of
UTF-8 source and 4096-byte relative paths on every executor. This narrows native
shell admission to the portable contract. MachineShell passes source in UTF-8
chunks, each within the existing 4096-byte argument bound, then executes the
original source in bash. Quotes and trailing newlines are preserved. The Machines
60-second timeout, 64-argument and 32768-byte total argv bounds are unchanged.
