# acyclic-harness-workspace

The selected checkout, its volumes and the tools bound to it: the workspace
layer below `acyclic-harness-local`'s host.

`LocalWorkspace` owns the selected view, its mounts and its publication
records. `CheckoutTools` binds the default commands to that selected,
identity-fenced checkout. `HostWorkspace` is the native host's view of the same
workspace, with no execution authority of its own.
