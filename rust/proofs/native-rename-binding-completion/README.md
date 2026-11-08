# Native rename binding completion

Run `node rust/proofs/qualify.mjs native-rename-binding-completion`.

The existing real-source cache control exercises accepted file and directory rename acknowledgements, a cached destination miss, and replacement of an admitted destination. It mutates the same node externally, executes the production invalidation pass, and requires the unchanged name to remain bound. A subsequent external rename must still invalidate that original name.

The retained production mutation removes completion-stamp adoption from the actual prepared binding installation, restoring the previous retention of pre-rename stamps. It leaves the source result, permissions, cached-miss retirement and oracle unchanged. The qualifier requires the original unchanged-name assertion to fail, restores the exact production source, then requires the same oracle to pass.

Rename returns its existing installed candidate result through the shared source adapters. FUSE reuses that stamp for each already-prepared moved binding and uses the shared accepted-entry transition to retire only the destination miss. There is no post-effect clock read, new wait, timer, task, syscall, allocation, durability barrier or payload copy. The existing preparation still traverses projected bindings and directory streams; this control does not establish constant whole-rename cost or real kernel mount survival.
