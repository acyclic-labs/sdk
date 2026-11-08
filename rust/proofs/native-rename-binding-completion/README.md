# Native rename binding completion

Run `node rust/proofs/qualify.mjs native-rename-binding-completion`.

The existing real-source cache control exercises accepted file and directory rename acknowledgements, a cached destination miss, and replacement of an admitted destination. It mutates the same node externally, executes the production invalidation pass, and requires the unchanged name to remain bound. A subsequent external rename must still invalidate that original name.

The retained production mutation removes completion-stamp adoption from the actual prepared binding installation, restoring the previous retention of pre-rename stamps. It leaves the source result, permissions, cached-miss retirement and oracle unchanged. The qualifier requires the original unchanged-name assertion to fail, restores the exact production source, then requires the same oracle to pass. The restored phase also runs the retained concurrent-FORGET control and its resurrection inverse; both obtain the stamp from the real source rename.

Rename returns only its existing installed `ViewStamp` through the shared source adapters: one `u64`, with no optional metadata carrier. FUSE reuses that stamp for each already-prepared moved binding verified in the final candidate and uses the shared accepted-entry transition to retire only the destination miss. There is no post-effect clock read, new wait, timer, task or durability barrier. Final-candidate identity checks use the existing metered path buffers and checkout traversal; see `../native-rename-reviewed-completion/README.md` for their cost and negative controls. The existing preparation still traverses projected bindings and directory streams; this control does not establish constant whole-rename cost or real kernel mount survival.
