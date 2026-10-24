# bb2 tool installation

All lanes and the efficiency loop must install bb2 tools through:

```
sudo /home/var/bin/bb-install.py --source /mnt/bb/<candidate>/scripts/buildbox
# prints /mnt/bb/tmp/bb-install-<qualified-generation>
# after acquiring the idle PR mkdir lock:
sudo python3 /mnt/bb/tmp/bb-install-<qualified-generation>/bb-install.py --publish /mnt/bb/tmp/bb-install-<qualified-generation>
```

For a change confined to selected tools, add `--only bb_target_store.py
bb-install.py` to publication. The complete frozen candidate still qualifies;
only the named tools are replaced, and the manifest records the actual live
hashes of all tools. Qualification records installed predecessors. Publication
refuses a selected destination that changed afterward, preserving another
lane's newer deployment. Unselected tools are preserved, even if they advanced
during qualification. The Cargo source-namespace controls are mandatory in the
installer's qualification suite.

The installer freezes a root-owned candidate, runs the receipt, runtime,
admission and publication controls as `var`, and publishes only after they
pass. Publication writes a temporary file beside the destination, flushes it,
renames it and flushes the directory. Failed qualification changes no tool.
`installed-tools.json` records the qualified hashes. Bin directories and tools
are root-owned and unwritable by the job/lane account; direct writes fail.
The canonical store is `/opt/buildbox/bin`, below protected root parents.
`home-var-bin.mount` pins the legacy `/home/var/bin` entry to that store, so a
lane cannot rename it through its writable HOME parent. The entry is read-only;
publication writes through the protected canonical store. The mount is enabled
at boot. The installer verifies that the entry and store have the same inode.
Do not use sudo to bypass this boundary. The installer is the only privileged
publication path. Existing jobs keep their running interpreter/inode.

Take the coordinator's idle `Q:/lanes/pr-gate/.lock` mkdir lock for the final
publication, releasing it immediately afterward. Qualification can happen
before this lock is acquired. Never interrupt a running gate. Daemon upgrades
must drain existing leases before stopping the idle daemon; replacing its file
does not replace already loaded code.

The current tool list is the SDK-owned runtime set. Another tool must be added
to the installer's explicit list with its required checks before installation;
an unreviewed filename cannot silently become a privileged tool.

Cargo's read-only metadata/tree/locate-project/pkgid/verify-project/read-manifest
commands bypass build tickets centrally in bb-admit. They retain the same
receipt and job HOME boundary. Callers need no tenant-specific copy of this
policy. Wrapper executables (including script interpreters) are resolved at
admission; an unavailable wrapper returns typed admission-INFRA (224).

Admission records every caller resource limit (both soft and hard) before
crossing the daemon boundary. The scope launcher restores that exact set as
root before dropping to `var`, without a PAM session resetting it again. The
HOME namespace repeats the restoration before its final privilege drop.
Public shell entries capture before starting the Node receipt wrapper: libuv
otherwise raises soft nofile to hard nofile before admission observes it. A
nested entry captures its own current limits, preserving deliberate changes.
There is no fixed nofile limit. Terminal runtime receipts record both the
requested and execution limits; mismatches or malformed limits fail as INFRA.
