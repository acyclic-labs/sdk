"""Host half of the real private-proc receipt recovery proof (bb2 only)."""
import json
import os
from pathlib import Path
import sys

root = Path(sys.argv[1])
assert root.parent == Path('/mnt/bb/jobs') and root.name.startswith('approvals-namespace-live-')
root.mkdir(mode=0o700)
for name in ['owner', 'witness']:
    os.mkfifo(root / name, mode=0o600)
print('HOST_NAMESPACE_PROOF_READY', root, flush=True)
with (root / 'owner').open() as channel:
    owner = json.load(channel)
assert owner['pid_namespace'] == os.readlink('/proc/self/ns/pid')
fields = Path(f'/proc/{owner["pid"]}/stat').read_text().split(') ')[-1].split()
assert fields[0] != 'Z'
identity = Path('/proc/sys/kernel/random/boot_id').read_text().strip() + ':' + fields[19]
assert identity == owner['identity'], 'host owner exited or was reused'
with (root / 'witness').open('w') as channel:
    json.dump({'owner': owner, 'live_in_host_namespace': True}, channel)
print('HOST_NAMESPACE_LIVE_OWNER_VERIFIED', json.dumps(owner), flush=True)
