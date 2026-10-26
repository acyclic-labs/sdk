"""Isolated real-kernel lease factory for tooling controls, never live policy.

Freeze the production adapter under root custody; relocate only its two storage
roots. Tests share this factory instead of inventing admission/cgroup shims.
"""
import json
import os
from pathlib import Path
import subprocess
import shutil
import uuid

HERE=Path(__file__).resolve().parent


def stage_worker_bundle(destination, cpu_adapter=None):
    """Keep isolated workers import-complete as the shared runtime evolves."""
    for dependency in HERE.glob('*.py'):
        shutil.copyfile(dependency,Path(destination)/dependency.name)
    if cpu_adapter is not None:
        shutil.copyfile(cpu_adapter,Path(destination)/'cpu_leases.py')


class KernelFixture:
    @staticmethod
    def environment(extra=None):
        # A fixture's constructor must never append lease accounting to its
        # enclosing real build-slot receipt. Tests can supply their own meta.
        env=dict(os.environ)
        env.pop('BUILD_SLOT_RECEIPT_META',None)
        return {**env,**(extra or {})}

    def __init__(self, source=HERE):
        name='bb-cpu-test-'+uuid.uuid4().hex
        self.control=Path('/run')/name
        self.group=Path('/sys/fs/cgroup/bbjobs.slice')/name
        script='''import sys,re
from pathlib import Path
source,root,group=map(Path,sys.argv[1:]);root.mkdir(mode=0o755)
for name in ['admission_policy.py','cpu_leases.py','bb-admit.py','bb-job.py',
             'job_exports.py','bb_target_store.py','bb-cache.py','input_closure.py','job_caps.py']:
 data=(source/name).read_text()
 if name=='cpu_leases.py':
  data=re.sub(r'^CGROUP_ROOT = .*$',lambda m:'CGROUP_ROOT = Path('+repr(str(group))+')',data,flags=re.M)
  data=re.sub(r'^CONTROL_ROOT = .*$',lambda m:'CONTROL_ROOT = Path('+repr(str(root))+')',data,flags=re.M)
 file=root/name;file.write_text(data);file.chmod(0o444)
'''
        subprocess.run(['sudo','-n','/usr/bin/python3','-B','-c',script,str(source),str(self.control),str(self.group)],check=True)
        self.module=self.control/'cpu_leases.py'

    def close(self):
        script='''import sys,shutil
from pathlib import Path
sys.path.insert(0,sys.argv[1]);import cpu_leases as c
for record in c.CONTROL_ROOT.glob('*.json'):c.observe(record.stem,True)
if c.CGROUP_ROOT.exists():
 for child in sorted(c.CGROUP_ROOT.rglob('*'),key=lambda p:len(p.parts),reverse=True):
  if child.is_dir():child.rmdir()
 c.CGROUP_ROOT.rmdir()
shutil.rmtree(c.CONTROL_ROOT)
'''
        subprocess.run(['sudo','-n','/usr/bin/python3','-B','-c',script,str(self.control)],check=True)

    def copy_adapter(self,destination):
        """Unprivileged workers import the same frozen kernel-root identity."""
        Path(destination).write_bytes(self.module.read_bytes())

    def bootstrap(self,argv,env=None,index=0,popen=False,**kwargs):
        from admission_policy import capture_limits,LIMIT_ENV
        env=self.environment({**(env or {}),LIMIT_ENV:json.dumps(capture_limits())})
        # Import the immutable relocated adapter before test modules load the
        # source runtime; copied workers retain its exact cgroup identity.
        code='import sys,runpy;from pathlib import Path;sys.path.insert(0,sys.argv[1]);sys.path.insert(1,str(Path(sys.argv[2]).resolve().parent));import cpu_leases;sys.argv=sys.argv[2:];runpy.run_path(sys.argv[0],run_name="__main__")'
        command=['sudo','-n','--preserve-env','/usr/bin/python3','-B',str(self.module),
            'exec',str(index),uuid.uuid4().hex,'graphcoder','--','/usr/bin/python3','-B','-c',code,str(self.control),*argv]
        return subprocess.Popen(command,env=env,**kwargs) if popen else subprocess.call(command,env=env,**kwargs)

    def __enter__(self):return self
    def __exit__(self,*args):self.close()


if __name__=='__main__':
    import sys
    with KernelFixture() as fixture:raise SystemExit(fixture.bootstrap(sys.argv[1:]))
