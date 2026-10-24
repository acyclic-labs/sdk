#!/usr/bin/env python3
"""Prepare and verify the exact main checkout without dispatching any build."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys

spec=importlib.util.spec_from_file_location('gate_prepare',Path(__file__).with_name('gate-prepare.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

def prepare(repo,worktree,state,head,runner,verify=False):
    gate=module.Gate(repo,worktree,state,state/'unused-alerts')
    gate.validate()
    record=state/'.main-prepared.json'
    if verify:
        expected=json.loads(record.read_text())
        if expected['head']!=head:raise module.PreparationError('prepared input does not match requested main')
    else:
        gate.checked(worktree,'checkout','-q','--detach','-f',head)
        gate.checked(worktree,'clean','-qfdx','-e','target')
        temporary=worktree/'.gate-checks.sh.new'
        shutil.copyfile(runner,temporary)
        expected={'head':head,'runner_sha256':hashlib.sha256(runner.read_bytes()).hexdigest()}
        if hashlib.sha256(temporary.read_bytes()).hexdigest()!=expected['runner_sha256']:
            raise module.PreparationError('runner copy does not match source')
        os.replace(temporary,worktree/'.gate-checks.sh')
    actual=gate.checked(worktree,'rev-parse','--verify','HEAD^{commit}')
    if actual!=head:raise module.PreparationError(f'main HEAD mismatch: requested={head} actual={actual}')
    if hashlib.sha256((worktree/'.gate-checks.sh').read_bytes()).hexdigest()!=expected['runner_sha256']:
        raise module.PreparationError('prepared runner changed')
    if not verify:
        temporary=record.with_suffix('.new');temporary.write_text(json.dumps(expected)+'\n');os.replace(temporary,record)
    print('GATE_MAIN_VERIFIED' if verify else 'GATE_MAIN_PREPARED',json.dumps(expected,sort_keys=True))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('repo',type=Path);p.add_argument('worktree',type=Path);p.add_argument('state',type=Path)
    p.add_argument('head');p.add_argument('runner',type=Path);p.add_argument('--verify',action='store_true')
    a=p.parse_args()
    try:prepare(a.repo,a.worktree,a.state,a.head,a.runner,a.verify)
    except (OSError,ValueError,module.PreparationError) as error:
        print('error: GATE_RUNNER_INFRA main preparation: '+str(error),file=sys.stderr);sys.exit(6)
