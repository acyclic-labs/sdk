import concurrent.futures
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('installer',HERE/'bb-install.py')
installer=importlib.util.module_from_spec(spec);spec.loader.exec_module(installer)


class InstallTests(unittest.TestCase):
    def test_fifty_concurrent_exec_jobs_see_complete_generations(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool'
            def content(value):return f'#!/bin/sh\nprintf "{value}\\n"\n'.encode()
            installer.publish(tool,content('old'))
            barrier=threading.Barrier(51)
            def execute(_):
                barrier.wait()
                for _ in range(8):
                    run=subprocess.run([str(tool)],capture_output=True)
                    self.assertEqual(run.returncode,0,run.stderr)
                    self.assertIn(run.stdout,[b'old\n',b'new\n'])
            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as workers:
                futures=[workers.submit(execute,i) for i in range(50)]
                # Publication is a separate process, as in production. A fork
                # from a writer thread can inherit another thread's open write
                # fd before CLOEXEC and manufacture ETXTBSY unrelated to rename.
                writer=subprocess.Popen([sys.executable,'-c',
                    'import importlib.util,sys;from pathlib import Path;'
                    's=importlib.util.spec_from_file_location("i",sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);'
                    '[m.publish(Path(sys.argv[2]),bytes.fromhex(sys.argv[3+n%2])) for n in range(80)]',
                    str(HERE/'bb-install.py'),str(tool),content('old').hex(),content('new').hex()])
                barrier.wait()
                for future in futures:future.result()
                self.assertEqual(writer.wait(),0)
            # Deterministic old in-place publication window: exec sees an empty
            # file between truncate and write, even though the path exists.
            tool.chmod(0o755)
            with tool.open('wb'):
                with self.assertRaises(OSError):subprocess.run([str(tool)],check=True)

    def test_failed_qualification_never_publishes(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool';installer.publish(tool,b'original')
            with patch.object(installer.subprocess,'run',side_effect=subprocess.CalledProcessError(1,['test'])):
                with self.assertRaises(subprocess.CalledProcessError):installer.qualify(Path(temporary),'var')
            self.assertEqual(tool.read_bytes(),b'original')
            self.assertEqual(list(Path(temporary).iterdir()),[tool])

    def test_privileged_publication_refuses_direct_lane_edits(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool'
            script='from pathlib import Path;import sys,os;p=Path(sys.argv[1]);p.write_bytes(b"qualified");os.chown(p,0,0);p.chmod(0o555)'
            subprocess.run(['sudo','-n','python3','-c',script,str(tool)],check=True)
            with self.assertRaises(PermissionError):tool.write_bytes(b'partial')
            self.assertEqual(tool.read_bytes(),b'qualified')
            # The former var-owned mode admits exactly the same write.
            unprotected=Path(temporary)/'unprotected';unprotected.write_bytes(b'old');unprotected.write_bytes(b'partial')
            self.assertEqual(unprotected.read_bytes(),b'partial')


if __name__=='__main__':unittest.main()
