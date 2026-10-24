"""Exercise the actual Cargo proxy's kernel lease ownership."""
import fcntl
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import bb_cargo_cache as cache


class LocalTargetLease(unittest.TestCase):
    def test_independent_jobs_with_the_same_virtual_target_do_not_block(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / 'work' / 'target'
            target.mkdir(parents=True)
            first = cache.local_target_lock(root / 'first', target)
            second = cache.local_target_lock(root / 'second', target)
            self.assertNotEqual(first, second)
            with first.open('a') as held:
                fcntl.flock(held, fcntl.LOCK_EX)
                result = self.contender(second)
            self.assertEqual(result.returncode, 0, result.stderr)
            # The former parent-based path aliases distinct job identities.
            old_first = target.parent / ('.' + target.name + '.bb-store-lock')
            old_second = target.parent / ('.' + target.name + '.bb-store-lock')
            with old_first.open('a') as held:
                fcntl.flock(held, fcntl.LOCK_EX)
                self.assertEqual(self.contender(old_second).returncode, 73)

    def test_one_job_target_and_its_aliases_share_one_kernel_lease(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / 'target'
            target.mkdir()
            original = cache.local_target_lock(root / 'job', target)
            alias = cache.local_target_lock(root / 'job', target / '..' / 'target')
            self.assertEqual(original, alias)
            self.assertTrue(original.is_relative_to(root / 'job'))
            self.assertFalse(original.is_relative_to(target))
            with original.open('a') as held:
                fcntl.flock(held, fcntl.LOCK_EX)
                self.assertEqual(self.contender(alias).returncode, 73)
            self.assertEqual(self.contender(alias).returncode, 0)

    @staticmethod
    def contender(path):
        return subprocess.run(
            [sys.executable, '-c', '''import fcntl,sys
with open(sys.argv[1], 'a') as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(73)
''', str(path)], capture_output=True, text=True, check=False,
        )


if __name__ == '__main__':
    unittest.main()
