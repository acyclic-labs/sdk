import base64
import datetime
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import urllib.parse
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('relaunch', HERE/'relaunch.py')
relaunch = importlib.util.module_from_spec(spec); spec.loader.exec_module(relaunch)
CFG = json.loads((HERE/'relaunch.json').read_text())
NS = 'xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"'


class Fake:
    """EC2/SSM endpoint double: records every decoded request."""
    def __init__(self, fleets):
        self.calls = []; self.fleets = list(fleets)

    def __call__(self, region, service, body, headers):
        if service == 'ssm': return json.dumps({'Parameter': {'Value': 'ami-0123'}}).encode()
        query = dict(urllib.parse.parse_qsl(body.decode())); action = query['Action']
        self.calls.append((region, action, query))
        reply = {
            'DescribeVpcs': '<vpcSet><item><vpcId>vpc-1</vpcId></item></vpcSet>',
            'DescribeSubnets': ''.join(f'<item><subnetId>subnet-{az}</subnetId><availabilityZone>{az}</availabilityZone></item>'
                                       for r in CFG['regions'] if r['region'] == region for az in r['azs'][:2]),
            'DescribeSecurityGroups': '<securityGroupInfo><item><groupId>sg-1</groupId></item></securityGroupInfo>',
            'DescribeLaunchTemplates': '<launchTemplates><item><launchTemplateName>x</launchTemplateName></item></launchTemplates>',
            'CreateLaunchTemplateVersion': '<launchTemplateVersion><versionNumber>7</versionNumber></launchTemplateVersion>',
        }.get(action)
        if action == 'GetSpotPlacementScores': raise relaunch.AwsError('UnauthorizedOperation', 'not permitted')
        if action == 'CreateFleet':
            outcome = self.fleets.pop(0)
            if isinstance(outcome, Exception): raise outcome
            reply = outcome
        return f'<R {NS}>{reply or ""}</R>'.encode()


class RelaunchTests(unittest.TestCase):
    def test_signature_matches_the_aws_sigv4_vector(self):
        # AWS SigV4 test suite "get-vanilla".
        headers = relaunch.sign('GET', 'example.amazonaws.com', 'us-east-1', 'service', b'', {},
                                'AKIDEXAMPLE', 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY',
                                datetime.datetime(2015, 8, 30, 12, 36, tzinfo=datetime.timezone.utc))
        self.assertEqual(headers['authorization'],
                         'AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, '
                         'SignedHeaders=host;x-amz-date, '
                         'Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31')

    def test_candidates_rank_by_live_score_then_sku_and_az(self):
        static = relaunch.candidates(CFG)
        self.assertEqual([r for r, _ in static], ['us-west-2', 'us-east-2', 'us-east-1'])
        self.assertEqual(static[0][1][0], (0, 'c8a.48xlarge', 'us-west-2d'))
        self.assertEqual(len(static[0][1]), len(CFG['types'])*4)
        scored = relaunch.candidates(CFG, {'us-east-1c': 9, 'us-west-2d': 3})
        self.assertEqual(scored[0][0], 'us-east-1')
        self.assertEqual(scored[0][1][:2], [(0, 'c8a.48xlarge', 'us-east-1c'), (1, 'c8a.metal-48xl', 'us-east-1c')])

    def test_capacity_moves_to_next_region_and_other_errors_stop(self):
        capacity = '<errorSet><item><errorCode>InsufficientInstanceCapacity</errorCode><errorMessage>none</errorMessage></item></errorSet>'
        for fleets, expect in [([capacity, '<fleetInstanceSet><item><instanceIds><item>i-0abc</item></instanceIds></item></fleetInstanceSet>'], 'us-east-2'),
                               ([relaunch.AwsError('UnauthorizedOperation', 'no')], None)]:
            fake = Fake(fleets); seen = []
            with patch.object(relaunch, 'TRANSPORT', fake), patch.object(relaunch, 'alert', lambda k, t: seen.append(k)):
                cfg = json.loads(json.dumps(CFG))
                plan = relaunch.candidates(cfg)
                region = None
                for name, overrides in plan:
                    try:
                        _, by_az, version = relaunch.ensure_region(name, cfg, '203.0.113.9', 'ssh-ed25519 AAAA test', 'dXNlcg==')
                        relaunch.launch(name, relaunch.fleet_params(cfg['name'], version, overrides, by_az, cfg))
                        region = name; break
                    except relaunch.AwsError as error:
                        if error.code in relaunch.CAPACITY: continue
                        region = error.code; break
            fleets = [q for r, a, q in fake.calls if a == 'CreateFleet']
            self.assertEqual(region, expect or 'UnauthorizedOperation')
            first = fleets[0]
            self.assertEqual((first['Type'], first['SpotOptions.AllocationStrategy'],
                              first['TargetCapacitySpecification.DefaultTargetCapacityType']),
                             ('instant', 'capacity-optimized-prioritized', 'spot'))
            # Only AZs with a default subnet become overrides, in rank order.
            self.assertEqual(first['LaunchTemplateConfigs.1.Overrides.1.SubnetId'], 'subnet-us-west-2d')
            self.assertEqual(first['LaunchTemplateConfigs.1.Overrides.1.Priority'], '0')
            self.assertNotIn('subnet-us-west-2b', [v for k, v in first.items() if k.endswith('SubnetId')])
            self.assertEqual(first['LaunchTemplateConfigs.1.LaunchTemplateSpecification.Version'], '7')
            template = [q for r, a, q in fake.calls if a == 'CreateLaunchTemplateVersion'][0]
            self.assertEqual((template['LaunchTemplateData.BlockDeviceMapping.2.Ebs.VolumeSize'],
                              template['LaunchTemplateData.MetadataOptions.HttpTokens'],
                              template['LaunchTemplateData.ImageId']), ('1536', 'required', 'ami-0123'))

    def test_user_data_renders_within_the_ec2_limit(self):
        encoded = relaunch.user_data(b'-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n', 'ssh-ed25519 AAAA var')
        text = base64.b64decode(encoded).decode()
        self.assertNotIn('@HOST_KEY_B64@', text); self.assertIn('ssh-ed25519 AAAA var', text)
        self.assertLessEqual(len(encoded), relaunch.USER_DATA_LIMIT)
        subprocess.run(['bash', '-n'], input=text.encode(), check=True)

    def test_host_pin_replaces_only_bb2_entries(self):
        with tempfile.TemporaryDirectory() as temporary:
            d = Path(temporary)
            (d/'known_hosts').write_text('bb2 ssh-ed25519 OLD\nother ssh-ed25519 KEEP\n')
            (d/'config').write_text('Host buildbox\n  HostKeyAlias bb2\n  HostName 1.1.1.1\nHost bb2\n  HostName 1.1.1.1\nHost ivar\n  HostName 9.9.9.9\n')
            relaunch.pin_host('203.0.113.7', ['ssh-ed25519', 'NEW'], d)
            self.assertEqual((d/'known_hosts').read_text(), 'other ssh-ed25519 KEEP\nbb2 ssh-ed25519 NEW\n')
            self.assertEqual((d/'config').read_text().count('HostName 203.0.113.7'), 2)
            self.assertIn('HostName 9.9.9.9', (d/'config').read_text())


class CommitTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp()); ssh = self.dir/'ssh'; ssh.mkdir()
        (ssh/'config').write_text('Host bb2\n  HostName 198.51.100.1\n'); (ssh/'known_hosts').write_text('bb2 ssh-ed25519 OLD\n')
        (self.dir/'instance.env').write_text('AWS_DEFAULT_REGION=us-west-2\nBB2_INSTANCE_ID=i-old\nBB2_SECURITY_GROUP_ID=sg-1\n')
        self.terminated = []
        def ec2(region, action, params):
            if action == 'TerminateInstances': self.terminated.append(params['InstanceId.1'])
        for name, value in {'CONFIG_DIR': self.dir, 'SSH_DIR': ssh, 'RETIRE': self.dir/'retire.list', 'ec2': ec2,
                            'instance_state': lambda r, i: ('running', '203.0.113.7', None), 'wait': lambda f, s, t: f(),
                            'ssh': lambda *a, **k: subprocess.CompletedProcess([], 0), 'provision': lambda s: None,
                            'alert': lambda k, t: None}.items():
            patcher = patch.object(relaunch, name, value); patcher.start(); self.addCleanup(patcher.stop)
        self.old = relaunch.env_file(self.dir/'instance.env')

    def test_failed_canary_restores_old_coordinates_and_retires_the_replacement(self):
        before = {p: p.read_bytes() for p in [self.dir/'instance.env', self.dir/'ssh/config', self.dir/'ssh/known_hosts']}
        with patch.object(relaunch, 'canary', side_effect=RuntimeError('canary failed')), self.assertRaises(RuntimeError):
            relaunch.commit('us-east-2', 'i-new', 'sg-2', ['ssh-ed25519', 'NEW'], None, self.old)
        self.assertEqual({p: p.read_bytes() for p in before}, before)
        self.assertEqual((self.terminated, relaunch.retirements()), (['i-new'], []))

    def test_success_switches_then_retires_predecessor_and_failed_retirement_persists(self):
        def refuse(region, action, params):
            if action == 'TerminateInstances': raise relaunch.AwsError('UnauthorizedOperation', 'no')
        with patch.object(relaunch, 'canary', lambda: None), patch.object(relaunch, 'ec2', refuse):
            relaunch.commit('us-east-2', 'i-new', 'sg-2', ['ssh-ed25519', 'NEW'], None, self.old)
        self.assertEqual(relaunch.env_file(self.dir/'instance.env')['BB2_INSTANCE_ID'], 'i-new')
        self.assertIn('203.0.113.7', (self.dir/'ssh/config').read_text())
        self.assertEqual(relaunch.retirements(), [('us-west-2', 'i-old')])  # Retried by every later run.
        relaunch.retire_pending(keep={'i-new'})
        self.assertEqual((self.terminated, relaunch.retirements()), (['i-old'], []))


if __name__ == '__main__':
    unittest.main()
