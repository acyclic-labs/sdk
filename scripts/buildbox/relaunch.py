#!/usr/bin/env python3
"""bb2 eviction relaunch: a fresh spot box wherever Zen 5 capacity exists.

The old disk is not needed, so an evicted or capacity-starved bb2 is replaced
rather than waited for. Candidates are every (region, AZ, SKU) in
relaunch.json, reordered by live spot placement scores. One instant
CreateFleet per region uses capacity-optimized-prioritized across all of that
region's SKU/AZ overrides. The region's default VPC, security group, key pair
and launch template are created on demand. Fresh disks are provisioned by
bootstrap.sh (user-data, public sources) and host-install.sh (over ssh: host
plumbing plus the exact live tool generation from `snapshot`). The old
instance is retired only after a script job and a cargo job pass.

  relaunch.py plan                 ranked candidates (with live scores when permitted)
  relaunch.py relaunch [--dry-run] launch, provision, canary, retire the old box
                                   (--dry-run prepares the region's SG/key/template, launches nothing)
  relaunch.py snapshot             save the live tool generation (on change)

Runs in WSL as var. Credentials come from ~/.config/buildbox/aws.env and are
never printed; instance coordinates live in instance.env, which buildbox-ctl
sources after aws.env.
"""
import base64
import datetime
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
CONFIG_DIR = Path(os.environ.get('BUILDBOX_CONFIG', '/home/var/.config/buildbox'))
SSH_DIR = Path(os.environ.get('BUILDBOX_SSH_DIR', '/home/var/.ssh/buildbox'))
ALERTS = Path('/mnt/c/Users/varun/.codex-fleet/alerts.log')
SDK = Path(os.environ.get('BUILDBOX_SDK', '/mnt/q/sdk'))
# A region without this capacity moves on to the next region; anything else stops.
CAPACITY = {'InsufficientInstanceCapacity', 'InsufficientCapacity', 'MaxSpotInstanceCountExceeded',
            'SpotMaxPriceTooLow', 'UnfulfillableCapacity', 'InsufficientFreeAddressesInSubnet',
            'Unsupported', 'NoDefaultVpc'}
USER_DATA_LIMIT = 16384


class AwsError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(code+': '+message); self.code = code


def env_file(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        key, sep, value = line.strip().partition('=')
        if sep and not key.startswith('#'): values[key.removeprefix('export ').strip()] = value.strip().strip('"\'')
    return values


def settings():
    values = env_file(CONFIG_DIR/'aws.env')
    if (CONFIG_DIR/'instance.env').exists(): values.update(env_file(CONFIG_DIR/'instance.env'))
    return values


def sign(method, host, region, service, body, headers, key, secret, now, path='/', query=''):
    """AWS Signature Version 4 over the given headers (host and x-amz-date added)."""
    stamp = now.strftime('%Y%m%dT%H%M%SZ'); day = stamp[:8]
    headers = {**{k.lower(): v for k, v in headers.items()}, 'host': host, 'x-amz-date': stamp}
    names = sorted(headers)
    canonical = '\n'.join([method, path, query, ''.join(f'{n}:{headers[n].strip()}\n' for n in names),
                           ';'.join(names), hashlib.sha256(body).hexdigest()])
    scope = f'{day}/{region}/{service}/aws4_request'
    to_sign = '\n'.join(['AWS4-HMAC-SHA256', stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    signing = ('AWS4'+secret).encode()
    for part in [day, region, service, 'aws4_request']: signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing, to_sign.encode(), hashlib.sha256).hexdigest()
    headers['authorization'] = (f'AWS4-HMAC-SHA256 Credential={key}/{scope}, SignedHeaders={";".join(names)}, '
                                f'Signature={signature}')
    return headers


def send(region, service, body, headers):
    values = settings(); host = f'{service}.{region}.amazonaws.com'
    signed = sign('POST', host, region, service, body, headers, values['AWS_ACCESS_KEY_ID'],
                  values['AWS_SECRET_ACCESS_KEY'], datetime.datetime.now(datetime.timezone.utc))
    request = urllib.request.Request('https://'+host+'/', data=body, headers=signed, method='POST')
    try:
        with urllib.request.urlopen(request, timeout=60) as response: return response.read()
    except urllib.error.HTTPError as error:
        payload = error.read()
        if service == 'ec2':
            root = strip(ET.fromstring(payload))
            raise AwsError(root.findtext('.//Code') or str(error.code), root.findtext('.//Message') or '') from None
        detail = json.loads(payload or b'{}')
        raise AwsError(detail.get('__type', str(error.code)).split('#')[-1], detail.get('message', '')) from None


TRANSPORT = send


def strip(root):
    for element in root.iter(): element.tag = element.tag.rpartition('}')[2]
    return root


def ec2(region, action, params=None):
    query = {'Action': action, 'Version': '2016-11-15', **{k: str(v) for k, v in (params or {}).items()}}
    body = urllib.parse.urlencode(sorted(query.items()), quote_via=urllib.parse.quote, safe='-_.~').encode()
    payload = TRANSPORT(region, 'ec2', body, {'content-type': 'application/x-www-form-urlencoded; charset=utf-8'})
    return strip(ET.fromstring(payload))


def ssm_parameter(region, name):
    payload = TRANSPORT(region, 'ssm', json.dumps({'Name': name}).encode(),
                        {'content-type': 'application/x-amz-json-1.1', 'x-amz-target': 'AmazonSSM.GetParameter'})
    return json.loads(payload)['Parameter']['Value']


def tags(prefix, resource, values):
    params = {f'{prefix}.ResourceType': resource}
    for n, (key, value) in enumerate(sorted(values.items()), 1):
        params[f'{prefix}.Tag.{n}.Key'] = key; params[f'{prefix}.Tag.{n}.Value'] = value
    return params


def placement_scores(cfg):
    """{az name: score} from GetSpotPlacementScores; {} when not permitted."""
    first = cfg['regions'][0]['region']
    params = {'TargetCapacity': 1, 'SingleAvailabilityZone': 'true'}
    for n, name in enumerate(cfg['types'], 1): params[f'InstanceType.{n}'] = name
    for n, region in enumerate(cfg['regions'], 1): params[f'RegionName.{n}'] = region['region']
    try:
        scores = ec2(first, 'GetSpotPlacementScores', params)
        names = {}
        for region in cfg['regions']:
            zones = ec2(region['region'], 'DescribeAvailabilityZones')
            names.update({z.findtext('zoneId'): z.findtext('zoneName') for z in zones.iter('item') if z.findtext('zoneId')})
    except AwsError as error:
        print('placement scores unavailable ('+error.code+'); static ranking', file=sys.stderr); return {}
    return {names.get(i.findtext('availabilityZoneId'), i.findtext('availabilityZoneId')): int(i.findtext('score') or 0)
            for i in scores.iter('item') if i.findtext('availabilityZoneId')}


def candidates(cfg, scores=None):
    """[(region, [(priority, type, az)])]: AZs by live score, then SKU and AZ rank."""
    scores = scores or {}; plan = []
    for region in cfg['regions']:
        items = sorted(((-scores.get(az, 0), t_rank, a_rank), name, az)
                       for t_rank, name in enumerate(cfg['types']) for a_rank, az in enumerate(region['azs']))
        plan.append((region['region'], [(n, name, az) for n, (_, name, az) in enumerate(items)]))
    if scores: plan.sort(key=lambda entry: -max(scores.get(az, 0) for _, _, az in entry[1]))
    return plan


def user_data(host_key, authorized_key):
    text = (HERE/'bootstrap.sh').read_text()
    text = text.replace('@HOST_KEY_B64@', base64.b64encode(host_key).decode()).replace('@AUTHORIZED_KEY@', authorized_key)
    if '@HOST_KEY_B64@' in text or '@AUTHORIZED_KEY@' in text: raise ValueError('unrendered bootstrap placeholder')
    encoded = base64.b64encode(text.encode()).decode()
    if len(encoded) > USER_DATA_LIMIT: raise ValueError(f'user-data is {len(encoded)} B; limit {USER_DATA_LIMIT}')
    return encoded


def ensure_region(region, cfg, my_ip, public_key, data):
    """Default VPC subnets, security group, key pair and launch template version."""
    vpcs = ec2(region, 'DescribeVpcs', {'Filter.1.Name': 'isDefault', 'Filter.1.Value.1': 'true'})
    vpc = vpcs.findtext('.//vpcId')
    if not vpc: raise AwsError('NoDefaultVpc', region)
    subnets = ec2(region, 'DescribeSubnets', {'Filter.1.Name': 'vpc-id', 'Filter.1.Value.1': vpc,
                                             'Filter.2.Name': 'default-for-az', 'Filter.2.Value.1': 'true'})
    by_az = {i.findtext('availabilityZone'): i.findtext('subnetId') for i in subnets.iter('item') if i.findtext('subnetId')}
    name = cfg['name']
    groups = ec2(region, 'DescribeSecurityGroups', {'Filter.1.Name': 'group-name', 'Filter.1.Value.1': name,
                                                   'Filter.2.Name': 'vpc-id', 'Filter.2.Value.1': vpc})
    group = groups.findtext('.//groupId')
    if not group:
        group = ec2(region, 'CreateSecurityGroup', {'GroupName': name, 'GroupDescription': 'bb2 build box ssh', 'VpcId': vpc,
                                                    **tags('TagSpecification.1', 'security-group', cfg['tags'])}).findtext('groupId')
    try:
        ec2(region, 'AuthorizeSecurityGroupIngress', {'GroupId': group, 'IpPermissions.1.IpProtocol': 'tcp',
            'IpPermissions.1.FromPort': 22, 'IpPermissions.1.ToPort': 22,
            'IpPermissions.1.IpRanges.1.CidrIp': my_ip+'/32', 'IpPermissions.1.IpRanges.1.Description': 'buildbox-ctl'})
    except AwsError as error:
        if error.code != 'InvalidPermission.Duplicate': raise
    try: ec2(region, 'DescribeKeyPairs', {'KeyName.1': name})
    except AwsError as error:
        if error.code != 'InvalidKeyPair.NotFound': raise
        ec2(region, 'ImportKeyPair', {'KeyName': name, 'PublicKeyMaterial': base64.b64encode(public_key.encode()).decode(),
                                      **tags('TagSpecification.1', 'key-pair', cfg['tags'])})
    prefix = 'LaunchTemplateData.'
    template = {prefix+'ImageId': ssm_parameter(region, cfg['ami_parameter']), prefix+'KeyName': name,
                prefix+'SecurityGroupId.1': group, prefix+'UserData': data,
                prefix+'InstanceInitiatedShutdownBehavior': 'terminate', prefix+'MetadataOptions.HttpTokens': 'required',
                **{prefix+k: v for k, v in {
                    'BlockDeviceMapping.1.DeviceName': '/dev/sda1', 'BlockDeviceMapping.1.Ebs.VolumeSize': cfg['root_gib'],
                    'BlockDeviceMapping.1.Ebs.VolumeType': 'gp3', 'BlockDeviceMapping.1.Ebs.DeleteOnTermination': 'true',
                    'BlockDeviceMapping.2.DeviceName': '/dev/sdb', 'BlockDeviceMapping.2.Ebs.VolumeSize': cfg['data_gib'],
                    'BlockDeviceMapping.2.Ebs.VolumeType': 'gp3', 'BlockDeviceMapping.2.Ebs.Iops': cfg['data_iops'],
                    'BlockDeviceMapping.2.Ebs.Throughput': cfg['data_throughput'],
                    'BlockDeviceMapping.2.Ebs.DeleteOnTermination': 'true'}.items()},
                **tags(prefix+'TagSpecification.1', 'instance', cfg['tags']),
                **tags(prefix+'TagSpecification.2', 'volume', cfg['tags'])}
    try:
        ec2(region, 'DescribeLaunchTemplates', {'LaunchTemplateName.1': name})
        version = ec2(region, 'CreateLaunchTemplateVersion', {'LaunchTemplateName': name, **template})
    except AwsError as error:
        if error.code != 'InvalidLaunchTemplateName.NotFoundException': raise
        version = ec2(region, 'CreateLaunchTemplate', {'LaunchTemplateName': name, **template,
                                                       **tags('TagSpecification.1', 'launch-template', cfg['tags'])})
    return group, by_az, version.findtext('.//versionNumber') or version.findtext('.//latestVersionNumber') or '$Latest'


def fleet_params(name, version, overrides, by_az, cfg, dry_run=False):
    params = {'Type': 'instant', 'TargetCapacitySpecification.TotalTargetCapacity': 1,
              'TargetCapacitySpecification.DefaultTargetCapacityType': 'spot',
              'SpotOptions.AllocationStrategy': 'capacity-optimized-prioritized',
              'SpotOptions.InstanceInterruptionBehavior': 'terminate',
              'LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateName': name,
              'LaunchTemplateConfigs.1.LaunchTemplateSpecification.Version': version,
              **tags('TagSpecification.1', 'fleet', cfg['tags'])}
    n = 0
    for priority, sku, az in overrides:
        if az not in by_az: continue
        n += 1; base = f'LaunchTemplateConfigs.1.Overrides.{n}.'
        params.update({base+'InstanceType': sku, base+'SubnetId': by_az[az], base+'Priority': priority})
    if not n: raise AwsError('Unsupported', 'no default subnet in any candidate AZ')
    if dry_run: params['DryRun'] = 'true'
    return params


def launch(region, params):
    result = ec2(region, 'CreateFleet', params)
    ids = [i.text for i in result.iter('item') if i.text and i.text.startswith('i-')]
    if ids: return ids[0]
    errors = [(e.findtext('errorCode'), e.findtext('errorMessage')) for e in result.iter('item') if e.findtext('errorCode')]
    code = errors[0][0] if errors else 'UnfulfillableCapacity'
    raise AwsError(code, '; '.join(f'{c}: {m}' for c, m in errors) or 'no instance launched')


def alert(kind, text):
    line = f'{datetime.datetime.now(datetime.timezone.utc):%Y-%m-%dT%H:%M:%S.000Z} buildbox {kind} {text}'
    print(line)
    try:
        with ALERTS.open('a') as stream: stream.write(line+'\n')
    except OSError: pass


def atomic_write(path, data, mode=0o600):
    path = Path(path); temporary = path.with_name('.'+path.name+'.'+str(os.getpid()))
    temporary.write_bytes(data); temporary.chmod(mode); os.replace(temporary, path)


def host_identity():
    """The fixed bb2 host key that every relaunched box presents."""
    key = CONFIG_DIR/'host_ed25519'
    if not key.exists():
        subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-C', 'bb2-host', '-f', str(key)], check=True)
    return key.read_bytes(), (CONFIG_DIR/'host_ed25519.pub').read_text().split()[:2]


def pin_host(ip, public, ssh_dir=None):
    ssh_dir = Path(ssh_dir or SSH_DIR)
    known = ssh_dir/'known_hosts'
    lines = [l for l in (known.read_text().splitlines() if known.exists() else []) if l.split(' ', 1)[0] != 'bb2']
    atomic_write(known, ('\n'.join([*lines, 'bb2 '+' '.join(public)])+'\n').encode(), 0o644)
    config = ssh_dir/'config'; out = []; host = None
    for line in config.read_text().splitlines():
        words = line.split()
        if words[:1] == ['Host']: host = words[1] if len(words) > 1 else None
        if words[:1] == ['HostName'] and host in ('bb2', 'buildbox'):
            line = line[:len(line)-len(line.lstrip())]+'HostName '+ip
        out.append(line)
    atomic_write(config, ('\n'.join(out)+'\n').encode(), 0o644)


def ssh(command, data=None, timeout=600):
    return subprocess.run(['ssh', '-F', str(SSH_DIR/'config'), '-o', 'BatchMode=yes', '-o', 'ControlPath=none', 'bb2', command],
                          input=data, capture_output=True, timeout=timeout)


def wait(predicate, seconds, step):
    deadline = time.monotonic()+seconds
    while time.monotonic() < deadline:
        if predicate(): return True
        time.sleep(step)
    return False


def instance_state(region, instance):
    item = ec2(region, 'DescribeInstances', {'InstanceId.1': instance})
    return item.findtext('.//instanceState/name'), item.findtext('.//ipAddress'), item.findtext('.//spotInstanceRequestId')


def provision(snapshot):
    """Phase 2 over ssh: tracked host plumbing from origin/main plus the tool snapshot."""
    archive = subprocess.run(['git', '-C', str(SDK), 'archive', '--format=tar', 'origin/main', 'scripts/buildbox'],
                             capture_output=True, check=True).stdout
    hooks = io.BytesIO()
    with tarfile.open(fileobj=hooks, mode='w') as bundle:
        if (CONFIG_DIR/'bootstrap.d').is_dir(): bundle.add(CONFIG_DIR/'bootstrap.d', arcname='hooks')
    work = ssh('mktemp -d /mnt/bb/tmp/bb-host-XXXXXX').stdout.decode().strip()
    for name, data in [('src.tar', archive), ('tools.tar', snapshot.read_bytes()), ('hooks.tar', hooks.getvalue())]:
        if ssh(f'cat > {work}/{name}', data).returncode: raise RuntimeError('upload failed: '+name)
    result = ssh(f'cd {work} && tar -xf src.tar && mkdir -p hooks && tar -xf hooks.tar && '
                 f'sudo -n bash scripts/buildbox/host-install.sh {work}/scripts/buildbox {work}/tools.tar {work}/hooks', timeout=1200)
    if result.returncode: raise RuntimeError('host-install failed: '+result.stdout.decode()[-1500:]+result.stderr.decode()[-1500:])


def canary():
    """One script job and one cargo job through the normal client path."""
    jobs = [['bash', '-c', 'echo BB_RELAUNCH_SCRIPT_OK'], ['cargo', 'check', '--locked', '-q', '-p', 'acyclic-markdown']]
    for argv in jobs:
        # Exactly bs.ps1's entry: the WSL remote client with forwarded job env names.
        result = subprocess.run(['/home/var/bin/build-slot', '--remote', *argv], cwd=SDK, capture_output=True, timeout=900,
                                env={**os.environ, 'BUILD_SLOT_NO_NPM': '1', 'BUILD_SLOT_LABEL': 'bb2-relaunch-canary',
                                     'BUILD_SLOT_REMOTE_ENV': 'BUILD_SLOT_NO_NPM BUILD_SLOT_LABEL'})
        if result.returncode or (argv[0] == 'bash' and b'BB_RELAUNCH_SCRIPT_OK' not in result.stdout):
            raise RuntimeError('canary failed: '+' '.join(argv)+' rc='+str(result.returncode))


def relaunch(dry_run=False):
    cfg = json.loads((HERE/'relaunch.json').read_text())
    snapshot = CONFIG_DIR/'tools-snapshot.tar'
    if not dry_run and not snapshot.exists(): raise RuntimeError('no tool snapshot; run `relaunch.py snapshot` while bb2 is up')
    old = settings(); my_ip = urllib.request.urlopen('https://api.ipify.org', timeout=10).read().decode().strip()
    host_key, public = host_identity()
    authorized = (SSH_DIR/'bb2_ed25519.pub').read_text().strip()
    data = user_data(host_key, authorized)
    for region, overrides in candidates(cfg, placement_scores(cfg)):
        try:
            group, by_az, version = ensure_region(region, cfg, my_ip, authorized, data)
            instance = launch(region, fleet_params(cfg['name'], version, overrides, by_az, cfg, dry_run))
        except AwsError as error:
            if dry_run and error.code == 'DryRunOperation': alert('BUILDBOX_RELAUNCH', f'dry-run authorized in {region}'); return
            if error.code in CAPACITY: alert('BUILDBOX_RELAUNCH', f'{region}: {error}; next region'); continue
            alert('BUILDBOX_RELAUNCH_BLOCKED', f'{region}: {error}'); raise
        alert('BUILDBOX_RELAUNCH', f'launched {instance} in {region}')
        state = {}
        if not wait(lambda: state.update(zip(['name', 'ip', 'spot'], instance_state(region, instance))) or
                    (state['name'] == 'running' and state['ip']), 300, 5):
            raise RuntimeError(f'{instance} not running with a public IP after 300 s')
        atomic_write(CONFIG_DIR/'instance.env', (f'AWS_DEFAULT_REGION={region}\nBB2_INSTANCE_ID={instance}\n'
                     f'BB2_SECURITY_GROUP_ID={group}\nBB2_PREVIOUS_REGION={old.get("AWS_DEFAULT_REGION", "")}\n'
                     f'BB2_PREVIOUS_INSTANCE_ID={old.get("BB2_INSTANCE_ID", "")}\n').encode())
        pin_host(state['ip'], public)
        if not wait(lambda: ssh('test -e /var/lib/buildbox/bootstrap.done', timeout=60).returncode == 0, 2700, 30):
            raise RuntimeError('phase 1 bootstrap did not finish in 45 min; see /var/log/bb-bootstrap.log')
        provision(snapshot); canary()
        previous, previous_region = old.get('BB2_INSTANCE_ID'), old.get('AWS_DEFAULT_REGION')
        if previous and previous != instance:
            try:
                _, _, request = instance_state(previous_region, previous)
                if request: ec2(previous_region, 'CancelSpotInstanceRequests', {'SpotInstanceRequestId.1': request})
                ec2(previous_region, 'TerminateInstances', {'InstanceId.1': previous})
            except AwsError as error: alert('BUILDBOX_RELAUNCH_BLOCKED', f'retire {previous}: {error}')
        alert('BUILDBOX_RELAUNCH', f'bb2 restored on {instance} ({region}, {state["ip"]})'); return instance
    alert('BUILDBOX_RELAUNCH_BLOCKED', 'no candidate region had capacity'); raise SystemExit(1)


def snapshot():
    """Exact bytes of the live generation; written only when its manifest changes."""
    manifest = ssh('cat /home/var/bin/installed-tools.json', timeout=60)
    if manifest.returncode: raise RuntimeError('bb2 tool manifest unavailable')
    digest = hashlib.sha256(manifest.stdout).hexdigest(); stamp = CONFIG_DIR/'tools-snapshot.sha256'
    if stamp.exists() and stamp.read_text().strip() == digest: return False
    archive = ssh('d=$(readlink -e /opt/buildbox/current 2>/dev/null || echo /opt/buildbox/bin); sudo -n tar -C "$d" -cf - .', timeout=120)
    if archive.returncode: raise RuntimeError('bb2 tool snapshot failed')
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as bundle:
        files = {m.name.removeprefix('./'): bundle.extractfile(m).read() for m in bundle.getmembers() if m.isfile()}
    expected = json.loads(files['installed-tools.json'])['sha256']
    bad = [n for n, h in expected.items() if h is not None and hashlib.sha256(files.get(n, b'')).hexdigest() != h]
    if bad: raise RuntimeError('snapshot does not match its manifest: '+' '.join(bad))
    atomic_write(CONFIG_DIR/'tools-snapshot.tar', archive.stdout); atomic_write(stamp, (digest+'\n').encode())
    return True


if __name__ == '__main__':
    command = sys.argv[1:2] or ['plan']
    if command == ['plan']:
        cfg = json.loads((HERE/'relaunch.json').read_text())
        for region, overrides in candidates(cfg, placement_scores(cfg)):
            print(region, ' '.join(f'{sku}@{az}' for _, sku, az in overrides[:6]), '...')
    elif command == ['relaunch']: relaunch('--dry-run' in sys.argv[2:])
    elif command == ['snapshot']: print('snapshot updated' if snapshot() else 'snapshot current')
    else: print(__doc__); raise SystemExit(2)
