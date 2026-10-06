import 'package:test/test.dart';
import '../lib/src/type_policy.dart';
import '../lib/src/remote_policy.dart';

void main() {
  test('rejects invalid ActorId', () { expect(() => ActorId.from(''), throwsArgumentError); });
  test('rejects invalid Method', () { expect(() => Method.from(''), throwsArgumentError); });
  test('rejects invalid Path', () { expect(() => Path.from(''), throwsArgumentError); });
  test('rejects invalid Source', () { expect(() => Source.from(''), throwsArgumentError); });
  test('rejects invalid Destination', () { expect(() => Destination.from(''), throwsArgumentError); });
  test('rejects invalid BucketName', () { expect(() => BucketName.from(''), throwsArgumentError); });
  test('rejects invalid ObjectKey', () { expect(() => ObjectKey.from(''), throwsArgumentError); });
  test('rejects invalid Alias', () { expect(() => Alias.from(''), throwsArgumentError); });
  test('rejects invalid JobId', () { expect(() => JobId.from(''), throwsArgumentError); });
  test('rejects invalid MachineId', () { expect(() => MachineId.from(<int>[]), throwsArgumentError); });
  test('rejects invalid OperationId', () { expect(() => OperationId.from(<int>[]), throwsArgumentError); });
  test('rejects invalid WorkspaceId', () { expect(() => WorkspaceId.from(<int>[]), throwsArgumentError); });
  test('rejects invalid CheckpointId', () { expect(() => CheckpointId.from(<int>[]), throwsArgumentError); });
  test('rejects invalid IdempotencyKeyBytes', () { expect(() => IdempotencyKeyBytes.from(<int>[]), throwsArgumentError); });
  test('rejects invalid IdempotencyKeyText', () { expect(() => IdempotencyKeyText.from(''), throwsArgumentError); });
  test('rejects invalid IdempotencyKeyMessage', () { expect(() => IdempotencyKeyMessage.from(''), throwsArgumentError); });
  test('rejects invalid OpaqueText', () { expect(() => OpaqueText.from(''), throwsArgumentError); });
  test('rejects invalid UploadId', () { expect(() => UploadId.from(''), throwsArgumentError); });
  test('rejects invalid VersionSha256', () { expect(() => VersionSha256.from(<int>[0]), throwsArgumentError); });
  test('rejects invalid Sha256Digest', () { expect(() => Sha256Digest.from(<int>[0]), throwsArgumentError); });
  test('rejects invalid RevisionDigest', () { expect(() => RevisionDigest.from(<int>[0]), throwsArgumentError); });
  test('rejects invalid Revision', () { expect(() => Revision.from(-1), throwsArgumentError); });
  test('rejects invalid RunId', () { expect(() => RunId.from(<int>[0]), throwsArgumentError); });
  test('rejects invalid EvaluationId', () { expect(() => EvaluationId.from(<int>[0]), throwsArgumentError); });
  test('rejects invalid PageLimit', () { expect(() => PageLimit.from(0), throwsArgumentError); });
  test('rejects invalid StreamPageLimit', () { expect(() => StreamPageLimit.from(0), throwsArgumentError); });
  test('rejects invalid MachinePageLimit', () { expect(() => MachinePageLimit.from(0), throwsArgumentError); });
  test('rejects invalid MachineEventPageLimit', () { expect(() => MachineEventPageLimit.from(0), throwsArgumentError); });
  test('rejects invalid CommitId', () { expect(() => CommitId.from(<int>[]), throwsArgumentError); });
  test('rejects invalid OneofArm', () { expect(() => OneofArm.from(<String, Object?>{}), throwsArgumentError); });
  test('rejects invalid Sequence', () { expect(() => Sequence.from(-1), throwsArgumentError); });
  test('rejects invalid NonNegativeCount', () { expect(() => NonNegativeCount.from(-1), throwsArgumentError); });
  test('rejects invalid PositiveCount', () { expect(() => PositiveCount.from(0), throwsArgumentError); });
  test('rejects invalid TimestampMillis', () { expect(() => TimestampMillis.from(-1), throwsArgumentError); });
  test('public remote facade serializes Rust-owned value objects', () async {
    Object? captured;
    final client = RemoteClient(family: 'stream', installed: const {RemoteTransport.grpc: false, RemoteTransport.grpcWeb: false, RemoteTransport.httpJson: true}, endpoint: const {RemoteTransport.grpc: false, RemoteTransport.grpcWeb: false, RemoteTransport.httpJson: true}, invoker: (operation, request, transport) { captured = request; return 'ok'; });
    expect(await client.call('append', {'path': Path.from('events')}), 'ok');
    expect((captured as Map)['path'], 'events');
    expect(() => client.call('append', {'path': ''}), throwsArgumentError);
  });
}
