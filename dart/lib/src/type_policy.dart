// Generated exclusively from rust/crates/sdk-contract-wire/src/type_policy.rs.

abstract interface class _RustOwnedValue {
  Object? get wireValue;
}
sealed class ActorId implements _RustOwnedValue {
  const ActorId._();
  String get value;
  @override Object? get wireValue => value;
  factory ActorId.from(String value) => ActorIdValue.from(value);
}

final class ActorIdValue extends ActorId {
  ActorIdValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory ActorIdValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('actor_id: value must be non-empty'); }
    return ActorIdValue._(value);
  }
}

sealed class Method implements _RustOwnedValue {
  const Method._();
  String get value;
  @override Object? get wireValue => value;
  factory Method.from(String value) => MethodValue.from(value);
}

final class MethodValue extends Method {
  MethodValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory MethodValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('method: value must be non-empty'); }
    return MethodValue._(value);
  }
}

sealed class Path implements _RustOwnedValue {
  const Path._();
  String get value;
  @override Object? get wireValue => value;
  factory Path.from(String value) => PathValue.from(value);
}

final class PathValue extends Path {
  PathValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory PathValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('path: value must be non-empty'); }
    return PathValue._(value);
  }
}

sealed class Source implements _RustOwnedValue {
  const Source._();
  String get value;
  @override Object? get wireValue => value;
  factory Source.from(String value) => SourceValue.from(value);
}

final class SourceValue extends Source {
  SourceValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory SourceValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('source: value must be non-empty'); }
    return SourceValue._(value);
  }
}

sealed class Destination implements _RustOwnedValue {
  const Destination._();
  String get value;
  @override Object? get wireValue => value;
  factory Destination.from(String value) => DestinationValue.from(value);
}

final class DestinationValue extends Destination {
  DestinationValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory DestinationValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('destination: value must be non-empty'); }
    return DestinationValue._(value);
  }
}

sealed class BucketName implements _RustOwnedValue {
  const BucketName._();
  String get value;
  @override Object? get wireValue => value;
  factory BucketName.from(String value) => BucketNameValue.from(value);
}

final class BucketNameValue extends BucketName {
  BucketNameValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory BucketNameValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('bucket_name: value must be non-empty'); }
    return BucketNameValue._(value);
  }
}

sealed class ObjectKey implements _RustOwnedValue {
  const ObjectKey._();
  String get value;
  @override Object? get wireValue => value;
  factory ObjectKey.from(String value) => ObjectKeyValue.from(value);
}

final class ObjectKeyValue extends ObjectKey {
  ObjectKeyValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory ObjectKeyValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('object_key: value must be non-empty'); }
    return ObjectKeyValue._(value);
  }
}

sealed class Alias implements _RustOwnedValue {
  const Alias._();
  String get value;
  @override Object? get wireValue => value;
  factory Alias.from(String value) => AliasValue.from(value);
}

final class AliasValue extends Alias {
  AliasValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory AliasValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('alias: value must be non-empty'); }
    return AliasValue._(value);
  }
}

sealed class JobId implements _RustOwnedValue {
  const JobId._();
  String get value;
  @override Object? get wireValue => value;
  factory JobId.from(String value) => JobIdValue.from(value);
}

final class JobIdValue extends JobId {
  JobIdValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory JobIdValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('job_id: value must be non-empty'); }
    return JobIdValue._(value);
  }
}

sealed class MachineId implements _RustOwnedValue {
  const MachineId._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory MachineId.from(List<int> value) => MachineIdValue.from(value);
}

final class MachineIdValue extends MachineId {
  MachineIdValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory MachineIdValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('machine_id: value must be non-empty'); }
    if (value.length != 16) { throw ArgumentError('machine_id: invalid byte length'); }
    return MachineIdValue._(value);
  }
}

sealed class OperationId implements _RustOwnedValue {
  const OperationId._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory OperationId.from(List<int> value) => OperationIdValue.from(value);
}

final class OperationIdValue extends OperationId {
  OperationIdValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory OperationIdValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('operation_id: value must be non-empty'); }
    if (value.length != 16) { throw ArgumentError('operation_id: invalid byte length'); }
    return OperationIdValue._(value);
  }
}

sealed class WorkspaceId implements _RustOwnedValue {
  const WorkspaceId._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory WorkspaceId.from(List<int> value) => WorkspaceIdValue.from(value);
}

final class WorkspaceIdValue extends WorkspaceId {
  WorkspaceIdValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory WorkspaceIdValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('workspace_id: value must be non-empty'); }
    if (value.length != 16) { throw ArgumentError('workspace_id: invalid byte length'); }
    return WorkspaceIdValue._(value);
  }
}

sealed class CheckpointId implements _RustOwnedValue {
  const CheckpointId._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory CheckpointId.from(List<int> value) => CheckpointIdValue.from(value);
}

final class CheckpointIdValue extends CheckpointId {
  CheckpointIdValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory CheckpointIdValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('checkpoint_id: value must be non-empty'); }
    if (value.length != 16) { throw ArgumentError('checkpoint_id: invalid byte length'); }
    return CheckpointIdValue._(value);
  }
}

sealed class IdempotencyKeyBytes implements _RustOwnedValue {
  const IdempotencyKeyBytes._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory IdempotencyKeyBytes.from(List<int> value) => IdempotencyKeyBytesValue.from(value);
}

final class IdempotencyKeyBytesValue extends IdempotencyKeyBytes {
  IdempotencyKeyBytesValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory IdempotencyKeyBytesValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('idempotency_key_bytes: value must be non-empty'); }
    return IdempotencyKeyBytesValue._(value);
  }
}

sealed class IdempotencyKeyText implements _RustOwnedValue {
  const IdempotencyKeyText._();
  String get value;
  @override Object? get wireValue => value;
  factory IdempotencyKeyText.from(String value) => IdempotencyKeyTextValue.from(value);
}

final class IdempotencyKeyTextValue extends IdempotencyKeyText {
  IdempotencyKeyTextValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory IdempotencyKeyTextValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('idempotency_key_text: value must be non-empty'); }
    return IdempotencyKeyTextValue._(value);
  }
}

sealed class IdempotencyKeyMessage implements _RustOwnedValue {
  const IdempotencyKeyMessage._();
  Object? get value;
  @override Object? get wireValue => value;
  factory IdempotencyKeyMessage.from(Object? value) => IdempotencyKeyMessageValue.from(value);
}

final class IdempotencyKeyMessageValue extends IdempotencyKeyMessage {
  IdempotencyKeyMessageValue._(this.value) : super._();
  @override final Object? value;
  @override Object? get wireValue => value;
  factory IdempotencyKeyMessageValue.from(Object? value) {
    if (value.length != 16) { throw ArgumentError('idempotency_key_message: invalid byte length'); }
    return IdempotencyKeyMessageValue._(value);
  }
}

sealed class OpaqueText implements _RustOwnedValue {
  const OpaqueText._();
  String get value;
  @override Object? get wireValue => value;
  factory OpaqueText.from(String value) => OpaqueTextValue.from(value);
}

final class OpaqueTextValue extends OpaqueText {
  OpaqueTextValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory OpaqueTextValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('opaque_text: value must be non-empty'); }
    return OpaqueTextValue._(value);
  }
}

sealed class UploadId implements _RustOwnedValue {
  const UploadId._();
  String get value;
  @override Object? get wireValue => value;
  factory UploadId.from(String value) => UploadIdValue.from(value);
}

final class UploadIdValue extends UploadId {
  UploadIdValue._(this.value) : super._();
  @override final String value;
  @override Object? get wireValue => value;
  factory UploadIdValue.from(String value) {
    if (value.isEmpty) { throw ArgumentError('upload_id: value must be non-empty'); }
    return UploadIdValue._(value);
  }
}

sealed class VersionSha256 implements _RustOwnedValue {
  const VersionSha256._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory VersionSha256.from(List<int> value) => VersionSha256Value.from(value);
}

final class VersionSha256Value extends VersionSha256 {
  VersionSha256Value._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory VersionSha256Value.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('version_sha256: value must be non-empty'); }
    if (value.length != 32) { throw ArgumentError('version_sha256: invalid byte length'); }
    return VersionSha256Value._(value);
  }
}

sealed class Sha256Digest implements _RustOwnedValue {
  const Sha256Digest._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory Sha256Digest.from(List<int> value) => Sha256DigestValue.from(value);
}

final class Sha256DigestValue extends Sha256Digest {
  Sha256DigestValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory Sha256DigestValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('sha256_digest: value must be non-empty'); }
    if (value.length != 32) { throw ArgumentError('sha256_digest: invalid byte length'); }
    return Sha256DigestValue._(value);
  }
}

sealed class RevisionDigest implements _RustOwnedValue {
  const RevisionDigest._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory RevisionDigest.from(List<int> value) => RevisionDigestValue.from(value);
}

final class RevisionDigestValue extends RevisionDigest {
  RevisionDigestValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory RevisionDigestValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('revision_digest: value must be non-empty'); }
    if (value.length != 32) { throw ArgumentError('revision_digest: invalid byte length'); }
    return RevisionDigestValue._(value);
  }
}

sealed class ImmutableImage implements _RustOwnedValue {
  const ImmutableImage._();
  Object? get value;
  @override Object? get wireValue => value;
  factory ImmutableImage.from(Object? value) => ImmutableImageValue.from(value);
}

final class ImmutableImageValue extends ImmutableImage {
  ImmutableImageValue._(this.value) : super._();
  @override final Object? value;
  @override Object? get wireValue => value;
  factory ImmutableImageValue.from(Object? value) {
    return ImmutableImageValue._(value);
  }
}

sealed class Revision implements _RustOwnedValue {
  const Revision._();
  int get value;
  @override Object? get wireValue => value;
  factory Revision.from(int value) => RevisionValue.from(value);
}

final class RevisionValue extends Revision {
  RevisionValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory RevisionValue.from(int value) {
    if (value < 0) { throw ArgumentError('revision: must be non-negative'); }
    return RevisionValue._(value);
  }
}

sealed class RunId implements _RustOwnedValue {
  const RunId._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory RunId.from(List<int> value) => RunIdValue.from(value);
}

final class RunIdValue extends RunId {
  RunIdValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory RunIdValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('run_id: value must be non-empty'); }
    if (value.length != 16) { throw ArgumentError('run_id: invalid byte length'); }
    return RunIdValue._(value);
  }
}

sealed class EvaluationId implements _RustOwnedValue {
  const EvaluationId._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory EvaluationId.from(List<int> value) => EvaluationIdValue.from(value);
}

final class EvaluationIdValue extends EvaluationId {
  EvaluationIdValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory EvaluationIdValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('evaluation_id: value must be non-empty'); }
    if (value.length != 16) { throw ArgumentError('evaluation_id: invalid byte length'); }
    return EvaluationIdValue._(value);
  }
}

sealed class PageLimit implements _RustOwnedValue {
  const PageLimit._();
  int get value;
  @override Object? get wireValue => value;
  factory PageLimit.from(int value) => PageLimitValue.from(value);
}

final class PageLimitValue extends PageLimit {
  PageLimitValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory PageLimitValue.from(int value) {
    if (value <= 0) { throw ArgumentError('page_limit: must be positive'); }
    if (value > 1000) { throw ArgumentError('page_limit: exceeds maximum'); }
    return PageLimitValue._(value);
  }
}

sealed class StreamPageLimit implements _RustOwnedValue {
  const StreamPageLimit._();
  int get value;
  @override Object? get wireValue => value;
  factory StreamPageLimit.from(int value) => StreamPageLimitValue.from(value);
}

final class StreamPageLimitValue extends StreamPageLimit {
  StreamPageLimitValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory StreamPageLimitValue.from(int value) {
    if (value <= 0) { throw ArgumentError('stream_page_limit: must be positive'); }
    if (value > 1024) { throw ArgumentError('stream_page_limit: exceeds maximum'); }
    return StreamPageLimitValue._(value);
  }
}

sealed class MachinePageLimit implements _RustOwnedValue {
  const MachinePageLimit._();
  int get value;
  @override Object? get wireValue => value;
  factory MachinePageLimit.from(int value) => MachinePageLimitValue.from(value);
}

final class MachinePageLimitValue extends MachinePageLimit {
  MachinePageLimitValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory MachinePageLimitValue.from(int value) {
    if (value <= 0) { throw ArgumentError('machine_page_limit: must be positive'); }
    if (value > 256) { throw ArgumentError('machine_page_limit: exceeds maximum'); }
    return MachinePageLimitValue._(value);
  }
}

sealed class MachineEventPageLimit implements _RustOwnedValue {
  const MachineEventPageLimit._();
  int get value;
  @override Object? get wireValue => value;
  factory MachineEventPageLimit.from(int value) => MachineEventPageLimitValue.from(value);
}

final class MachineEventPageLimitValue extends MachineEventPageLimit {
  MachineEventPageLimitValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory MachineEventPageLimitValue.from(int value) {
    if (value <= 0) { throw ArgumentError('machine_event_page_limit: must be positive'); }
    if (value > 1024) { throw ArgumentError('machine_event_page_limit: exceeds maximum'); }
    return MachineEventPageLimitValue._(value);
  }
}

sealed class CommitId implements _RustOwnedValue {
  const CommitId._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory CommitId.from(List<int> value) => CommitIdValue.from(value);
}

final class CommitIdValue extends CommitId {
  CommitIdValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory CommitIdValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('commit_id: value must be non-empty'); }
    return CommitIdValue._(value);
  }
}

sealed class EnumValue implements _RustOwnedValue {
  const EnumValue._();
  int get value;
  @override Object? get wireValue => value;
  factory EnumValue.from(int value) => EnumValueValue.from(value);
}

final class EnumValueValue extends EnumValue {
  EnumValueValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory EnumValueValue.from(int value) {
    return EnumValueValue._(value);
  }
}

sealed class OneofArm implements _RustOwnedValue {
  const OneofArm._();
  Object? get value;
  @override Object? get wireValue => value;
  factory OneofArm.from(Object? value) => OneofArmValue.from(value);
}

final class OneofArmValue extends OneofArm {
  OneofArmValue._(this.value) : super._();
  @override final Object? value;
  @override Object? get wireValue => value;
  factory OneofArmValue.from(Object? value) {
    if (value is! Map || value.length != 1) { throw ArgumentError('oneof_arm: exactly one arm is required'); }
    return OneofArmValue._(value);
  }
}

sealed class OpaqueBytes implements _RustOwnedValue {
  const OpaqueBytes._();
  List<int> get value;
  @override Object? get wireValue => value;
  factory OpaqueBytes.from(List<int> value) => OpaqueBytesValue.from(value);
}

final class OpaqueBytesValue extends OpaqueBytes {
  OpaqueBytesValue._(this.value) : super._();
  @override final List<int> value;
  @override Object? get wireValue => value;
  factory OpaqueBytesValue.from(List<int> value) {
    if (value.isEmpty) { throw ArgumentError('opaque_bytes: value must be non-empty'); }
    return OpaqueBytesValue._(value);
  }
}

sealed class Sequence implements _RustOwnedValue {
  const Sequence._();
  int get value;
  @override Object? get wireValue => value;
  factory Sequence.from(int value) => SequenceValue.from(value);
}

final class SequenceValue extends Sequence {
  SequenceValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory SequenceValue.from(int value) {
    if (value < 0) { throw ArgumentError('sequence: must be non-negative'); }
    return SequenceValue._(value);
  }
}

sealed class NonNegativeCount implements _RustOwnedValue {
  const NonNegativeCount._();
  int get value;
  @override Object? get wireValue => value;
  factory NonNegativeCount.from(int value) => NonNegativeCountValue.from(value);
}

final class NonNegativeCountValue extends NonNegativeCount {
  NonNegativeCountValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory NonNegativeCountValue.from(int value) {
    if (value < 0) { throw ArgumentError('non_negative_count: must be non-negative'); }
    return NonNegativeCountValue._(value);
  }
}

sealed class PositiveCount implements _RustOwnedValue {
  const PositiveCount._();
  int get value;
  @override Object? get wireValue => value;
  factory PositiveCount.from(int value) => PositiveCountValue.from(value);
}

final class PositiveCountValue extends PositiveCount {
  PositiveCountValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory PositiveCountValue.from(int value) {
    if (value <= 0) { throw ArgumentError('positive_count: must be positive'); }
    return PositiveCountValue._(value);
  }
}

sealed class TimestampMillis implements _RustOwnedValue {
  const TimestampMillis._();
  int get value;
  @override Object? get wireValue => value;
  factory TimestampMillis.from(int value) => TimestampMillisValue.from(value);
}

final class TimestampMillisValue extends TimestampMillis {
  TimestampMillisValue._(this.value) : super._();
  @override final int value;
  @override Object? get wireValue => value;
  factory TimestampMillisValue.from(int value) {
    if (value < 0) { throw ArgumentError('timestamp_millis: must be non-negative'); }
    return TimestampMillisValue._(value);
  }
}

final class TypePolicyWire {
  static final Map<String, Object Function(Object?)> _factories = {
    'actors.actor_id': (value) => ActorId.from(value as String),
    'actors.method': (value) => Method.from(value as String),
    'workers.alias': (value) => Alias.from(value as String),
    'workers.version_sha256': (value) => VersionSha256.from(value as List<int>),
    'workers.idempotency_key': (value) => IdempotencyKeyText.from(value as String),
    'workers.job_id': (value) => JobId.from(value as String),
    'workers.method': (value) => Method.from(value as String),
    'stream.idempotency_key': (value) => IdempotencyKeyBytes.from(value as List<int>),
    'stream.path': (value) => Path.from(value as String),
    'stream.source': (value) => Source.from(value as String),
    'stream.destination': (value) => Destination.from(value as String),
    'stream.limit': (value) => StreamPageLimit.from(value as int),
    'stream.commit_id': (value) => CommitId.from(value as List<int>),
    'objects.key': (value) => ObjectKey.from(value as String),
    'objects.etag': (value) => OpaqueText.from(value as String),
    'objects.idempotency_key': (value) => IdempotencyKeyText.from(value as String),
    'objects.upload_id': (value) => UploadId.from(value as String),
    'objects.page_size': (value) => PageLimit.from(value as int),
    'inference.run_id': (value) => RunId.from(value as List<int>),
    'inference.revision': (value) => RevisionDigest.from(value as List<int>),
    'inference.commitment': (value) => Sha256Digest.from(value as List<int>),
    'inference.evaluation_id': (value) => EvaluationId.from(value as List<int>),
    'inference.spec_digest': (value) => Sha256Digest.from(value as List<int>),
    'machines.image': (value) => ImmutableImage.from(value as Object?),
    'machines.managed_digest': (value) => Sha256Digest.from(value as List<int>),
    'machines.custom_digest': (value) => Sha256Digest.from(value as List<int>),
    'machines.idempotency_key': (value) => IdempotencyKeyMessage.from(value as Object?),
    'machines.machine_id': (value) => MachineId.from(value as List<int>),
    'machines.checkpoint_id': (value) => CheckpointId.from(value as List<int>),
    'machines.operation_id': (value) => OperationId.from(value as List<int>),
    'machines.page_limit': (value) => MachinePageLimit.from(value as int),
    'machines.event_page_limit': (value) => MachineEventPageLimit.from(value as int),
    'filesystem.path': (value) => Path.from(value as String),
    'harness.path': (value) => Path.from(value as String),
  };

  static Object? toWire({required String family, required String field, required Object? value}) {
    final factory = _factories['${family.toLowerCase()}.$field'];
    if (value is _RustOwnedValue) return value.wireValue;
    return factory == null ? value : (factory(value) as _RustOwnedValue).wireValue;
  }

  static Object? normalizeRequest(String family, Object? request) {
    if (request is! Map) return request;
    final normalized = <String, Object?>{};
    request.forEach((key, value) { normalized[key.toString()] = value; });
    for (final key in normalized.keys.toList()) {
      final value = normalized[key];
      if (value != null && _factories.containsKey("${family.toLowerCase()}.$key")) {
        normalized[key] = toWire(family: family, field: key, value: value);
      }
    }
    return normalized;
  }

  static Object? typedField({required String family, required String field, required Object? value}) {
    final factory = _factories['${family.toLowerCase()}.$field'];
    if (value is _RustOwnedValue || factory == null) return value;
    return factory(value);
  }
}
