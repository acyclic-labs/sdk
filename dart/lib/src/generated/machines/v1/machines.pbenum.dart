// This is a generated file - do not edit.
//
// Generated from machines/v1/machines.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class ImageKind extends $pb.ProtobufEnum {
  static const ImageKind IMAGE_KIND_UNSPECIFIED =
      ImageKind._(0, _omitEnumNames ? '' : 'IMAGE_KIND_UNSPECIFIED');
  static const ImageKind IMAGE_KIND_MANAGED_OCI =
      ImageKind._(1, _omitEnumNames ? '' : 'IMAGE_KIND_MANAGED_OCI');
  static const ImageKind IMAGE_KIND_CUSTOM =
      ImageKind._(2, _omitEnumNames ? '' : 'IMAGE_KIND_CUSTOM');
  static const ImageKind IMAGE_KIND_CHECKPOINT =
      ImageKind._(3, _omitEnumNames ? '' : 'IMAGE_KIND_CHECKPOINT');

  static const $core.List<ImageKind> values = <ImageKind>[
    IMAGE_KIND_UNSPECIFIED,
    IMAGE_KIND_MANAGED_OCI,
    IMAGE_KIND_CUSTOM,
    IMAGE_KIND_CHECKPOINT,
  ];

  static final $core.List<ImageKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static ImageKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ImageKind._(super.value, super.name);
}

class Capability extends $pb.ProtobufEnum {
  static const Capability CAPABILITY_UNSPECIFIED =
      Capability._(0, _omitEnumNames ? '' : 'CAPABILITY_UNSPECIFIED');
  static const Capability CAPABILITY_ELASTIC_CPU =
      Capability._(1, _omitEnumNames ? '' : 'CAPABILITY_ELASTIC_CPU');
  static const Capability CAPABILITY_ELASTIC_MEMORY =
      Capability._(2, _omitEnumNames ? '' : 'CAPABILITY_ELASTIC_MEMORY');
  static const Capability CAPABILITY_LIVE_CHECKPOINT =
      Capability._(3, _omitEnumNames ? '' : 'CAPABILITY_LIVE_CHECKPOINT');
  static const Capability CAPABILITY_LIVE_FORK =
      Capability._(4, _omitEnumNames ? '' : 'CAPABILITY_LIVE_FORK');
  static const Capability CAPABILITY_SUSPEND_RESUME =
      Capability._(5, _omitEnumNames ? '' : 'CAPABILITY_SUSPEND_RESUME');
  static const Capability CAPABILITY_LIVE_MOVEMENT =
      Capability._(6, _omitEnumNames ? '' : 'CAPABILITY_LIVE_MOVEMENT');

  /// ForkMachine copies a running machine's persistent disk, but not its memory or processes,
  /// into fresh children. Which paths are persistent is provider-defined: a provider whose
  /// machines boot from an immutable image may copy only its declared data directory. CAPABILITY_LIVE_FORK is the memory-and-disk form and takes precedence
  /// when both are declared.
  static const Capability CAPABILITY_DISK_FORK =
      Capability._(7, _omitEnumNames ? '' : 'CAPABILITY_DISK_FORK');

  static const $core.List<Capability> values = <Capability>[
    CAPABILITY_UNSPECIFIED,
    CAPABILITY_ELASTIC_CPU,
    CAPABILITY_ELASTIC_MEMORY,
    CAPABILITY_LIVE_CHECKPOINT,
    CAPABILITY_LIVE_FORK,
    CAPABILITY_SUSPEND_RESUME,
    CAPABILITY_LIVE_MOVEMENT,
    CAPABILITY_DISK_FORK,
  ];

  static final $core.List<Capability?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 7);
  static Capability? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const Capability._(super.value, super.name);
}

class CompatibilityMode extends $pb.ProtobufEnum {
  static const CompatibilityMode COMPATIBILITY_MODE_UNSPECIFIED =
      CompatibilityMode._(
          0, _omitEnumNames ? '' : 'COMPATIBILITY_MODE_UNSPECIFIED');
  static const CompatibilityMode COMPATIBILITY_MODE_BEST_EFFORT =
      CompatibilityMode._(
          1, _omitEnumNames ? '' : 'COMPATIBILITY_MODE_BEST_EFFORT');
  static const CompatibilityMode COMPATIBILITY_MODE_REQUIRE =
      CompatibilityMode._(
          2, _omitEnumNames ? '' : 'COMPATIBILITY_MODE_REQUIRE');

  static const $core.List<CompatibilityMode> values = <CompatibilityMode>[
    COMPATIBILITY_MODE_UNSPECIFIED,
    COMPATIBILITY_MODE_BEST_EFFORT,
    COMPATIBILITY_MODE_REQUIRE,
  ];

  static final $core.List<CompatibilityMode?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static CompatibilityMode? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const CompatibilityMode._(super.value, super.name);
}

class ExpirationKind extends $pb.ProtobufEnum {
  static const ExpirationKind EXPIRATION_KIND_UNSPECIFIED =
      ExpirationKind._(0, _omitEnumNames ? '' : 'EXPIRATION_KIND_UNSPECIFIED');
  static const ExpirationKind EXPIRATION_KIND_NEVER =
      ExpirationKind._(1, _omitEnumNames ? '' : 'EXPIRATION_KIND_NEVER');
  static const ExpirationKind EXPIRATION_KIND_MAX_AGE =
      ExpirationKind._(2, _omitEnumNames ? '' : 'EXPIRATION_KIND_MAX_AGE');
  static const ExpirationKind EXPIRATION_KIND_AT =
      ExpirationKind._(3, _omitEnumNames ? '' : 'EXPIRATION_KIND_AT');
  static const ExpirationKind EXPIRATION_KIND_IDLE =
      ExpirationKind._(4, _omitEnumNames ? '' : 'EXPIRATION_KIND_IDLE');

  static const $core.List<ExpirationKind> values = <ExpirationKind>[
    EXPIRATION_KIND_UNSPECIFIED,
    EXPIRATION_KIND_NEVER,
    EXPIRATION_KIND_MAX_AGE,
    EXPIRATION_KIND_AT,
    EXPIRATION_KIND_IDLE,
  ];

  static final $core.List<ExpirationKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 4);
  static ExpirationKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ExpirationKind._(super.value, super.name);
}

class OperationStatus extends $pb.ProtobufEnum {
  static const OperationStatus OPERATION_STATUS_UNSPECIFIED = OperationStatus._(
      0, _omitEnumNames ? '' : 'OPERATION_STATUS_UNSPECIFIED');
  static const OperationStatus OPERATION_STATUS_PENDING =
      OperationStatus._(1, _omitEnumNames ? '' : 'OPERATION_STATUS_PENDING');
  static const OperationStatus OPERATION_STATUS_SUCCEEDED =
      OperationStatus._(2, _omitEnumNames ? '' : 'OPERATION_STATUS_SUCCEEDED');
  static const OperationStatus OPERATION_STATUS_CANCELLED =
      OperationStatus._(3, _omitEnumNames ? '' : 'OPERATION_STATUS_CANCELLED');
  static const OperationStatus OPERATION_STATUS_INDETERMINATE =
      OperationStatus._(
          4, _omitEnumNames ? '' : 'OPERATION_STATUS_INDETERMINATE');
  static const OperationStatus OPERATION_STATUS_FAILED =
      OperationStatus._(5, _omitEnumNames ? '' : 'OPERATION_STATUS_FAILED');

  static const $core.List<OperationStatus> values = <OperationStatus>[
    OPERATION_STATUS_UNSPECIFIED,
    OPERATION_STATUS_PENDING,
    OPERATION_STATUS_SUCCEEDED,
    OPERATION_STATUS_CANCELLED,
    OPERATION_STATUS_INDETERMINATE,
    OPERATION_STATUS_FAILED,
  ];

  static final $core.List<OperationStatus?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static OperationStatus? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const OperationStatus._(super.value, super.name);
}

class MachineStatus extends $pb.ProtobufEnum {
  static const MachineStatus MACHINE_STATUS_UNSPECIFIED =
      MachineStatus._(0, _omitEnumNames ? '' : 'MACHINE_STATUS_UNSPECIFIED');
  static const MachineStatus MACHINE_STATUS_STARTING =
      MachineStatus._(1, _omitEnumNames ? '' : 'MACHINE_STATUS_STARTING');
  static const MachineStatus MACHINE_STATUS_RUNNING =
      MachineStatus._(2, _omitEnumNames ? '' : 'MACHINE_STATUS_RUNNING');
  static const MachineStatus MACHINE_STATUS_SUSPENDING =
      MachineStatus._(3, _omitEnumNames ? '' : 'MACHINE_STATUS_SUSPENDING');
  static const MachineStatus MACHINE_STATUS_SUSPENDED =
      MachineStatus._(4, _omitEnumNames ? '' : 'MACHINE_STATUS_SUSPENDED');
  static const MachineStatus MACHINE_STATUS_WAKING =
      MachineStatus._(5, _omitEnumNames ? '' : 'MACHINE_STATUS_WAKING');
  static const MachineStatus MACHINE_STATUS_DESTROYING =
      MachineStatus._(6, _omitEnumNames ? '' : 'MACHINE_STATUS_DESTROYING');
  static const MachineStatus MACHINE_STATUS_DESTROYED =
      MachineStatus._(7, _omitEnumNames ? '' : 'MACHINE_STATUS_DESTROYED');
  static const MachineStatus MACHINE_STATUS_FAILED =
      MachineStatus._(8, _omitEnumNames ? '' : 'MACHINE_STATUS_FAILED');
  static const MachineStatus MACHINE_STATUS_INDETERMINATE =
      MachineStatus._(9, _omitEnumNames ? '' : 'MACHINE_STATUS_INDETERMINATE');

  static const $core.List<MachineStatus> values = <MachineStatus>[
    MACHINE_STATUS_UNSPECIFIED,
    MACHINE_STATUS_STARTING,
    MACHINE_STATUS_RUNNING,
    MACHINE_STATUS_SUSPENDING,
    MACHINE_STATUS_SUSPENDED,
    MACHINE_STATUS_WAKING,
    MACHINE_STATUS_DESTROYING,
    MACHINE_STATUS_DESTROYED,
    MACHINE_STATUS_FAILED,
    MACHINE_STATUS_INDETERMINATE,
  ];

  static final $core.List<MachineStatus?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 9);
  static MachineStatus? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const MachineStatus._(super.value, super.name);
}

class ForkFidelity extends $pb.ProtobufEnum {
  static const ForkFidelity FORK_FIDELITY_UNSPECIFIED =
      ForkFidelity._(0, _omitEnumNames ? '' : 'FORK_FIDELITY_UNSPECIFIED');

  /// Children resume from the source's memory, processes, and disk at the fork instant.
  static const ForkFidelity FORK_FIDELITY_MEMORY_AND_DISK =
      ForkFidelity._(1, _omitEnumNames ? '' : 'FORK_FIDELITY_MEMORY_AND_DISK');

  /// Children boot fresh over a copy of the source's persistent disk (provider-defined; see
  /// CAPABILITY_DISK_FORK) taken at one consistent instant; no process state is inherited.
  static const ForkFidelity FORK_FIDELITY_DISK_ONLY =
      ForkFidelity._(2, _omitEnumNames ? '' : 'FORK_FIDELITY_DISK_ONLY');

  static const $core.List<ForkFidelity> values = <ForkFidelity>[
    FORK_FIDELITY_UNSPECIFIED,
    FORK_FIDELITY_MEMORY_AND_DISK,
    FORK_FIDELITY_DISK_ONLY,
  ];

  static final $core.List<ForkFidelity?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static ForkFidelity? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ForkFidelity._(super.value, super.name);
}

class PressureKind extends $pb.ProtobufEnum {
  static const PressureKind PRESSURE_KIND_UNSPECIFIED =
      PressureKind._(0, _omitEnumNames ? '' : 'PRESSURE_KIND_UNSPECIFIED');
  static const PressureKind PRESSURE_KIND_CUSTOMER_BUDGET =
      PressureKind._(1, _omitEnumNames ? '' : 'PRESSURE_KIND_CUSTOMER_BUDGET');
  static const PressureKind PRESSURE_KIND_MACHINE_LIMIT =
      PressureKind._(2, _omitEnumNames ? '' : 'PRESSURE_KIND_MACHINE_LIMIT');
  static const PressureKind PRESSURE_KIND_SERVICE_SATURATION = PressureKind._(
      3, _omitEnumNames ? '' : 'PRESSURE_KIND_SERVICE_SATURATION');

  static const $core.List<PressureKind> values = <PressureKind>[
    PRESSURE_KIND_UNSPECIFIED,
    PRESSURE_KIND_CUSTOMER_BUDGET,
    PRESSURE_KIND_MACHINE_LIMIT,
    PRESSURE_KIND_SERVICE_SATURATION,
  ];

  static final $core.List<PressureKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static PressureKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const PressureKind._(super.value, super.name);
}

class EventKind extends $pb.ProtobufEnum {
  static const EventKind EVENT_KIND_UNSPECIFIED =
      EventKind._(0, _omitEnumNames ? '' : 'EVENT_KIND_UNSPECIFIED');
  static const EventKind EVENT_KIND_STATE =
      EventKind._(1, _omitEnumNames ? '' : 'EVENT_KIND_STATE');
  static const EventKind EVENT_KIND_PRESSURE =
      EventKind._(2, _omitEnumNames ? '' : 'EVENT_KIND_PRESSURE');
  static const EventKind EVENT_KIND_CAPACITY =
      EventKind._(3, _omitEnumNames ? '' : 'EVENT_KIND_CAPACITY');

  static const $core.List<EventKind> values = <EventKind>[
    EVENT_KIND_UNSPECIFIED,
    EVENT_KIND_STATE,
    EVENT_KIND_PRESSURE,
    EVENT_KIND_CAPACITY,
  ];

  static final $core.List<EventKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static EventKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const EventKind._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
