// This is a generated file - do not edit.
//
// Generated from protocol/v1/protocol.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports
// ignore_for_file: unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use protocolIdentityDescriptor instead')
const ProtocolIdentity$json = {
  '1': 'ProtocolIdentity',
  '2': [
    {'1': 'version', '3': 1, '4': 1, '5': 9, '10': 'version'},
    {
      '1': 'descriptor_digest',
      '3': 2,
      '4': 1,
      '5': 9,
      '10': 'descriptorDigest'
    },
  ],
};

/// Descriptor for `ProtocolIdentity`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List protocolIdentityDescriptor = $convert.base64Decode(
    'ChBQcm90b2NvbElkZW50aXR5EhgKB3ZlcnNpb24YASABKAlSB3ZlcnNpb24SKwoRZGVzY3JpcH'
    'Rvcl9kaWdlc3QYAiABKAlSEGRlc2NyaXB0b3JEaWdlc3Q=');

@$core.Deprecated('Use capabilityDescriptor instead')
const Capability$json = {
  '1': 'Capability',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'version', '3': 2, '4': 1, '5': 9, '10': 'version'},
  ],
};

/// Descriptor for `Capability`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List capabilityDescriptor = $convert.base64Decode(
    'CgpDYXBhYmlsaXR5EhIKBG5hbWUYASABKAlSBG5hbWUSGAoHdmVyc2lvbhgCIAEoCVIHdmVyc2'
    'lvbg==');

@$core.Deprecated('Use capabilitySetDescriptor instead')
const CapabilitySet$json = {
  '1': 'CapabilitySet',
  '2': [
    {
      '1': 'capabilities',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.protocol.v1.Capability',
      '10': 'capabilities'
    },
  ],
};

/// Descriptor for `CapabilitySet`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List capabilitySetDescriptor = $convert.base64Decode(
    'Cg1DYXBhYmlsaXR5U2V0EkMKDGNhcGFiaWxpdGllcxgBIAMoCzIfLmFjeWNsaWMucHJvdG9jb2'
    'wudjEuQ2FwYWJpbGl0eVIMY2FwYWJpbGl0aWVz');

@$core.Deprecated('Use handshakeRequestDescriptor instead')
const HandshakeRequest$json = {
  '1': 'HandshakeRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'required',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.CapabilitySet',
      '10': 'required'
    },
  ],
};

/// Descriptor for `HandshakeRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List handshakeRequestDescriptor = $convert.base64Decode(
    'ChBIYW5kc2hha2VSZXF1ZXN0EkEKCHByb3RvY29sGAEgASgLMiUuYWN5Y2xpYy5wcm90b2NvbC'
    '52MS5Qcm90b2NvbElkZW50aXR5Ughwcm90b2NvbBI+CghyZXF1aXJlZBgCIAEoCzIiLmFjeWNs'
    'aWMucHJvdG9jb2wudjEuQ2FwYWJpbGl0eVNldFIIcmVxdWlyZWQ=');

@$core.Deprecated('Use handshakeResponseDescriptor instead')
const HandshakeResponse$json = {
  '1': 'HandshakeResponse',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'supported',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.CapabilitySet',
      '10': 'supported'
    },
  ],
};

/// Descriptor for `HandshakeResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List handshakeResponseDescriptor = $convert.base64Decode(
    'ChFIYW5kc2hha2VSZXNwb25zZRJBCghwcm90b2NvbBgBIAEoCzIlLmFjeWNsaWMucHJvdG9jb2'
    'wudjEuUHJvdG9jb2xJZGVudGl0eVIIcHJvdG9jb2wSQAoJc3VwcG9ydGVkGAIgASgLMiIuYWN5'
    'Y2xpYy5wcm90b2NvbC52MS5DYXBhYmlsaXR5U2V0UglzdXBwb3J0ZWQ=');
