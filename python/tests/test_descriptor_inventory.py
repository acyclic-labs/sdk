from __future__ import annotations

from google.protobuf import descriptor as descriptor_api
from google.protobuf import message_factory


MODULES = (
    "acyclic_sdk.generated.actors.v1.actors_pb2",
    "acyclic_sdk.generated.filesystem.v2.filesystem_pb2",
    "acyclic_sdk.generated.harness.v2.harness_pb2",
    "acyclic_sdk.generated.inference.v1.inference_pb2",
    "acyclic_sdk.generated.machines.v1.machines_pb2",
    "acyclic_sdk.generated.objects.v2.objects_pb2",
    "acyclic_sdk.generated.protocol.v1.protocol_pb2",
    "acyclic_sdk.generated.stream.v2.stream_pb2",
    "acyclic_sdk.generated.workers.v1.workers_pb2",
)


def _scalar(field, seed: int):
    kind = field.type
    field_types = descriptor_api.FieldDescriptor
    if kind == field_types.TYPE_BOOL:
        return True
    if kind in (field_types.TYPE_INT32, field_types.TYPE_SINT32, field_types.TYPE_SFIXED32):
        return seed
    if kind in (field_types.TYPE_INT64, field_types.TYPE_SINT64, field_types.TYPE_SFIXED64):
        return seed
    if kind in (field_types.TYPE_UINT32, field_types.TYPE_FIXED32):
        return seed + 1
    if kind in (field_types.TYPE_UINT64, field_types.TYPE_FIXED64):
        return 2**63 + 17
    if kind == field_types.TYPE_FLOAT:
        return 1.25
    if kind == field_types.TYPE_DOUBLE:
        return 2.5
    if kind == field_types.TYPE_STRING:
        return "descriptor-value"
    if kind == field_types.TYPE_BYTES:
        return b"descriptor-bytes"
    if kind == field_types.TYPE_ENUM:
        return field.enum_type.values[0].number
    raise AssertionError(f"unsupported scalar field {field.full_name}: {kind}")


def _populate(message, descriptor, seed: int) -> None:
    for index, field in enumerate(descriptor.fields):
        value_seed = seed + index + 1
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            container = getattr(message, field.name)
            key_field = field.message_type.fields_by_name["key"]
            value_field = field.message_type.fields_by_name["value"]
            key = _scalar(key_field, value_seed)
            if value_field.type == descriptor_api.FieldDescriptor.TYPE_MESSAGE:
                container[key].SetInParent()
            else:
                container[key] = _scalar(value_field, value_seed + 1)
        elif field.is_repeated:
            container = getattr(message, field.name)
            if field.type == descriptor_api.FieldDescriptor.TYPE_MESSAGE:
                container.add().SetInParent()
            else:
                container.append(_scalar(field, value_seed))
        elif field.type == descriptor_api.FieldDescriptor.TYPE_MESSAGE:
            getattr(message, field.name).SetInParent()
        else:
            setattr(message, field.name, _scalar(field, value_seed))


def _messages(descriptor, seen):
    if descriptor.full_name in seen or descriptor.GetOptions().map_entry:
        return
    seen.add(descriptor.full_name)
    yield descriptor
    for nested in descriptor.nested_types:
        yield from _messages(nested, seen)


def test_every_public_descriptor_roundtrips_representative_values() -> None:
    import importlib

    files = [importlib.import_module(name).DESCRIPTOR for name in MODULES]
    seen = set()
    messages = [
        descriptor
        for file_descriptor in files
        for descriptor in file_descriptor.message_types_by_name.values()
        for descriptor in _messages(descriptor, seen)
    ]
    assert len(files) == 9
    assert len(messages) == 405
    assert sum(len(descriptor.fields) for descriptor in messages) == 1360
    assert sum(
        1
        for descriptor in messages
        for field in descriptor.fields
        if field.type in (descriptor_api.FieldDescriptor.TYPE_UINT64, descriptor_api.FieldDescriptor.TYPE_FIXED64)
    ) == 172

    oneof_variants = 0
    for descriptor in messages:
        message_type = message_factory.GetMessageClass(descriptor)
        message = message_type()
        _populate(message, descriptor, len(descriptor.full_name))
        encoded = message.SerializeToString(deterministic=True)
        decoded = message_type.FromString(encoded)
        assert decoded.SerializeToString(deterministic=True) == encoded, descriptor.full_name
        for oneof in descriptor.oneofs:
            for field in oneof.fields:
                variant = message_type()
                if field.type == descriptor_api.FieldDescriptor.TYPE_MESSAGE:
                    getattr(variant, field.name).SetInParent()
                else:
                    setattr(variant, field.name, _scalar(field, oneof_variants + 1))
                assert variant.WhichOneof(oneof.name) == field.name
                assert message_type.FromString(variant.SerializeToString(deterministic=True)) == variant
                oneof_variants += 1
    assert oneof_variants == 186
