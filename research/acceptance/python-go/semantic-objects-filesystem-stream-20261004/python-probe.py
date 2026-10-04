import json, os, sys, time
from pathlib import Path
import grpc
from google.protobuf import message_factory

import acyclic_sdk.generated.objects.v2.objects_pb2 as objects_pb2
import acyclic_sdk.generated.filesystem.v2.filesystem_pb2 as fs_pb2
import acyclic_sdk.generated.stream.v2.stream_pb2 as stream_pb2


def unary_stream(channel, service, method, request, output_cls):
    call = channel.unary_stream(
        f"/{service}/{method}",
        request_serializer=request.SerializeToString,
        response_deserializer=output_cls.FromString,
    )
    return list(call(request, timeout=5))


def main():
    address = os.environ.get("FIXTURE_GRPC_ADDRESS", "127.0.0.1:58315")
    out_path = Path(os.environ.get("SEMANTIC_OUTPUT", "semantic-python.json"))
    channel = grpc.insecure_channel(address)
    rows = []
    try:
        put = objects_pb2.PutObjectRequest()
        put.header.bucket.name = "fixture-bucket"
        put.header.object_key = "hello.txt"
        put.body = b"hello"
        put.complete = True
        put_call = channel.stream_unary(
            "/acyclic.objects.v2.ObjectsService/PutObject",
            request_serializer=put.SerializeToString,
            response_deserializer=objects_pb2.ObjectInfo.FromString,
        )
        # The generated Python message represents the three wire frames as a
        # single message type; send header, body, and complete separately.
        header = objects_pb2.PutObjectRequest()
        header.header.bucket.name = "fixture-bucket"
        header.header.object_key = "hello.txt"
        body = objects_pb2.PutObjectRequest(body=b"hello")
        complete = objects_pb2.PutObjectRequest(complete=True)
        put_response = put_call(iter([header, body, complete]), timeout=5)
        rows.append({"operation": "ObjectsService/PutObject", "status": "passed", "request_frames": 3, "response": put_response.SerializeToString().hex()})

        get = objects_pb2.GetObjectRequest()
        get.bucket.name = "fixture-bucket"
        get.object_key = "hello.txt"
        get_frames = unary_stream(channel, "acyclic.objects.v2.ObjectsService", "GetObject", get, objects_pb2.GetObjectResponse)
        frame_names = [x.WhichOneof("frame") for x in get_frames]
        header_frame = get_frames[0].header
        payload = get_frames[1].body
        expected_payload = b"rust-owned-object-payload"
        assert frame_names == ["header", "body"], frame_names
        assert payload == expected_payload, payload
        assert header_frame.object.etag == "fixture-object-etag", header_frame
        assert header_frame.object.size == len(expected_payload), header_frame
        rows.append({"operation": "ObjectsService/GetObject", "status": "passed", "response_frames": len(get_frames), "frame_kinds": frame_names, "etag": header_frame.object.etag, "size": header_frame.object.size, "body_utf8": payload.decode()})

        export = fs_pb2.ExportRequest()
        export.generation.workspace.name = "fixture"
        export.generation.generation_id = b"fixture-generation"
        export_frames = unary_stream(channel, "acyclic.filesystem.v2.FilesystemService", "Export", export, fs_pb2.ExportChunk)
        assert len(export_frames) == 1, export_frames
        chunk = export_frames[0]
        assert chunk.cursor == b"fixture-export-cursor-1", chunk
        assert chunk.object_id == b"fixture-export-object-1", chunk
        assert chunk.contents == b"rust-owned-filesystem-export", chunk
        assert chunk.terminal is True, chunk
        rows.append({"operation": "FilesystemService/Export", "status": "passed", "response_frames": 1, "cursor": chunk.cursor.decode(), "object_id": chunk.object_id.decode(), "contents_utf8": chunk.contents.decode(), "terminal": chunk.terminal})

        follow_request = stream_pb2.FollowRequest(path="fixture/events")
        follow = channel.unary_stream(
            "/acyclic.stream.v2.StreamService/Follow",
            request_serializer=follow_request.SerializeToString,
            response_deserializer=stream_pb2.Record.FromString,
        )(follow_request, timeout=5)
        follow_cancelled = False
        try:
            follow.cancel()
            follow_cancelled = follow.cancelled()
        finally:
            follow.cancel()
        assert follow_cancelled, "Follow call did not enter the cancelled state"
        rows.append({"operation": "StreamService/Follow", "status": "passed", "event": "cancellation", "cancelled": follow_cancelled})

        append = stream_pb2.AppendRequest(path="fixture/events", records=[b"recovered"], idempotency_key=b"semantic-recovery")
        append_response = channel.unary_unary(
            "/acyclic.stream.v2.StreamService/Append",
            request_serializer=append.SerializeToString,
            response_deserializer=stream_pb2.AppendResponse.FromString,
        )(append, timeout=5)
        rows.append({"operation": "StreamService/Append", "status": "passed", "event": "recovery", "response_bytes": len(append_response.SerializeToString())})
    except Exception as error:
        rows.append({"status": "failed", "error": repr(error)})
        raise
    finally:
        channel.close()
    document = {
        "schema": "acyclic.sdk.python.semantic-objects-filesystem-stream.v1",
        "consumer": "python",
        "execution_mode": "remote",
        "transport": "grpc",
        "fixture_address": address,
        "fixture_binary_sha256": os.environ.get("FIXTURE_BINARY_SHA256"),
        "fixture_source_sha256": os.environ.get("FIXTURE_SOURCE_SHA256"),
        "model_source_sha256": os.environ.get("MODEL_SOURCE_SHA256"),
        "package_sha256": os.environ.get("PACKAGE_SHA256"),
        "rows": rows,
        "passed": sum(row.get("status") == "passed" for row in rows),
        "failed": sum(row.get("status") == "failed" for row in rows),
    }
    out_path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"consumer": "python", "passed": document["passed"], "failed": document["failed"]}))


if __name__ == "__main__":
    main()
