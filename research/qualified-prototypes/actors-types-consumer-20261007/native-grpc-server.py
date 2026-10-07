import grpc
from concurrent import futures
import pathlib
METHODS=['CreateActor','UpdateActor','InspectActor','AddSubscription','RemoveSubscription','ResumeSubscription','CheckpointActor','InvokeActor']
def reply(request, context): return b''
handlers={m:grpc.unary_unary_rpc_method_handler(reply, request_deserializer=lambda x:x, response_serializer=lambda x:x) for m in METHODS}
server=grpc.server(futures.ThreadPoolExecutor(max_workers=16))
server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('acyclic.actors.v1.ActorsService',handlers),))
root=pathlib.Path('research/qualified-prototypes/actors-types-consumer-20261007')
key=(root/'native-test-key.pem').read_bytes(); cert=(root/'native-test-cert.pem').read_bytes()
server.add_secure_port('127.0.0.1:54443', grpc.ssl_server_credentials(((key,cert),)))
server.start(); print('https://127.0.0.1:54443', flush=True); server.wait_for_termination()
