require 'google/protobuf/descriptor_pb'
gem 'acyclic-sdk-transport', '= 0.2.0.alpha.1'
require 'actors/v1/actors_services_pb'
require 'workers/v1/workers_services_pb'
require 'stream/v1/stream_services_pb'

def check(value, message)
  raise message unless value
end

# Remove only Buf's file-level image tag; every other unknown field is retained.
def api_bytes(proto)
  proto.clear_source_code_info
  bytes = Google::Protobuf::FileDescriptorProto.encode(proto)
  offset = 0
  read_varint = lambda do
    value = 0
    10.times do |index|
      raise 'truncated descriptor varint' if offset >= bytes.bytesize
      byte = bytes.getbyte(offset); offset += 1
      value |= (byte & 127) << (index * 7)
      return value if byte < 128
    end
    raise 'invalid descriptor varint'
  end
  retained = ''.b
  while offset < bytes.bytesize
    start = offset; tag = read_varint.call
    kind = tag & 7; field = tag >> 3
    check(field > 0, 'invalid descriptor tag')
    case kind
    when 0 then read_varint.call
    when 1 then offset += 8
    when 2 then length = read_varint.call; offset += length
    when 5 then offset += 4
    else raise 'unsupported descriptor wire kind'
    end
    check(offset <= bytes.bytesize, 'truncated descriptor field')
    if field == 8042
      check(kind == 2, 'unexpected Buf metadata encoding')
    else
      retained << bytes.byteslice(start, offset - start)
    end
  end
  retained
end

installed = File.realpath(Gem.loaded_specs.fetch('acyclic-sdk-transport').full_gem_path)
check(installed == File.realpath(ENV.fetch('SDK_QUALIFIED_GEM')), 'unexpected SDK installation')
features = $LOADED_FEATURES.grep(%r{/(actors/v1/actors|workers/v1/workers|stream/v1/stream)(_services)?_pb\.rb$})
check(features.size == 6, 'missing or duplicate generated source')
features.each { |file| check(File.realpath(file).start_with?(installed + '/'), 'SDK source did not load from installed gem') }

families = [
  [Acyclic::Actors::V1::CreateActorRequest, Acyclic::Actors::V1::ActorsService::Service],
  [Acyclic::Workers::V1::PublishVersionRequest, Acyclic::Workers::V1::WorkersService::Service],
  [Acyclic::Stream::V1::AppendRequest, Acyclic::Stream::V1::StreamService::Service]
]
check(ARGV.length == 3, 'pass three verified Rust descriptors')
families.zip(ARGV).each do |(message, service), path|
  actual = message.descriptor.file_descriptor.to_proto
  matches = Google::Protobuf::FileDescriptorSet.decode(File.binread(path)).file.select { |file| file.name == actual.name }
  check(matches.length == 1, 'missing or duplicate Rust descriptor')
  expected = matches.fetch(0)
  check(api_bytes(expected) == api_bytes(actual), "API descriptor changed: #{actual.name}")
  schema = expected.service.find { |candidate| "#{expected.package}.#{candidate.name}" == service.service_name }
  check(schema, 'service missing in Rust descriptor')
  check(service.rpc_descs.keys.map(&:to_s).sort == schema['method'].map(&:name).sort, 'RPC method set differs')
  schema['method'].each do |method|
    rpc = service.rpc_descs.fetch(method.name.to_sym)
    check(rpc.input.is_a?(GRPC::RpcDesc::Stream) == method.client_streaming, 'client-streaming shape differs')
    check(rpc.output.is_a?(GRPC::RpcDesc::Stream) == method.server_streaming, 'server-streaming shape differs')
    input = rpc.input.is_a?(GRPC::RpcDesc::Stream) ? rpc.input.type : rpc.input
    output = rpc.output.is_a?(GRPC::RpcDesc::Stream) ? rpc.output.type : rpc.output
    check(".#{input.descriptor.name}" == method.input_type && ".#{output.descriptor.name}" == method.output_type, 'RPC message types differ')
  end
end

bytes = "\x00\xff".b
actor = Acyclic::Actors::V1::CreateActorRequest.new(code_sha256: bytes, home_region: 'test', idempotency_key: 'test')
worker = Acyclic::Workers::V1::PublishVersionRequest.new(javascript_module: bytes, expected_sha256: bytes, idempotency_key: 'test')
append = Acyclic::Stream::V1::AppendRequest.new(path: 'test/path', records: [bytes, ''.b], if_tail: 2**64 - 1, idempotency_key: bytes)
read = Acyclic::Stream::V1::ReadRequest.new(path: 'test/path', from: 2**64 - 1, limit: 2**32 - 1)
check(actor.code_sha256 == bytes, 'actor bytes changed during construction')
check(worker.javascript_module == bytes && worker.expected_sha256 == bytes, 'worker bytes changed during construction')
check(append.records.to_a == [bytes, ''.b] && append.idempotency_key == bytes, 'stream bytes changed during construction')
check(append.if_tail == 2**64 - 1 && read.from == 2**64 - 1, 'uint64 maximum changed during construction')
check(read.limit == 2**32 - 1, 'uint32 maximum changed during construction')
[actor, worker, append, read].each { |message| check(message.class.decode(message.class.encode(message)) == message, 'wire round trip changed') }
absent = Acyclic::Stream::V1::AppendRequest.new
explicit = Acyclic::Stream::V1::AppendRequest.new(if_tail: 0)
check(!absent.has_if_tail? && explicit.has_if_tail?, 'optional presence lost')
check(absent.class.encode(absent).empty? && !explicit.class.encode(explicit).empty?, 'optional zero encoding lost')
target = Acyclic::Workers::V1::JobTarget.new(version_sha256: bytes)
check(target.target == :version_sha256 && target.deployment_alias.empty?, 'version branch lost')
target.deployment_alias = 'test-alias'
check(target.target == :deployment_alias && target.version_sha256.empty?, 'alias did not clear version')
target = target.class.decode(target.class.encode(target))
check(target.target == :deployment_alias && target.deployment_alias == 'test-alias', 'alias wire branch lost')
target.version_sha256 = bytes
check(target.target == :version_sha256 && target.deployment_alias.empty?, 'version did not clear alias')
check(target.class.decode(target.class.encode(target)) == target, 'version wire branch lost')
target.clear_target
check(target.target.nil? && target.version_sha256.empty? && target.deployment_alias.empty?, 'oneof clear lost')
puts 'PASS: installed Ruby descriptors, bytes, unsigned bounds, optional presence, oneof and gRPC shapes'
