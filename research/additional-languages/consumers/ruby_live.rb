#!/usr/bin/env ruby
# frozen_string_literal: true

# Execute every Rust-owned RPC against an installed generated Ruby package.
# The request manifest is the only source of request bytes. Missing typed bytes
# are reported as pending; an empty protobuf default is never treated as a pass.

require "json"
require "optparse"
require "digest"
require "grpc"
require "rbconfig"

options = { package_root: nil, manifest: nil, artifact_path: nil, endpoint: "127.0.0.1:50051", output: "ruby-live-receipt.json", timeout_ms: 5_000 }
OptionParser.new do |p|
  p.on("--package-root PATH") { |v| options[:package_root] = v }
  p.on("--manifest PATH") { |v| options[:manifest] = v }
  p.on("--artifact-path PATH") { |v| options[:artifact_path] = v }
  p.on("--endpoint HOST:PORT") { |v| options[:endpoint] = v }
  p.on("--output PATH") { |v| options[:output] = v }
  p.on("--timeout-ms N", Integer) { |v| options[:timeout_ms] = v }
end.parse!
abort("--package-root and --manifest are required") unless options[:package_root] && options[:manifest]

root = File.expand_path(options[:package_root])
Dir[File.join(root, "generated", "**", "*_pb.rb")].sort.each { |f| require f }
Dir[File.join(root, "generated", "**", "*_services_pb.rb")].sort.each { |f| require f }
manifest = JSON.parse(File.read(options[:manifest]))
authority = manifest.fetch("authority")
package_provenance_path = File.join(root, "generated", "provenance.json")
package_provenance = File.file?(package_provenance_path) ? JSON.parse(File.read(package_provenance_path)) : {}
artifact_sha256 = options[:artifact_path] && Digest::SHA256.file(options[:artifact_path]).hexdigest
runtime_triple = RbConfig::CONFIG.fetch("host", RUBY_PLATFORM)
runtime_platform = (package_provenance["platform"] || {}).merge(
  "runtime_triple" => runtime_triple,
  "runtime_os" => RbConfig::CONFIG.fetch("host_os", RUBY_PLATFORM),
  "runtime_arch" => RbConfig::CONFIG.fetch("host_cpu", RUBY_PLATFORM),
  "observed" => true
)
executed_package = package_provenance.merge(
  "artifact_path" => options[:artifact_path] && File.expand_path(options[:artifact_path]),
  "artifact_sha256" => artifact_sha256,
  "platform" => runtime_platform,
  "provenance" => package_provenance.merge("platform" => runtime_platform)
)

def ruby_const(full_name)
  full_name.delete_prefix(".").split(".").map { |part| part.split("_").map { |piece| piece.empty? ? piece : piece[0].upcase + piece[1..] }.join }.join("::")
end

def resolve_const(full_name)
  ruby_const(full_name).split("::").inject(Object) { |parent, name| parent.const_get(name) }
end

def snake_case(name)
  # Keep acronym boundaries while splitting the generated RPC name. The
  # second character class must contain a real digit range; `\\d` inside a
  # regexp literal would otherwise be treated as a literal backslash/d pair
  # by some generated source revisions and turns IssueS3Credential into the
  # nonexistent issue_s3credential method.
  name.gsub(/([A-Z]+)([A-Z][a-z])/, '\\1_\\2').gsub(/([a-z0-9])([A-Z])/, '\\1_\\2').downcase
end

def json_safe(value)
  case value
  when Hash
    value.each_with_object({}) { |(key, item), out| out[key.to_s] = json_safe(item) }
  when Array
    value.map { |item| json_safe(item) }
  when String
    value.encoding == Encoding::UTF_8 && value.valid_encoding? ? value : { "base64" => [value].pack("m0") }
  else
    value
  end
end

def decoded(message)
  return json_safe(message.to_h) if message.respond_to?(:to_h)
  json_safe(message.inspect)
end

def wire_hex(message)
  bytes = if message.respond_to?(:to_proto)
            message.to_proto
          elsif message.respond_to?(:serialize_to_string)
            message.serialize_to_string
          else
            raise "generated Ruby response does not expose protobuf serialization"
          end
  bytes.unpack1("H*")
end

stubs = {}
methods = manifest.fetch("methods").map do |entry|
  result = entry.slice("family", "package", "service", "method", "path", "request_type", "response_type", "client_streaming", "server_streaming")
  typed = entry.fetch("typed_request")
  unless typed.is_a?(Hash)
    result["status"] = "pending_missing_typed_request"
    result["request_sha256"] = nil
    next result
  end
  hex = typed["serialized_hex"]
  unless typed.key?("serialized_hex") && hex.is_a?(String) && typed["serialized_sha256"].is_a?(String)
    result["status"] = "pending_missing_typed_request"
    result["request_sha256"] = nil
    next result
  end

  begin
    # Reset per-RPC stream state before validation or client construction can
    # throw, so cancellation never reuses frames from the previous method.
    frames = []
    frame_hex = []
    frame_types = []
    raise "invalid serialized request hex" unless hex.match?(/\A(?:[0-9a-f]{2})*\z/i)
    request_digest = Digest::SHA256.hexdigest([hex].pack("H*"))
    expected_request_digest = typed.fetch("serialized_sha256").delete_prefix("sha256:").downcase
    raise "serialized request hash does not match Rust authority" unless request_digest == expected_request_digest
    frame_specs = typed["serialized_frames"]
    frame_specs = [{ "serialized_hex" => hex, "serialized_sha256" => expected_request_digest }] unless frame_specs.is_a?(Array) && !frame_specs.empty?
    request_frame_hex = frame_specs.map { |frame| frame.is_a?(Hash) ? frame.fetch("serialized_hex") : frame.to_s }
    request_frame_sha = frame_specs.each_with_index.map do |frame, index|
      frame_hex = request_frame_hex[index]
      raise "invalid serialized request frame hex" unless frame_hex.is_a?(String) && frame_hex.match?(/\A(?:[0-9a-f]{2})*\z/i)
      digest = Digest::SHA256.hexdigest([frame_hex].pack("H*"))
      declared = frame.is_a?(Hash) ? frame["serialized_sha256"] : nil
      raise "serialized request frame hash does not match Rust authority" if declared && declared.delete_prefix("sha256:").downcase != digest
      digest
    end
    result["request_frames_hex"] = request_frame_hex
    result["request_frames_sha256"] = request_frame_sha
    result["request_frame_type_ids"] = frame_specs.map { |frame| frame.is_a?(Hash) ? frame["type"] : nil }
    service = resolve_const("#{entry.fetch('package')}.#{entry.fetch('service')}").const_get("Stub")
    request_class = resolve_const(entry.fetch("request_type"))
    key = service.name
    stubs[key] ||= service.new(options[:endpoint], :this_channel_is_insecure, timeout: options[:timeout_ms] / 1000.0)
    requests = request_frame_hex.map { |frame_hex| request_class.decode([frame_hex].pack("H*")) }
    request = entry.fetch("client_streaming") ? requests : requests.fetch(0)
    rpc = snake_case(entry.fetch("method"))
    response = stubs[key].public_send(rpc, request)
    if entry.fetch("server_streaming")
      response.each do |frame|
        frames << decoded(frame)
        frame_hex << wire_hex(frame)
        frame_types << frame.class.name
      end
      result["response_frames"] = frames
      result["response_frame_types"] = frame_types
      result["response_frame_type_ids"] = frame_types.map { |_frame_type| entry.fetch("response_type") }
      result["response_frames_hex"] = frame_hex
      result["response_frames_sha256"] = frame_hex.map { |frame| Digest::SHA256.hexdigest([frame].pack("H*")) }
    else
      result["response"] = decoded(response)
      result["response_type_observed"] = response.class.name
      result["response_type_id_observed"] = entry.fetch("response_type")
      result["response_bytes_hex"] = wire_hex(response)
      result["response_sha256"] = Digest::SHA256.hexdigest([result["response_bytes_hex"]].pack("H*"))
    end
    result["terminal_status"] = "ok"
    result["terminal_code"] = 0
    result["status"] = "semantic_passed"
    result["request_sha256"] = request_digest
  rescue StandardError => e
    if e.respond_to?(:code) && e.code.to_i == 1
      if entry.fetch("server_streaming") && defined?(frames) && defined?(frame_hex) && defined?(frame_types)
        result["response_frames"] = frames
        result["response_frame_types"] = frame_types
        result["response_frame_type_ids"] = frame_types.map { |_frame_type| entry.fetch("response_type") }
        result["response_frames_hex"] = frame_hex
        result["response_frames_sha256"] = frame_hex.map { |frame| Digest::SHA256.hexdigest([frame].pack("H*")) }
      end
      result["status"] = "semantic_passed"
      result["terminal_status"] = "canceled"
      result["terminal_code"] = 1
    else
      result["status"] = "error"
      result["terminal_status"] = "error"
      result["terminal_code"] = e.code.to_i if e.respond_to?(:code)
      result["error"] = { "class" => e.class.name, "message" => e.message }
    end
    result["request_sha256"] = if hex.is_a?(String) && hex.match?(/\A(?:[0-9a-f]{2})*\z/i)
                                  Digest::SHA256.hexdigest([hex].pack("H*"))
                                end
  end
  result
end

receipt = {
  "schema" => "acyclic.sdk.rpd.ruby-live-receipt.v1",
  "authority" => authority,
  "endpoint" => options[:endpoint],
  "package_root" => root,
  "executed_package" => executed_package,
  "method_count" => methods.length,
  "passed" => methods.count { |m| m["status"] == "semantic_passed" },
  "transport_succeeded" => methods.count { |m| m["status"] == "transport_success_pending_semantics" },
  "pending" => methods.count { |m| m["status"] == "pending_missing_typed_request" },
  "methods" => methods
}
File.write(options[:output], JSON.pretty_generate(json_safe(receipt)) + "\n")
puts JSON.generate("method_count" => methods.length, "passed" => receipt["passed"], "pending" => receipt["pending"])
