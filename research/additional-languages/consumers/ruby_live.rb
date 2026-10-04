#!/usr/bin/env ruby
# frozen_string_literal: true

# Execute every Rust-owned RPC against an installed generated Ruby package.
# The request manifest is the only source of request bytes. Missing typed bytes
# are reported as pending; an empty protobuf default is never treated as a pass.

require "json"
require "optparse"
require "digest"
require "grpc"

options = { package_root: nil, manifest: nil, endpoint: "127.0.0.1:50051", output: "ruby-live-receipt.json", timeout_ms: 5_000 }
OptionParser.new do |p|
  p.on("--package-root PATH") { |v| options[:package_root] = v }
  p.on("--manifest PATH") { |v| options[:manifest] = v }
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

def ruby_const(full_name)
  full_name.delete_prefix(".").split(".").map { |part| part.split("_").map(&:capitalize).join }.join("::")
end

def resolve_const(full_name)
  ruby_const(full_name).split("::").inject(Object) { |parent, name| parent.const_get(name) }
end

def snake_case(name)
  name.gsub(/([A-Z]+)([A-Z][a-z])/, '\\1_\\2').gsub(/([a-z\\d])([A-Z])/, '\\1_\\2').downcase
end

def decoded(message)
  return message.to_h if message.respond_to?(:to_h)
  message.inspect
end

stubs = {}
methods = manifest.fetch("methods").map do |entry|
  result = entry.slice("family", "package", "service", "method", "path", "request_type", "response_type", "client_streaming", "server_streaming")
  typed = entry.fetch("typed_request")
  hex = typed["serialized_hex"]
  unless hex && !hex.empty?
    result["status"] = "pending_missing_typed_request"
    result["request_sha256"] = nil
    next result
  end

  begin
    service = resolve_const("#{entry.fetch('package')}.#{entry.fetch('service')}").const_get("Stub")
    request_class = resolve_const(entry.fetch("request_type"))
    key = service.name
    stubs[key] ||= service.new(options[:endpoint], :this_channel_is_insecure, timeout: options[:timeout_ms] / 1000.0)
    request = request_class.decode([hex].pack("H*"))
    rpc = snake_case(entry.fetch("method"))
    response = stubs[key].public_send(rpc, request)
    frames = []
    if entry.fetch("server_streaming")
      response.each { |frame| frames << decoded(frame) }
      result["response_frames"] = frames
    else
      result["response"] = decoded(response)
    end
    result["status"] = "transport_success_pending_semantics"
    result["request_sha256"] = Digest::SHA256.hexdigest([hex].pack("H*"))
  rescue StandardError => e
    result["status"] = "error"
    result["error"] = { "class" => e.class.name, "message" => e.message }
    result["request_sha256"] = Digest::SHA256.hexdigest([hex].pack("H*"))
  end
  result
end

receipt = {
  "schema" => "acyclic.sdk.rpd.ruby-live-receipt.v1",
  "authority" => authority,
  "endpoint" => options[:endpoint],
  "package_root" => root,
  "method_count" => methods.length,
  "passed" => methods.count { |m| m["status"] == "semantic_passed" },
  "transport_succeeded" => methods.count { |m| m["status"] == "transport_success_pending_semantics" },
  "pending" => methods.count { |m| m["status"] == "pending_missing_typed_request" },
  "methods" => methods
}
File.write(options[:output], JSON.pretty_generate(receipt) + "\n")
puts JSON.generate("method_count" => methods.length, "passed" => receipt["passed"], "pending" => receipt["pending"])
