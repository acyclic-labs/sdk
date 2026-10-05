# frozen_string_literal: true
require "json"
require "base64"
require "digest"
require "timeout"
require File.expand_path("../ruby/lib/acyclic_sdk", __dir__)

ROOT = File.expand_path("..", __dir__)
MANIFEST = File.join(ROOT, "work", "rpd-rust-106-manifest-current-20261005.json")
ENDPOINT = ENV.fetch("FIXTURE_GRPC_ADDRESS", "127.0.0.1:55839")
STREAMING_RPCS = %w[
  acyclic.stream.v2.StreamService/Read
  acyclic.stream.v2.StreamService/Follow
  acyclic.stream.v2.StreamService/Children
  acyclic.objects.v2.ObjectsService/GetObject
  acyclic.machines.v1.MachinesService/WatchOperation
  acyclic.filesystem.v2.FilesystemService/Export
  acyclic.harness.v2.HarnessService/Replay
  inference.customer.v1.RunsService/Watch
].freeze
CLIENT_STREAMING_RPCS = %w[
  acyclic.objects.v2.ObjectsService/PutObject
  acyclic.objects.v2.MultipartService/UploadPart
  acyclic.filesystem.v2.FilesystemService/Import
].freeze

module Helpers
  module_function
  def constantize(path)
    path.split(".").map { |part| part.gsub(/(^|_)([a-z])/) { Regexp.last_match(2).upcase } }.join("::")
  end
  def snake(value)
    value.gsub(/([A-Z]+)([A-Z][a-z])/, '\\1_\\2').gsub(/([a-z\\d])([A-Z])/, '\\1_\\2').downcase
  end
  def service_class(service)
    Object.const_get("#{constantize(service)}::Stub")
  end
  def message_class(type)
    Google::Protobuf::DescriptorPool.generated_pool.lookup(type).msgclass
  end
  def sha(bytes)
    "sha256:#{Digest::SHA256.hexdigest(bytes)}"
  end
  def semantic_hash(bytes)
    Digest::SHA256.hexdigest(bytes)
  end
end

manifest = JSON.parse(File.read(MANIFEST))
raise "manifest is not complete" unless manifest.fetch("complete") == true
raise "manifest does not contain 106 records" unless manifest.fetch("record_count") == 106
stubs = {}
results = []
manifest.fetch("records").each_with_index do |record, index|
  rpc = record.fetch("rpc")
  service, method = rpc.split("/", 2)
  ruby_method = Helpers.snake(method)
  stub = (stubs[service] ||= Helpers.service_class(service).new(ENDPOINT, :this_channel_is_insecure))
  request_class = Helpers.message_class(record.fetch("request_type"))
  request = request_class.decode(Base64.decode64(record.fetch("request_base64")))
  expected_frames = record.fetch("response_frames")
  expected_status = record.fetch("expected_status")
  expected_error = expected_status == "observed-status" && expected_frames.empty?
  outcome = { "index" => index, "rpc" => rpc, "status" => "passed", "transport" => "grpc", "request_type" => record.fetch("request_type"), "response_type" => record["response_type"], "expected_status" => expected_status }
  begin
    client = Acyclic::Remote::Client.new(family: record.fetch("family"), runtime: :native, bearer_auth: record.fetch("family") != "machines", endpoint: "grpc://#{ENDPOINT}", invoker: lambda { |_operation, req, _transport|
      if CLIENT_STREAMING_RPCS.include?(rpc)
        stub.public_send(ruby_method, [req])
      elsif STREAMING_RPCS.include?(rpc)
        operation = stub.public_send(ruby_method, req, return_op: true)
        frames = []
        begin
          operation.execute.each do |frame|
            frames << frame.class.encode(frame)
            break if frames.length >= [expected_frames.length, 1].max
          end
        ensure
          operation.cancel if operation.respond_to?(:cancel) && !operation.cancelled?
        end
        frames
      else
        stub.public_send(ruby_method, req)
      end
    })
    response = Timeout.timeout(20) { client.call(rpc, request) }
    frames = response.is_a?(Array) ? response : [response.class.encode(response)]
    actual = frames.map { |bytes| { "sha256" => Helpers.sha(bytes), "bytes" => bytes.bytesize, "response_base64" => Base64.strict_encode64(bytes) } }
    expected = expected_frames.map { |frame| { "sha256" => frame.fetch("response_sha256"), "bytes" => Base64.decode64(frame.fetch("response_base64")).bytesize, "response_base64" => frame.fetch("response_base64") } }
    outcome["frames"] = actual
    outcome["expected_frames"] = expected
    unless actual.map { |frame| frame.fetch("sha256") } == expected.map { |frame| frame.fetch("sha256") }
      outcome["status"] = "response_mismatch"
    end
  rescue StandardError => error
    expected_timeout = expected_error && rpc == "acyclic.stream.v2.StreamService/Follow" && error.is_a?(Timeout::Error)
    outcome["status"] = if expected_timeout || (expected_error && error.message.match?(/(?:GRPC|NOT_FOUND|INVALID_ARGUMENT|FAILED_PRECONDITION|UNIMPLEMENTED|CANCELLED|DEADLINE_EXCEEDED)/i))
      "passed"
    else
      "failed"
    end
    outcome["error_class"] = error.class.name
    outcome["error"] = error.message
    outcome["cancellation_observed"] = true if expected_timeout
  end
  results << outcome
  puts JSON.generate(outcome)
end
summary = { "schema" => "acyclic.sdk.ruby.installed-106-rpc.v2", "status" => (results.all? { |r| r["status"] == "passed" } ? "passed" : "failed"), "endpoint" => ENDPOINT, "record_count" => results.length, "passed" => results.count { |r| r["status"] == "passed" }, "response_mismatch" => results.count { |r| r["status"] == "response_mismatch" }, "failed" => results.count { |r| r["status"] == "failed" }, "results" => results }
File.write(ENV.fetch("RPD_RUBY_RECEIPT", File.join(ROOT, "work", "rpd-ruby-106-result.json")), JSON.pretty_generate(summary) + "\n")
exit(summary["status"] == "passed" ? 0 : 1)
