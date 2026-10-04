require "acyclic_actors_crystal"
require "acyclic_stream_crystal"
require "json"

base = ARGV[0]
output_path = ARGV[1]
token = "bash-fixture-token"
checks = [] of String

actor_request = AcyclicActorsHttp::AcyclicActorsV1InvokeActorRequest.new(body: "AQID", method: "POST", url: "/")
raise "actor serialization" unless actor_request.to_json.includes?(%q("body":"AQID"))
checks << "actors-json"
actor = AcyclicActorsHttp::Client.new(host: base, token: token, scheme: "http").invoke.actor(actor_request)
raise "actor status #{actor.status}" unless actor.success? && actor.value.body == "AQID"
checks << "actors-http-200"

stream = AcyclicStreamHttp::Client.new(host: base, token: token, scheme: "http")
begin
  stream.read.create(AcyclicStreamHttp::AcyclicStreamV2ReadRequest.new(path: "root", limit: 0))
  raise "stream retry status missing"
rescue ex : AcyclicStreamHttp::ApiError
  raise "stream expected 503, got #{ex.code}" unless ex.code == 503
  checks << "stream-http-503"
end
recovered = AcyclicStreamHttp::Client.new(host: base, token: token, scheme: "http").read.create(AcyclicStreamHttp::AcyclicStreamV2ReadRequest.new(path: "root", limit: 1))
raise "stream recovery status #{recovered.status}" unless recovered.success? && recovered.value.record.try(&.value) == "AQID"
checks << "stream-http-200"

begin
  AcyclicActorsHttp::Client.new(host: base, token: "wrong", scheme: "http").invoke.actor(actor_request)
  raise "auth rejection missing"
rescue ex : AcyclicActorsHttp::ApiError
  raise "auth expected 401, got #{ex.code}" unless ex.code == 401
  checks << "auth-401"
end

receipt = {
  "schema" => "acyclic.sdk.openapi.runtime-scenario.v1",
  "target" => "crystal",
  "status" => "pass",
  "runtime" => {"compiler" => "Crystal 1.21.1", "shards" => "0.20.0"},
  "fixture" => {"base_url" => base, "auth" => "Bearer bash-fixture-token", "families" => ["actors", "stream"]},
  "checks" => checks,
  "passed" => checks.size,
  "failed" => 0
}
File.write(output_path, receipt.to_pretty_json)
puts "crystal-runtime-pass checks=#{checks.size}"
