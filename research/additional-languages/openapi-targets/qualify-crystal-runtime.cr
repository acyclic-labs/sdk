require "acyclic_actors_crystal"
require "acyclic_workers_crystal"
require "acyclic_stream_crystal"
require "acyclic_objects_crystal"
require "acyclic_inference_crystal"
require "json"

base = ARGV[0]
output_path = ARGV[1]
token = "bash-fixture-token"
checks = [] of NamedTuple(name: String, status: String, detail: String)

def pass_check(checks, name, detail)
  checks << {name: name, status: "pass", detail: detail}
end

def fail_check(checks, name, detail)
  checks << {name: name, status: "fail", detail: detail}
end

begin
  actor_request = AcyclicActorsHttp::AcyclicActorsV1InvokeActorRequest.new(body: "AQID", method: "POST", url: "/")
  raise "actor JSON body mismatch" unless actor_request.to_json.includes?(%q("body":"AQID"))
  pass_check(checks, "actors-json", "invoke request serialized body AQID")
  actor_response = AcyclicActorsHttp::Client.new(host: base, token: token, scheme: "http").invoke.actor(actor_request)
  raise "unexpected status #{actor_response.status}" unless actor_response.success? && actor_response.value.body == "AQID"
  pass_check(checks, "actors-http", "POST /v1/actors/invoke returned 200")
rescue ex
  fail_check(checks, "actors", ex.message || ex.to_s)
end

begin
  workers_request = AcyclicWorkersHttp::AcyclicWorkersV1InvokeDeploymentRequest.new(_alias: "prod", body: "AQID", method: "POST", url: "/")
  workers_response = AcyclicWorkersHttp::Client.new(host: base, token: token, scheme: "http").deployments.invoke("prod", workers_request)
  raise "unexpected status #{workers_response.status}" unless workers_response.success? && workers_response.value.body == "AQID"
  pass_check(checks, "workers-http", "POST /v1/workers/deployments/prod/invoke returned 200")
rescue ex
  fail_check(checks, "workers", ex.message || ex.to_s)
end

begin
  client = AcyclicStreamHttp::Client.new(host: base, token: token, scheme: "http")
  begin
    client.read.create(AcyclicStreamHttp::AcyclicStreamV2ReadRequest.new(path: "root", limit: 0))
    fail_check(checks, "stream-recovery-503", "expected retryable 503")
  rescue ex : AcyclicStreamHttp::ApiError
    raise "expected 503, got #{ex.code}" unless ex.code == 503
    pass_check(checks, "stream-recovery-503", "retryable 503 surfaced as ApiError")
  end
  response = client.read.create(AcyclicStreamHttp::AcyclicStreamV2ReadRequest.new(path: "root", limit: 1))
  raise "unexpected stream status #{response.status}" unless response.success?
  pass_check(checks, "stream-recovery-200", "retry after 503 returned 200")
rescue ex
  fail_check(checks, "stream", ex.message || ex.to_s)
end

begin
  header = AcyclicObjectsHttp::AcyclicObjectsV2PutObjectHeader.new(bucket: AcyclicObjectsHttp::AcyclicObjectsV2BucketRef.new(name: "test"), object_key: "test")
  request = AcyclicObjectsHttp::AcyclicObjectsV2PutObjectRequest.new(body: "AQID", header: header)
  response = AcyclicObjectsHttp::Client.new(host: base, token: token, scheme: "http").objects.put(request)
  raise "unexpected status #{response.status}" unless response.success? && response.value.body == "AQID"
  pass_check(checks, "objects-http", "POST /v2/objects/objects/put returned 200")
rescue ex
  fail_check(checks, "objects", ex.message || ex.to_s)
end

begin
  request = AcyclicInferenceHttp::InferenceCustomerV1GenerateRunRequest.new(context: "AQID", maximum_output: "18446744073709551615")
  response = AcyclicInferenceHttp::Client.new(host: base, token: token, scheme: "http").runs.generate(request)
  raise "unexpected status #{response.status}" unless response.success? && response.value.maximum_output == "18446744073709551615"
  pass_check(checks, "inference-http", "POST /v1/inference/runs/generate preserved uint64 string")
rescue ex
  fail_check(checks, "inference", ex.message || ex.to_s)
end

begin
  AcyclicActorsHttp::Client.new(host: base, token: "wrong", scheme: "http").invoke.actor(AcyclicActorsHttp::AcyclicActorsV1InvokeActorRequest.new(body: "AQID"))
  fail_check(checks, "auth-401", "expected unauthorized error")
rescue ex : AcyclicActorsHttp::ApiError
  if ex.code == 401
    pass_check(checks, "auth-401", "invalid bearer token returned 401")
  else
    fail_check(checks, "auth-401", "unexpected status #{ex.code}")
  end
rescue ex
  fail_check(checks, "auth-401", ex.message || ex.to_s)
end

passed = checks.count { |check| check[:status] == "pass" }
failed = checks.size - passed
receipt = {
  "schema" => "acyclic.sdk.openapi.runtime-scenario.v1",
  "target" => "crystal",
  "status" => failed == 0 ? "pass" : "fail",
  "runtime" => {"compiler" => "Crystal 1.21.1", "shards" => "0.20.0"},
  "fixture" => {"base_url" => base, "auth" => "Bearer bash-fixture-token", "families" => ["actors", "workers", "stream", "objects", "inference"]},
  "checks" => checks,
  "passed" => passed,
  "failed" => failed
}
File.write(output_path, receipt.to_pretty_json)
abort "Crystal runtime checks failed: #{failed}" if failed > 0
