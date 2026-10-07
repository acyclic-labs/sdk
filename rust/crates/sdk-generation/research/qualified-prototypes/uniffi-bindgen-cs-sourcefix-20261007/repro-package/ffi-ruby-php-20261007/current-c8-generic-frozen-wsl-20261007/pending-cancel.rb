require "json"
require "net/http"
require "uri"
require "/tmp/ruby-c8-wsl-generated-frozen-20261007/acyclic_actors.rb"
require "/tmp/ruby-c8-wsl-generated-frozen-20261007/acyclic_actors_uniffi.rb"
options = JSON.parse(File.read(ARGV.fetch(0)))
raise "missing control endpoint" unless options["controlEndpoint"]
control = URI(options["controlEndpoint"])
def state(control)
  JSON.parse(Net::HTTP.get(URI("#{control}/state")))
end
actor_id = AcyclicActors::ActorId.new("pending-actor-a")
request = AcyclicActors::InspectActorRequest.new(actor_id: actor_id)
client = AcyclicActorsUniffi.connect_actors_with_ca(options["endpoint"], options["token"], options["caCertificate"].b, nil)
handle = AcyclicActorsUniffi::CancellationHandle.new
error = nil
thread = Thread.new do
  begin
    client.inspect_actor(request, handle)
  rescue Exception => e
    error = e
  end
end
started = false
20.times do
  s = state(control)
  if s["started"].to_i >= 1 && s["active"].to_i >= 1
    started = true
    break
  end
  sleep 0.1
end
raise "pending operation did not enter fixture: #{state(control)}" unless started
handle.cancel
thread.join(10)
raise "worker did not finish" if thread.alive?
s = state(control)
raise "fixture did not observe abort cleanup: #{s}" unless s["aborted"].to_i >= 1 && s["active"].to_i == 0
raise "missing typed cancellation error: #{error.inspect}" unless error && error.class.name.end_with?("::Cancelled")
puts "pending_started=#{s["started"]}"
puts "pending_aborted=#{s["aborted"]}"
puts "pending_active=#{s["active"]}"
puts "cancel_handle_is_cancelled=#{handle.is_cancelled ? 'true' : 'false'}"
puts "cancel_error=#{error.class}:#{error.message}"
puts "RUBY_CURRENT_C8_GENERIC_PENDING_CANCEL_PASS"
