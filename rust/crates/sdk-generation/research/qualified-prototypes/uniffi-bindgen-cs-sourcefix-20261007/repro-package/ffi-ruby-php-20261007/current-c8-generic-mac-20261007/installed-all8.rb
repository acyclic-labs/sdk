require "json"
require "/tmp/ruby-candidate-mac-c8-generic-generated4-20261007/acyclic_actors.rb"
require "/tmp/ruby-candidate-mac-c8-generic-generated4-20261007/acyclic_actors_uniffi.rb"
options = JSON.parse(File.read(ARGV.fetch(0)))
actor_id = AcyclicActors::ActorId.new(options["actorId"])
digest = AcyclicActors::CodeSha256.new(Array.new(32, 1).pack("C*"))
limits = AcyclicActors::ActorLimits.new(handler_timeout_millis: AcyclicActors::PositiveU64.new(1), memory_bytes: AcyclicActors::PositiveU64.new(2), checkpoint_bytes: AcyclicActors::PositiveU64.new(3))
binding = AcyclicActors::Binding.new(name: "binding-a", capability: "capability-a", resource: "resource-a")
subscription = AcyclicActors::SubscriptionSpec.new(subscription_id: "subscription-a", stream_path: "events/input", start: AcyclicActors::SubscriptionStart.new(start: AcyclicActors::Start::CURSOR.new(9007199254740993)), placement_anchor: true)
create = AcyclicActors::CreateActorRequest.new(code_sha256: digest, home_region: "eu", bindings: [binding], limits: limits, subscriptions: [subscription], idempotency_key: "ruby-c8-create")
update = AcyclicActors::UpdateActorRequest.new(actor_id: actor_id, code_sha256: digest, bindings: [binding], limits: limits, expected_configuration_revision: 0, idempotency_key: "ruby-c8-update")
inspect_request = AcyclicActors::InspectActorRequest.new(actor_id: actor_id)
add = AcyclicActors::AddSubscriptionRequest.new(actor_id: actor_id, subscription: subscription, idempotency_key: "ruby-c8-add")
remove = AcyclicActors::RemoveSubscriptionRequest.new(actor_id: actor_id, subscription_id: "subscription-a", idempotency_key: "ruby-c8-remove")
resume = AcyclicActors::ResumeSubscriptionRequest.new(actor_id: actor_id, subscription_id: "subscription-a", idempotency_key: "ruby-c8-resume")
checkpoint = AcyclicActors::CheckpointActorRequest.new(actor_id: actor_id, idempotency_key: "checkpoint-a")
invoke = AcyclicActors::InvokeActorRequest.new(actor_id: actor_id, method: "POST", url: "/invoke", body: "request-body".b, headers: [AcyclicActors::Header.new(name: "content-type", value: "application/json")])
client = AcyclicActorsUniffi.connect_actors_with_ca(options["endpoint"], options["token"], options["caCertificate"].b, nil)
puts "connect_ca=PASS"
client.create_actor(create, nil); puts "create_actor=PASS"
client.update_actor(update, nil); puts "update_actor=PASS"
client.inspect_actor(inspect_request, nil); puts "inspect_actor=PASS"
client.add_subscription(add, nil); puts "add_subscription=PASS"
client.remove_subscription(remove, nil); puts "remove_subscription=PASS"
client.resume_subscription(resume, nil); puts "resume_subscription=PASS"
client.checkpoint_actor(checkpoint, nil); puts "checkpoint_actor=PASS"
client.invoke_actor(invoke, nil); puts "invoke_actor=PASS"
puts "RUBY_CURRENT_C8_GENERIC_ALL8_PASS"
