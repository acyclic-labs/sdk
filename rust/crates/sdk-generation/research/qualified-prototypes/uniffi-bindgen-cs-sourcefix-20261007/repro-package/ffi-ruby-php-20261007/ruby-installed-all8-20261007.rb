require "json"
require "acyclic_actors_uniffi"

options = JSON.parse(File.read("/tmp/mac-fixture-options-normal.json"))
actor_id = AcyclicActorsUniffi::ActorId.new("actor-a")
digest = AcyclicActorsUniffi::CodeSha256.new(Array.new(32, 1).pack("C*"))
limits = AcyclicActorsUniffi::ActorLimits.new(1, 2, 3)
binding = AcyclicActorsUniffi::Binding.new("binding-a", "capability-a", "resource-a")
subscription = AcyclicActorsUniffi::SubscriptionSpec.new("subscription-a", "events/input", AcyclicActorsUniffi::SubscriptionStart::CURSOR.new(9007199254740993), true)
create = AcyclicActorsUniffi::CreateActorRequest.new(digest, "eu", [binding], limits, [subscription], "ruby-create-installed")
update = AcyclicActorsUniffi::UpdateActorRequest.new(actor_id, digest, [binding], limits, 0, "ruby-update-installed")
inspect_request = AcyclicActorsUniffi::InspectActorRequest.new(actor_id)
add = AcyclicActorsUniffi::AddSubscriptionRequest.new(actor_id, subscription, "ruby-add-installed")
remove = AcyclicActorsUniffi::RemoveSubscriptionRequest.new(actor_id, "subscription-a", "ruby-remove-installed")
resume = AcyclicActorsUniffi::ResumeSubscriptionRequest.new(actor_id, "subscription-a", "ruby-resume-installed")
checkpoint = AcyclicActorsUniffi::CheckpointActorRequest.new(actor_id, "checkpoint-a")
invoke = AcyclicActorsUniffi::InvokeActorRequest.new(actor_id, "POST", "/invoke", "request-body".b, [AcyclicActorsUniffi::Header.new(name: "content-type", value: "application/json")])

client = AcyclicActorsUniffi.connect_actors_with_ca(options["endpoint"], options["token"], options["caCertificate"].b, nil)
client.create_actor(create, nil)
client.update_actor(update, nil)
client.inspect_actor(actor_id, nil)
client.inspect_actor_request(inspect_request, nil)
client.add_subscription(add, nil)
client.remove_subscription(remove, nil)
client.resume_subscription(resume, nil)
client.checkpoint_actor(checkpoint, nil)
client.invoke_actor(invoke, nil)
puts "RUBY_INSTALLED_PACKAGE_ALL8_PASS"
