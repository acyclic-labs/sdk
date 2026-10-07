require "json"
require "/tmp/ruby-c8-wsl-generated-frozen-20261007/acyclic_actors.rb"
require "/tmp/ruby-c8-wsl-generated-frozen-20261007/acyclic_actors_uniffi.rb"

def check(label)
  raise "#{label} failed" unless yield
  puts "#{label}=PASS"
end

values = [2**63, 2**64 - 1]
values.each do |value|
  wrapped = AcyclicActors::PositiveU64.new(value)
  check("positive_u64_#{value}") { wrapped.value == value && wrapped.frozen? }
end

limits = AcyclicActors::ActorLimits.new(
  handler_timeout_millis: AcyclicActors::PositiveU64.new(2**63),
  memory_bytes: AcyclicActors::PositiveU64.new(2**64 - 1),
  checkpoint_bytes: AcyclicActors::PositiveU64.new(9007199254740993)
)
roundtrip_limits = AcyclicActors::RustBuffer.alloc_from_TypeActorLimits(limits).consumeIntoTypeActorLimits
check("actor_limits_wire_u64") do
  roundtrip_limits.frozen? &&
    roundtrip_limits.handler_timeout_millis.value == 2**63 &&
    roundtrip_limits.memory_bytes.value == 2**64 - 1 &&
    roundtrip_limits.checkpoint_bytes.value == 9007199254740993
end

values.each do |value|
  start = AcyclicActors::Start::CURSOR.new(value)
  roundtrip = AcyclicActors::RustBuffer.alloc_from_TypeStart(start).consumeIntoTypeStart
  check("cursor_wire_u64_#{value}") { start.frozen? && roundtrip.frozen? && roundtrip.field_0 == value }
end

check("record_frozen") { limits.frozen? && limits.respond_to?(:handler_timeout_millis) }
check("nominal_frozen") { AcyclicActors::ActorId.new("actor-a").frozen? }
begin
  limits.handler_timeout_millis = AcyclicActors::PositiveU64.new(1)
  raise "record mutation unexpectedly succeeded"
rescue NoMethodError, FrozenError
  puts "record_readonly=PASS"
end
begin
  AcyclicActors::Start.new
  raise "enum root factory unexpectedly succeeded"
rescue RuntimeError
  puts "enum_private_factory=PASS"
end
puts "RUBY_CURRENT_C8_GENERIC_FROZEN_U64_PASS"
