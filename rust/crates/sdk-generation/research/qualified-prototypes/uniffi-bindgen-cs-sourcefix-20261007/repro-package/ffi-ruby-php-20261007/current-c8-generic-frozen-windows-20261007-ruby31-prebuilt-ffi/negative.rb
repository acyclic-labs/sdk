require "C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk/rust/crates/sdk-generation/research/qualified-prototypes/uniffi-bindgen-cs-sourcefix-20261007/repro-package/ffi-ruby-php-20261007/current-c8-generic-frozen-windows-20261007/acyclic_actors.rb"
require "C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk/rust/crates/sdk-generation/research/qualified-prototypes/uniffi-bindgen-cs-sourcefix-20261007/repro-package/ffi-ruby-php-20261007/current-c8-generic-frozen-windows-20261007/acyclic_actors_uniffi.rb"
checks = {}
begin; AcyclicActors::ActorId.new(""); checks[:actor_id_empty] = "FAIL"; rescue => e; checks[:actor_id_empty] = e.class.name; end
begin; AcyclicActors::CodeSha256.new("short"); checks[:sha_short] = "FAIL"; rescue => e; checks[:sha_short] = e.class.name; end
begin; AcyclicActors::PositiveU64.new(0); checks[:positive_zero] = "FAIL"; rescue => e; checks[:positive_zero] = e.class.name; end
begin; AcyclicActors::CurrentHeadMarker.new(false); checks[:current_head_false] = "FAIL"; rescue => e; checks[:current_head_false] = e.class.name; end
marker = AcyclicActors::CurrentHeadMarker.new(true)
variant = AcyclicActors::Start::CURRENT_HEAD.new(marker)
checks[:current_head_true] = (marker.value == true && variant.field_0 == marker) ? "PASS" : "FAIL"
begin; AcyclicActors::ActorId.uniffi_check_lower("actor-a"); checks[:actor_id_type] = "FAIL"; rescue TypeError => e; checks[:actor_id_type] = "TypeError"; end
begin; AcyclicActors::PositiveU64.uniffi_check_lower(1); checks[:positive_type] = "FAIL"; rescue TypeError => e; checks[:positive_type] = "TypeError"; end
begin; marker.value = false; checks[:readonly] = "FAIL"; rescue NoMethodError => e; checks[:readonly] = "NoMethodError"; end
puts checks.map { |k,v| "#{k}=#{v}" }
raise "negative check failed" if checks.values.any? { |v| v == "FAIL" }
puts "RUBY_CURRENT_C8_GENERIC_NOMINAL_NEGATIVE_PASS"

