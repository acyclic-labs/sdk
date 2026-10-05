# frozen_string_literal: true

require "fileutils"
require "json"
require "digest"
require "rbconfig"

ROOT = File.expand_path("..", __dir__)
OUT = File.join(__dir__, "generated")

def option_value(arguments, name)
  index = arguments.index(name)
  return nil unless index
  value = arguments[index + 1]
  abort "#{name} requires a value" if value.nil? || value.start_with?("--")
  value
end

def command!(name)
  path = if Gem.win_platform?
    bundled = File.join(Gem.bindir, "#{name}.bat")
    File.file?(bundled) ? bundled : nil
  else
    `command -v #{name}`.lines.first&.strip
  end
  abort "#{name} is required; install the pinned grpc-tools package" if path.nil? || path.empty?
  path
end

def run!(argv)
  puts "> #{argv.join(' ')}"
  abort "generation failed" unless system(*argv)
end

def pascal_identifier(value)
  value.split(/[^a-zA-Z0-9]+/).reject(&:empty?).map(&:capitalize).join
end

def ruby_value_type(wire_kind)
  case wire_kind
  when "string", "bytes" then "String"
  when "signed_integer", "unsigned_integer", "enum" then "Integer"
  else "untyped"
  end
end

def ruby_validation_lines(item)
  id = item.fetch("id")
  wire_kind = item.fetch("wire_kind")
  lines = []
  if ["string", "bytes"].include?(wire_kind)
    lines << "raise ArgumentError, \"#{id}: expected String\" unless value.is_a?(String)"
    lines << "raise ArgumentError, \"#{id}: value must be non-empty\" if value.empty?" if item.fetch("rules").any? { |rule| rule["kind"] == "non_empty" }
    lines << "raise ArgumentError, \"#{id}: value must be valid UTF-8\" unless value.valid_encoding?" if wire_kind == "string" && item.fetch("rules").any? { |rule| rule["kind"] == "utf8" }
  elsif ["signed_integer", "unsigned_integer", "enum"].include?(wire_kind)
    lines << "raise ArgumentError, \"#{id}: expected Integer\" unless value.is_a?(Integer)"
  end
  item.fetch("rules").each do |rule|
    case rule.fetch("kind")
    when "fixed_length"
      lines << "raise ArgumentError, \"#{id}: expected #{rule.fetch("length")} bytes\" unless value.bytesize == #{rule.fetch("length")}"
    when "strictly_positive"
      lines << "raise ArgumentError, \"#{id}: must be positive\" unless value > 0"
    when "non_negative"
      lines << "raise ArgumentError, \"#{id}: must be non-negative\" unless value >= 0"
    when "max_items"
      lines << "raise ArgumentError, \"#{id}: exceeds maximum\" unless value <= #{rule.fetch("max")}"
    when "bounded_integer"
      lines << "raise ArgumentError, \"#{id}: outside bounds\" unless value.between?(#{rule.fetch("min")}, #{rule.fetch("max")})"
    when "exact_oneof"
      lines << "raise ArgumentError, \"#{id}: exactly one arm is required\" unless value.respond_to?(:keys) && value.keys.length == 1"
    end
  end
  lines
end

lock = JSON.parse(File.read(File.join(__dir__, "generator.lock.json")))
abort "unexpected generator lock" unless lock.fetch("generator_version") == "1.82.0"

arguments = ARGV.dup
schema_root = option_value(arguments, "--schema-root")
manifest_path = option_value(arguments, "--manifest")
schema_roots = if schema_root
  [File.expand_path(schema_root)]
else
  [File.join(ROOT, "proto"), File.join(ROOT, "rust", "crates", "stream", "proto")]
end
manifest_path = File.expand_path(manifest_path) if manifest_path
authority = manifest_path && File.file?(manifest_path) ? JSON.parse(File.read(manifest_path)) : nil
families = authority && authority["families"].is_a?(Array) ? authority["families"] : []
schema_names = families.filter_map { |family| family["source"] if family.is_a?(Hash) }.then { |names|
  names.empty? ? (authority && authority["schemas"].is_a?(Array) && !authority["schemas"].empty? ? authority["schemas"] : [
  "actors/v1/actors.proto",
  "stream/v2/stream.proto"
]) : names
}
dependency_names = schema_roots.flat_map do |candidate|
  next [] unless Dir.exist?(candidate)
  Dir[File.join(candidate, "**", "*.proto")].filter_map do |path|
    path.delete_prefix("#{candidate}#{File::SEPARATOR}").tr("\\", "/")
  end
end
schema_names = if manifest_path
  (schema_names + ["validation/v1/options.proto"]).uniq
else
  (schema_names + dependency_names).uniq
end
expected_schema_hashes = families.each_with_object({}) do |family, hashes|
  hashes[family["source"]] = family["source_sha256"] if family.is_a?(Hash) && family["source"] && family["source_sha256"]
end
expected_descriptor_hashes = families.each_with_object({}) do |family, hashes|
  hashes[family["descriptor"]] = family["descriptor_sha256"] if family.is_a?(Hash) && family["descriptor"] && family["descriptor_sha256"]
end

schema_files = {}
schema_names.each do |relative|
  source = schema_roots.map { |candidate| File.join(candidate, relative) }.find { |path| File.file?(path) }
  abort "schema input missing: #{relative}" unless source
  expected = expected_schema_hashes[relative]
  abort "schema hash mismatch: #{relative}" if expected && Digest::SHA256.file(source).hexdigest != expected
  schema_files[relative] = source
end

type_policy = nil
type_policy_metadata = nil
if manifest_path
  type_policy_source = schema_roots.map { |candidate| File.join(candidate, "type-policy.json") }.find { |path| File.file?(path) }
  abort "Rust-owned type policy missing from schema root" unless type_policy_source
  type_policy_bytes = File.binread(type_policy_source)
  type_policy = JSON.parse(type_policy_bytes)
  abort "unexpected Rust type policy schema" unless type_policy["schema"] == "acyclic.sdk.type-policy.v1"
  profile = type_policy["languages"].find { |entry| entry.is_a?(Hash) && entry["language"] == "ruby" }
  abort "Rust type policy has no Ruby profile" unless profile.is_a?(Hash) && profile["nominal_types"] && profile["refinements"] && profile["unions"]
  type_policy_destination = File.join(__dir__, "type-policy.json")
  File.binwrite(type_policy_destination, type_policy_bytes)
  type_policy_metadata = {
    "path" => "type-policy.json",
    "sha256" => Digest::SHA256.hexdigest(type_policy_bytes),
    "schema" => type_policy["schema"],
    "language" => "ruby",
    "profile" => profile
  }

  semantic_types = type_policy.fetch("semantic_types")
  abort "Rust type policy semantic_types must be non-empty" unless semantic_types.is_a?(Array) && !semantic_types.empty?
  generated_type_policy = [
    "# frozen_string_literal: true",
    "# Generated exclusively from rust/crates/sdk-contract-wire/src/type_policy.rs.",
    "module Acyclic",
    "  module TypePolicy",
    *semantic_types.map do |item|
      class_name = pascal_identifier(item.fetch("id"))
      [
        "    class #{class_name}",
        "      attr_reader :value",
        "      def self.from(value)",
        "        new(value)",
        "      end",
        "      private_class_method :new",
        "      def initialize(value)",
        *ruby_validation_lines(item).map { |line| "        #{line}" },
        "        @value = value",
        "        freeze",
        "      end",
        "    end",
      ].join("\n")
    end,
    "    FIELD_TYPES = {",
    *type_policy.fetch("field_mappings").map do |mapping|
      key = "[#{mapping.fetch("family").to_s.inspect}, #{mapping.fetch("field").to_s.inspect}]"
      "      #{key} => #{pascal_identifier(mapping.fetch("semantic_type"))},"
    end,
    "    }.freeze",
    "",
    "    module Wire",
    "      module_function",
    "",
    "      def coerce(family:, field:, value:)",
    "        type = FIELD_TYPES[[family.to_s, field.to_s]]",
    "        return value unless type",
    "        value.is_a?(type) ? value.value : type.from(value).value",
    "      end",
    "",
    "      def normalize_request(family:, request:)",
    "        return request unless request",
    "        if request.is_a?(Hash)",
    "          return request.each_with_object({}) do |(key, value), normalized|",
    "            field = key.to_s",
    "            normalized[key] = FIELD_TYPES.key?([family.to_s, field]) && !value.nil? ? coerce(family: family, field: field, value: value) : value",
    "          end",
    "        end",
    "        FIELD_TYPES.each_key do |mapped_family, field|",
    "          next unless mapped_family == family.to_s",
    "          reader = field.to_s",
    "          writer = \"\#{field}=\"",
    "          next unless request.respond_to?(reader) && request.respond_to?(writer)",
    "          current = request.public_send(reader)",
    "          request.public_send(writer, coerce(family: mapped_family, field: field, value: current)) unless current.nil?",
    "        end",
    "        request",
    "      end",
    "",
    "      def typed_field(family:, field:, value:)",
    "        type = FIELD_TYPES[[family.to_s, field.to_s]]",
    "        type ? (value.is_a?(type) ? value : type.from(value)) : value",
    "      end",
    "    end",
    "  end",
    "end",
    "",
  ].join("\n")
  FileUtils.mkdir_p(File.join(__dir__, "lib", "acyclic_sdk"))
  File.write(File.join(__dir__, "lib", "acyclic_sdk", "type_policy.rb"), generated_type_policy)
  rbs = ["module Acyclic", "  module TypePolicy"]
  rbi = ["# typed: strict", "module Acyclic", "  module TypePolicy"]
  semantic_types.each do |item|
    class_name = pascal_identifier(item.fetch("id"))
    value_type = ruby_value_type(item.fetch("wire_kind"))
    rbs += ["    class #{class_name}", "      attr_reader value: #{value_type}", "      def self.from: (#{value_type} value) -> #{class_name}", "    end"]
    rbi += ["    class #{class_name}", "      extend T::Sig", "      sig { returns(#{value_type}) }", "      def value; end", "      sig { params(value: #{value_type}).returns(#{class_name}) }", "      def self.from(value); end", "    end"]
  end
  rbs += [
    "    module Wire",
    "      def self.coerce: (family: String, field: String, value: untyped) -> untyped",
    "      def self.normalize_request: (family: String, request: untyped) -> untyped",
    "      def self.typed_field: (family: String, field: String, value: untyped) -> untyped",
    "    end",
    "  end", "end", ""
  ]
  rbi += [
    "    module Wire",
    "      extend T::Sig",
    "      sig { params(family: String, field: String, value: T.untyped).returns(T.untyped) }",
    "      def self.coerce(family:, field:, value:); end",
    "      sig { params(family: String, request: T.untyped).returns(T.untyped) }",
    "      def self.normalize_request(family:, request:); end",
    "      sig { params(family: String, field: String, value: T.untyped).returns(T.untyped) }",
    "      def self.typed_field(family:, field:, value:); end",
    "    end",
    "  end", "end", ""
  ]
  # OUT is rebuilt by protoc below; keep these values in memory and emit the
  # declarations again after protobuf generation has created the directory.
  negative_cases = semantic_types.filter_map do |item|
    rule_kinds = item.fetch("rules").map { |rule| rule.fetch("kind") }
    value = if rule_kinds.include?("non_empty") then "\"\""
            elsif rule_kinds.include?("fixed_length") then "\"x\""
            elsif rule_kinds.include?("strictly_positive") then "0"
            elsif rule_kinds.include?("non_negative") then "-1"
            elsif rule_kinds.include?("exact_oneof") then "{}"
            end
    value && [pascal_identifier(item.fetch("id")), value]
  end
  negative_test = ["# Generated contract checks from the Rust type policy.", "require \"minitest/autorun\"", "require_relative \"../lib/acyclic_sdk\"", "class RustTypePolicyNegativeTest < Minitest::Test"]
  negative_cases.each { |class_name, value| negative_test += ["  def test_rejects_invalid_#{class_name.downcase}", "    assert_raises(ArgumentError) { Acyclic::TypePolicy::#{class_name}.from(#{value}) }", "  end"] }
  negative_test += [
    "  def test_public_remote_client_serializes_rust_owned_value_objects",
    "    request_class = Struct.new(:path)",
    "    captured = nil",
    "    client = Acyclic::Remote::Client.new(family: \"stream\", invoker: ->(_operation, request, _transport) { captured = request; :ok }, installed: { grpc: false, http_json: true }, endpoint: { grpc: false, http_json: true })",
    "    assert_equal :ok, client.call(\"append\", request_class.new(Acyclic::TypePolicy::Path.from(\"events\")))",
    "    assert_equal \"events\", captured.path",
    "    assert_raises(ArgumentError) { client.call(\"append\", request_class.new(\"\")) }",
    "  end",
    "end",
    "",
  ]
  FileUtils.mkdir_p(File.join(__dir__, "test"))
  File.write(File.join(__dir__, "test", "type_policy_negative_test.rb"), negative_test.join("\n"))
  type_policy_metadata["artifacts"] = ["lib/acyclic_sdk/type_policy.rb", "generated/type_policy.rbs", "generated/type_policy.rbi", "test/type_policy_negative_test.rb"]
end
expected_descriptor_hashes.each do |relative, expected|
  descriptor = schema_roots.map { |root| File.join(root, relative) }.find { |path| File.file?(path) }
  abort "descriptor input missing: #{relative}" unless descriptor
  abort "descriptor hash mismatch: #{relative}" if Digest::SHA256.file(descriptor).hexdigest != expected
end

protoc = command!("grpc_tools_ruby_protoc")
FileUtils.rm_rf(OUT)
FileUtils.mkdir_p(OUT)

run!([
  protoc,
  *schema_roots.flat_map { |root| ["-I", root] },
  "--ruby_out=#{OUT}",
  "--grpc_out=#{OUT}",
  *schema_names
])

if type_policy
  File.write(File.join(OUT, "type_policy.rbs"), rbs.join("\n"))
  File.write(File.join(OUT, "type_policy.rbi"), rbi.join("\n"))
end

rust_family_goldens = nil
if manifest_path
  fixture_source = File.join(schema_roots.fetch(0), "rust-family-goldens.json")
  abort "Rust-owned fixture missing: #{fixture_source}" unless File.file?(fixture_source)

  fixture_bytes = File.binread(fixture_source)
  fixture = JSON.parse(fixture_bytes)
  abort "Rust-owned fixture must contain nine family goldens" unless fixture.is_a?(Array) && fixture.length == 9

  manifest_sha256 = Digest::SHA256.file(manifest_path).hexdigest
  unless fixture.all? { |entry| entry.is_a?(Hash) && entry["authority_manifest_sha256"] == manifest_sha256 }
    abort "Rust-owned fixture is bound to a different authority manifest"
  end

  fixture_destination = File.join(__dir__, "test", "fixtures", "rust-family-goldens.json")
  FileUtils.mkdir_p(File.dirname(fixture_destination))
  File.binwrite(fixture_destination, fixture_bytes)
  rust_family_goldens = {
    "path" => "test/fixtures/rust-family-goldens.json",
    "sha256" => Digest::SHA256.hexdigest(fixture_bytes)
  }
end

root_prefix = "#{ROOT.tr('\\', '/')}/"
lock_path = File.join(__dir__, "generator.lock.json")
runtime_triple = RbConfig::CONFIG.fetch("host", RUBY_PLATFORM)
provenance = {
  "generator" => lock,
  "source_revision" => ENV.fetch("GIT_COMMIT", "unknown"),
  "source_git_sha" => ENV.fetch("GIT_COMMIT", "unknown"),
  "rust_model_digest" => ENV.fetch("ACYCLIC_RUST_MODEL_DIGEST", "unknown"),
  "generator_lock_sha256" => Digest::SHA256.file(lock_path).hexdigest,
  "schema_inputs_sha256" => schema_files.transform_values { |path| Digest::SHA256.file(path).hexdigest },
  "schema_root" => schema_root ? File.expand_path(schema_root).tr('\\', '/') : "diagnostic repository proto roots",
  "authority_manifest" => manifest_path&.tr('\\', '/'),
  "authority_manifest_sha256" => manifest_path && File.file?(manifest_path) ? Digest::SHA256.file(manifest_path).hexdigest : nil,
  "authority_manifest_schema" => authority && authority["schema"],
  "authority_source_revision" => authority && authority["source_revision"],
  "authority_exporter" => authority && authority["exporter"],
  "platform" => {
    "execution_scope" => "portable",
    "target_triple" => "portable",
    "build_host_triple" => runtime_triple
  },
  "rust_family_goldens" => rust_family_goldens,
  "type_policy" => type_policy_metadata,
  "generated_files" => Dir[File.join(OUT, "**", "*.rb")].sort.map { |path| path.tr('\\', '/').delete_prefix(root_prefix) }
}
unless provenance["source_git_sha"].match?(/\A[0-9a-f]{40}\z/i)
  abort "Ruby provenance requires a 40-character Rust source Git SHA (GIT_COMMIT)"
end
unless provenance["rust_model_digest"].match?(/\A[0-9a-f]{64}\z/i)
  abort "Ruby provenance requires the 64-character Rust model digest (ACYCLIC_RUST_MODEL_DIGEST)"
end
if authority && authority["source_revision"] && authority["source_revision"] != provenance["rust_model_digest"]
  abort "Ruby provenance model digest does not match the Rust authority manifest"
end
File.write(File.join(OUT, "provenance.json"), JSON.pretty_generate(provenance) + "\n")
