# frozen_string_literal: true

require "fileutils"
require "json"
require "digest"

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
schema_names = (schema_names + dependency_names).uniq
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

root_prefix = "#{ROOT.tr('\\', '/')}/"
lock_path = File.join(__dir__, "generator.lock.json")
provenance = {
  "generator" => lock,
  "source_revision" => ENV.fetch("GIT_COMMIT", "unknown"),
  "generator_lock_sha256" => Digest::SHA256.file(lock_path).hexdigest,
  "schema_inputs_sha256" => schema_files.transform_values { |path| Digest::SHA256.file(path).hexdigest },
  "schema_root" => schema_root ? File.expand_path(schema_root).tr('\\', '/') : "diagnostic repository proto roots",
  "authority_manifest" => manifest_path&.tr('\\', '/'),
  "authority_manifest_sha256" => manifest_path && File.file?(manifest_path) ? Digest::SHA256.file(manifest_path).hexdigest : nil,
  "authority_manifest_schema" => authority && authority["schema"],
  "authority_source_revision" => authority && authority["source_revision"],
  "authority_exporter" => authority && authority["exporter"],
  "generated_files" => Dir[File.join(OUT, "**", "*.rb")].sort.map { |path| path.tr('\\', '/').delete_prefix(root_prefix) }
}
File.write(File.join(OUT, "provenance.json"), JSON.pretty_generate(provenance) + "\n")
