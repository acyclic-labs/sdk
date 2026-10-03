# frozen_string_literal: true

generated = File.expand_path("../generated", __dir__)
$LOAD_PATH.unshift(generated) unless $LOAD_PATH.include?(generated)
require_relative "acyclic_sdk/version"
require_relative "acyclic_sdk/generated_remote_policy"
require_relative "acyclic_sdk/remote_policy"

# Generated transport files are loaded after `ruby generate.rb`. Keeping the
# entrypoint this small prevents shared protocol behavior from being authored
# in Ruby.
Dir[File.join(generated, "**", "*_pb.rb")].sort.each { |path| require path }
Dir[File.join(generated, "**", "*_services_pb.rb")].sort.each { |path| require path }
