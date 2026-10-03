# frozen_string_literal: true

require_relative "acyclic_sdk/version"
require_relative "acyclic_sdk/remote_policy"

# Generated transport files are loaded after `ruby generate.rb`. Keeping the
# entrypoint this small prevents shared protocol behavior from being authored
# in Ruby.
generated = File.expand_path("../generated", __dir__)
$LOAD_PATH.unshift(generated) unless $LOAD_PATH.include?(generated)
Dir[File.join(generated, "**", "*_pb.rb")].sort.each { |path| require path }
Dir[File.join(generated, "**", "*_services_pb.rb")].sort.each { |path| require path }
