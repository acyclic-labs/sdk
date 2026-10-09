gem 'acyclic-sdk-transport', '= 0.2.0.alpha.1'
require 'actors/v1/actors_pb'
begin
  Acyclic::Actors::V1::CreateActorRequest.new(code_sha256: 123)
  raise 'invalid assignment accepted'
rescue TypeError => error
  raise 'unrelated type failure' unless error.message.include?('code_sha256')
  puts "PASS: intended code_sha256 TypeError: #{error.message}"
end
