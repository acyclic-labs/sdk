gem 'acyclic-sdk-transport', '= 0.2.0.alpha.1'
require 'workers/v1/workers_pb'
begin
  Acyclic::Workers::V1::PublishVersionRequest.new(javascript_module: 123)
  raise 'invalid assignment accepted'
rescue TypeError => error
  raise 'unrelated type failure' unless error.message.include?('javascript_module')
  puts "PASS: intended javascript_module TypeError: #{error.message}"
end
