gem 'acyclic-sdk-transport', '= 0.2.0.alpha.1'
require 'stream/v1/stream_pb'
begin
  Acyclic::Stream::V1::AppendRequest.new(if_tail: "wrong")
  raise 'invalid assignment accepted'
rescue TypeError => error
  raise 'unrelated type failure' unless error.message.include?('if_tail')
  puts "PASS: intended if_tail TypeError: #{error.message}"
end
