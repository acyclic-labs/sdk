# frozen_string_literal: true
require "acyclic_sdk"

endpoint = ENV.fetch("FIXTURE_GRPC_ADDRESS")
path = ENV.fetch("RECOVERY_PATH")
id = ENV.fetch("RECOVERY_ID")
client = Acyclic::Stream::V2::StreamService::Stub.new(endpoint, :this_channel_is_insecure)
append = Acyclic::Stream::V2::AppendRequest.new(
  path: path,
  records: ["ruby-recovery-0", "ruby-recovery-1"],
  if_tail: 0,
  idempotency_key: "ruby-recovery-append-#{id}"
)
appended = client.append(append)
raise "append tail mismatch" unless appended.committed.end == 2
items = client.read(Acyclic::Stream::V2::ReadRequest.new(path: path, from: 0, limit: 2)).map do |item|
  [item.record.sequence, item.record.value]
end
raise "page mismatch #{items.inspect}" unless items == [[0, "ruby-recovery-0"], [1, "ruby-recovery-1"]]
resumed = client.read(Acyclic::Stream::V2::ReadRequest.new(path: path, from: 1, limit: 1)).map do |item|
  [item.record.sequence, item.record.value]
end
raise "resume mismatch #{resumed.inspect}" unless resumed == [[1, "ruby-recovery-1"]]
operation = client.follow(Acyclic::Stream::V2::FollowRequest.new(path: path, from: 0), return_op: true)
responses = operation.execute
first = responses.next
raise "cancel first mismatch" unless first.record.sequence == 0
operation.cancel
cancel_error = nil
begin
  responses.each { |_item| }
rescue GRPC::Cancelled => error
  cancel_error = error
end
raise "missing cancelled status" unless operation.cancelled? && cancel_error
puts "append_end=#{appended.committed.end}"
puts "page=#{items.map(&:first).join(",")}"
puts "resume=#{resumed.map(&:first).join(",")}"
puts "cancel_first=#{first.record.sequence}"
puts "cancel_status=#{operation.status.code}:#{operation.status.details}"
