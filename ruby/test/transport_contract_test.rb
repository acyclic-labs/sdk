# frozen_string_literal: true

require "minitest/autorun"
require "acyclic_sdk"

class TransportContractTest < Minitest::Test
  def test_uint64_and_bytes_round_trip
    limits = Acyclic::Actors::V1::ActorLimits.new(memory_bytes: 18_446_744_073_709_551_615)
    assert_equal 18_446_744_073_709_551_615, limits.memory_bytes

    observation = Acyclic::Actors::V1::ActorObservation.new(code_sha256: "\x00\xff".b)
    assert_equal "\x00\xff".b, observation.code_sha256
  end

  def test_optional_presence_and_oneof_are_retained
    request = Acyclic::Stream::V2::AppendRequest.new
    refute request.has_if_tail?
    request.if_tail = 9_007_199_254_740_992
    assert request.has_if_tail?
    assert_equal 9_007_199_254_740_992, request.if_tail

    response = Acyclic::Stream::V2::AppendResponse.new
    response.committed = Acyclic::Stream::V2::AppendReceipt.new(tail: 3)
    assert_equal :committed, response.outcome
  end

  def test_streaming_methods_are_generated
    service = Acyclic::Stream::V2::StreamService::Service
    assert_respond_to service.new, :read
    assert_respond_to service.new, :follow
    assert_respond_to service.new, :children
  end
end
