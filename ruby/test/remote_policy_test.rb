# frozen_string_literal: true

require "minitest/autorun"
require "acyclic_sdk"

class RemotePolicyTest < Minitest::Test
  def test_native_defaults_to_grpc_and_invokes_once
    calls = []
    client = Acyclic::Remote::Client.new(family: "stream", invoker: lambda { |operation, request, transport|
      calls << [operation, request, transport]
      :ok
    })

    assert_equal :grpc, client.transport
    assert_equal :native, client.runtime
    assert_equal :ok, client.call("append", { path: "events" })
    assert_equal [["append", { path: "events" }, :grpc]], calls
  end

  def test_auto_runtime_resolver_can_select_embedded_browser_policy
    assert_equal :native, Acyclic::Remote::Policy.resolve_runtime
    assert_equal :browser, Acyclic::Remote::Policy.resolve_runtime(:browser)
    assert_equal :http_json, Acyclic::Remote::Policy.select(
      family: "stream", runtime: :browser, streaming: true
    )
  end

  def test_streaming_http_override_is_supported_but_unary_only_override_is_rejected
    client = Acyclic::Remote::Client.new(family: "stream", streaming: true, transport: :http_json, invoker: ->(*) { nil })
    assert_equal :http_json, client.transport

    assert_raises(ArgumentError) do
      Acyclic::Remote::Client.new(family: "actors", streaming: true, transport: :http_json, invoker: ->(*) { nil })
    end
  end

  def test_bearer_policy_rejects_empty_and_header_injection
    assert_equal " token ", Acyclic::Remote::Policy.validate_bearer(" token ")
    assert_raises(ArgumentError) { Acyclic::Remote::Policy.validate_bearer("  ") }
    assert_raises(ArgumentError) { Acyclic::Remote::Policy.validate_bearer("token\r\nInjected: yes") }
  end
end
