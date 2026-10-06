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

    expected = Acyclic::Remote::Policy.installed_availability["grpc"] ? :grpc : :http_json
    assert_equal expected, client.transport
    assert_equal :native, client.runtime
    assert_equal :ok, client.call("append", { path: "events" })
    assert_equal [["append", { path: "events" }, expected]], calls
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

    https_client = Acyclic::Remote::Client.new(family: "actors", endpoint: "https://api.example", bearer: " token ", invoker: ->(*) { nil })
    assert_equal :http_json, https_client.transport

    assert_raises(ArgumentError) do
      Acyclic::Remote::Client.new(family: "actors", streaming: true, transport: :http_json, invoker: ->(*) { nil })
    end
  end

  def test_installed_and_endpoint_capabilities_are_forwarded_to_generated_policy
    client = Acyclic::Remote::Client.new(
      family: "actors",
      installed: { grpc: false, http_json: true },
      endpoint: { grpc: false, http_json: true },
      invoker: ->(*) { nil },
    )
    assert_equal :http_json, client.transport

    assert_raises(ArgumentError) do
      Acyclic::Remote::Client.new(
        family: "actors",
        endpoint: { grpc: false, http_json: false },
        invoker: ->(*) { nil },
      )
    end
  end

  def test_endpoint_metadata_is_consumed_without_capability_maps
    previous = ENV["ACYCLIC_ENDPOINT_TRANSPORTS"]
    ENV["ACYCLIC_ENDPOINT_TRANSPORTS"] = "http_json"
    client = Acyclic::Remote::Client.new(family: "actors", invoker: ->(*) { nil })
    assert_equal :http_json, client.transport
  ensure
    previous.nil? ? ENV.delete("ACYCLIC_ENDPOINT_TRANSPORTS") : ENV["ACYCLIC_ENDPOINT_TRANSPORTS"] = previous
  end

  def test_bearer_policy_rejects_empty_and_header_injection
    assert_equal " token ", Acyclic::Remote::Policy.validate_bearer(" token ")
    assert_raises(ArgumentError) { Acyclic::Remote::Policy.validate_bearer("  ") }
    assert_raises(ArgumentError) { Acyclic::Remote::Policy.validate_bearer("token\r\nInjected: yes") }
  end

  def test_public_client_round_trips_unknown_enum_and_oneof_wire_values
    enum_wire = "\x0A\x02\x20\x63".b
    enum_response = Acyclic::Actors::V1::InspectActorResponse.decode(enum_wire)
    enum_client = Acyclic::Remote::Client.new(
      family: "actors",
      installed: { grpc: false, http_json: true },
      endpoint: { grpc: false, http_json: true },
      invoker: ->(_operation, _request, _transport) { enum_response },
    )
    returned_enum = enum_client.call("InspectActor", { actor_id: "actor-1" })
    assert_equal enum_wire, Acyclic::Actors::V1::InspectActorResponse.encode(returned_enum)

    oneof_wire = "\x0A\x00\x9A\x06\x01\x7F".b
    oneof_response = Acyclic::Stream::V2::AppendResponse.decode(oneof_wire)
    oneof_client = Acyclic::Remote::Client.new(
      family: "stream",
      installed: { grpc: false, http_json: true },
      endpoint: { grpc: false, http_json: true },
      invoker: ->(_operation, _request, _transport) { oneof_response },
    )
    returned_oneof = oneof_client.call("Append", { path: "events" })
    assert_equal oneof_wire, Acyclic::Stream::V2::AppendResponse.encode(returned_oneof)
  end
end
