# frozen_string_literal: true

# PROTOTYPE ONLY: handwritten adapter pending Rust generator emission.

# PROTOTYPE ONLY: this handwritten adapter is not generated output. The Rust
# generator must emit the policy metadata and selection contract before this
# can be promoted to a qualified package facade.

module Acyclic
  # Rust-owned transport selection for the thin remote facade. The injected
  # invoker owns wire encoding; this layer only enforces the shared selection
  # and credential policy before a call is made.
  module Remote
    module Policy
      OPTIONS = {
        native: {
          "actors" => [[:grpc, false], [:http_json, false]],
          "workers" => [[:grpc, false], [:http_json, false]],
          "objects" => [[:grpc, true], [:http_json, true]],
          "stream" => [[:grpc, true], [:http_json, true]],
          "inference" => [[:grpc, true]],
          "machines" => [[:grpc, true]],
          "filesystem" => [[:grpc, true]],
          "harness" => [[:grpc, true]]
        },
        browser: {
          "actors" => [[:http_json, false]],
          "workers" => [[:http_json, false]],
          "objects" => [[:http_json, true]],
          "stream" => [[:http_json, true]],
          "inference" => [[:http_json, true]],
          "machines" => [],
          "filesystem" => [[:grpc_web, true]],
          "harness" => []
        }
      }.freeze

      module_function

      def select(family:, runtime: :native, streaming: false, override: nil)
        options = OPTIONS.fetch(runtime.to_sym) { raise ArgumentError, "unknown client runtime: #{runtime}" }.fetch(family.to_s) { raise ArgumentError, "unknown family: #{family}" }
        candidates = options.select { |_kind, supports_streaming| !streaming || supports_streaming }
        if override
          selected = candidates.find { |kind, _supports_streaming| kind == override.to_sym }
          raise ArgumentError, "unsupported transport override #{override} for #{family}/#{runtime}" unless selected
          return selected.first
        end
        raise ArgumentError, "no compatible transport for #{family}/#{runtime}" if candidates.empty?

        candidates.first.first
      end

      def validate_bearer(token)
        value = String(token)
        raise ArgumentError, "invalid bearer credential" if value.strip.empty? || value.match?(/[\r\n]/)

        value
      end
    end

    class Client
      attr_reader :family, :runtime, :transport

      def initialize(family:, invoker:, runtime: :native, streaming: false, transport: nil, bearer: nil)
        @family = family.to_s
        @runtime = runtime.to_sym
        @transport = Policy.select(family: @family, runtime: @runtime, streaming: streaming, override: transport)
        Policy.validate_bearer(bearer) if bearer
        @invoker = invoker
      end

      def call(operation, request)
        @invoker.call(operation, request, @transport)
      end
    end
  end
end
