# frozen_string_literal: true

require_relative "generated_remote_policy"

module Acyclic
  # Rust-owned transport selection for the thin remote facade. The injected
  # invoker owns wire encoding; this layer only enforces the shared selection
  # and credential policy before a call is made.
  module Remote
    module Policy
      SOURCE_BINDING = GeneratedPolicy::SOURCE_BINDING
      OPTIONS = GeneratedPolicy::OPTIONS

      module_function

      def resolve_runtime(runtime = :auto)
        value = runtime.to_sym
        return value unless value == :auto

        # ruby.wasm is the embedded browser runtime; regular Ruby processes
        # use the native Rust-qualified transport set.
        defined?(RUBY_ENGINE) && RUBY_ENGINE == "ruby.wasm" ? :browser : :native
      end

      def select(family:, runtime: :auto, streaming: false, bearer_auth: true,
                 installed: nil, endpoint: nil, override: nil)
        runtime = resolve_runtime(runtime)
        GeneratedPolicy.select(
          family: family,
          runtime: runtime,
          streaming: streaming,
          bearer_auth: bearer_auth,
          installed: normalize_availability(installed),
          endpoint: normalize_availability(endpoint),
          override: override,
        ).to_sym
      end

      def validate_bearer(token)
        GeneratedPolicy.validate_bearer(token)
      end

      def normalize_availability(value)
        value&.each_with_object({}) { |(kind, available), result| result[kind.to_s] = available }
      end
      private_class_method :normalize_availability
    end

    class Client
      attr_reader :family, :runtime, :transport

      def initialize(family:, invoker:, runtime: :auto, streaming: false, transport: nil, bearer: nil)
        @family = family.to_s
        @runtime = Policy.resolve_runtime(runtime)
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
