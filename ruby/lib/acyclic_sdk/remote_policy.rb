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
          installed: normalize_availability(installed || installed_availability(runtime)),
          endpoint: normalize_availability(endpoint || endpoint_availability),
          override: override,
        ).to_sym
      end

      def installed_availability(runtime = :native)
        return { "grpc" => false, "grpc_web" => false, "http_json" => true } if runtime.to_sym == :browser

        grpc = defined?(GRPC) || (defined?(Gem) && Gem::Specification.find_all_by_name("grpc").any?)
        { "grpc" => grpc, "grpc_web" => false, "http_json" => true }
      rescue Gem::LoadError
        { "grpc" => false, "grpc_web" => false, "http_json" => true }
      end

      def endpoint_availability(endpoint = nil)
        return normalize_endpoint(endpoint) if endpoint

        parse_transport_list(ENV["ACYCLIC_ENDPOINT_TRANSPORTS"])
      end

      def validate_bearer(token)
        GeneratedPolicy.validate_bearer(token)
      end

      def normalize_availability(value)
        return nil unless value

        value = normalize_endpoint(value) if value.is_a?(String)
        value.each_with_object({}) { |(kind, available), result| result[kind.to_s] = available }
      end

      def normalize_endpoint(value)
        return value unless value.is_a?(String)

        scheme = value.split(":", 2).first.downcase
        return { "grpc" => true, "grpc_web" => false, "http_json" => false } if scheme == "grpc"
        return { "grpc" => false, "grpc_web" => true, "http_json" => false } if scheme == "grpc-web"
        return { "grpc" => false, "grpc_web" => false, "http_json" => true } if %w[http https].include?(scheme)

        parse_transport_list(ENV["ACYCLIC_ENDPOINT_TRANSPORTS"])
      end

      def parse_transport_list(value)
        return { "grpc" => true, "grpc_web" => true, "http_json" => true } if value.nil? || value.strip.empty?

        kinds = value.split(",").map { |kind| kind.strip.downcase }.to_h { |kind| [kind, true] }
        { "grpc" => kinds["grpc"] == true, "grpc_web" => kinds["grpc_web"] == true, "http_json" => kinds["http_json"] == true }
      end
      private_class_method :normalize_availability
      private_class_method :parse_transport_list
    end

    class Client
      attr_reader :family, :runtime, :transport

      def initialize(family:, invoker:, runtime: :auto, streaming: false, transport: nil,
                     bearer: nil, bearer_auth: true, installed: nil, endpoint: nil)
        @family = family.to_s
        @runtime = Policy.resolve_runtime(runtime)
        @transport = Policy.select(
          family: @family,
          runtime: @runtime,
          streaming: streaming,
          bearer_auth: bearer_auth,
          installed: installed || Policy.installed_availability(@runtime),
          endpoint: endpoint || Policy.endpoint_availability,
          override: transport,
        )
        Policy.validate_bearer(bearer) if bearer
        @invoker = invoker
      end

      def call(operation, request)
        @invoker.call(operation, request, @transport)
      end
    end
  end
end
