defmodule AcyclicSdkTransport.MixProject do
  use Mix.Project
  def project do
    [app: :acyclic_sdk_transport, version: "0.2.0", elixir: "~> 1.16",
     deps: [{:protobuf, "== 0.17.1"}, {:grpc_core, "== 1.0.5"}, {:grpc, "== 1.0.5"}]]
  end
  def application, do: [extra_applications: [:logger]]
end
