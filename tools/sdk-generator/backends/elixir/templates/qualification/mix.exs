defmodule InstalledConsumer.MixProject do
  use Mix.Project
  def project do
    [app: :installed_consumer, version: "0.1.0", deps: [{:acyclic_sdk_transport, path: "../sdk"},
     {:protobuf, "== 0.17.1"}, {:grpc_core, "== 1.0.5"}, {:grpc, "== 1.0.5"}]]
  end
  def application, do: [extra_applications: [:logger]]
end
