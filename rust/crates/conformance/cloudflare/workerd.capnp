using Workerd = import "/workerd/workerd.capnp";

# Uses the pinned runtime's public schema, with one loopback listener. No Node
# proxy, inspector or randomly assigned auxiliary listener is needed.
const config :Workerd.Config = (
  services = [
    (name = "harness", worker = (
      modules = [
        (name = "worker.mjs", esModule = embed "worker.mjs"),
        (name = "generated/acyclic_cloudflare_conformance_bg.wasm", wasm = embed "generated/acyclic_cloudflare_conformance_bg.wasm"),
        (name = "generated/agent_guest_bg.wasm", wasm = embed "generated/agent_guest_bg.wasm")
      ],
      compatibilityDate = "2026-07-30",
      bindings = [
        (name = "SUITE", durableObjectNamespace = "Suite"),
        (name = "LEDGER", durableObjectNamespace = "Ledger"),
        (name = "AGENT", durableObjectNamespace = "Agent")
      ],
      durableObjectNamespaces = [
        (className = "Suite", uniqueKey = "acyclic-conformance-suite", enableSql = true),
        (className = "Ledger", uniqueKey = "acyclic-conformance-ledger", enableSql = true),
        (className = "Agent", uniqueKey = "acyclic-conformance-agent", enableSql = true)
      ],
      durableObjectStorage = (localDisk = "data")
    )),
    (name = "data", disk = (writable = true))
  ],
  sockets = [(name = "http", address = "127.0.0.1:15420", http = (), service = "harness")]
);
