import { generate } from "../src/generate.mjs";
import { registerGenerationTests } from "../../../shared/tests/generation.mjs";

registerGenerationTests("Ruby", generate, {
  target: "ruby", pluginArgument: "grpc-ruby", pluginKey: "grpc_ruby",
  outputFlag: "--ruby_out=", extension: ".rb",
  metadata: ["acyclic-sdk-transport.gemspec", "LICENSE"], pluginMismatch: /published host pin/,
});
