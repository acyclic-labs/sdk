import { generate } from "../src/generate.mjs";
import { registerGenerationTests } from "../../../shared/tests/generation.mjs";

registerGenerationTests("C#", generate, {
  target: "dotnet", pluginArgument: "grpc-csharp", pluginKey: "grpc_csharp",
  outputFlag: "--csharp_out=", extension: ".cs",
  metadata: ["Acyclic.Sdk.Transport.csproj", "LICENSE"], pluginMismatch: /published package host pin/,
});
