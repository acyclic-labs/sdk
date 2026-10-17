import { generate } from "../src/generate.mjs";
import { registerGenerationTests } from "../../../shared/tests/generation.mjs";

registerGenerationTests("Dart", generate, {
  target: "dart", pluginArgument: "dart-plugin", pluginKey: "dart_plugin",
  outputFlag: "--dart_out=grpc:", extension: ".dart",
  metadata: ["pubspec.yaml", "LICENSE"], pluginMismatch: /admitted host pin/,
});
