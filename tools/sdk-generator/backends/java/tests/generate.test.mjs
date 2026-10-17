import { generate } from "../src/generate.mjs";
import { registerGenerationTests } from "../../../shared/tests/generation.mjs";

registerGenerationTests("Java", generate, {
  target: "java", pluginArgument: "grpc-java", pluginKey: "grpc_java",
  outputFlag: "--java_out=", extension: ".java",
  metadata: ["pom.xml", "src/main/resources/META-INF/LICENSE"], pluginMismatch: /published host pin/,
});
