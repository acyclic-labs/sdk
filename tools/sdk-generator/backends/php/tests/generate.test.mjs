import { generate } from "../src/generate.mjs";
import { registerGenerationTests } from "../../../shared/tests/generation.mjs";

registerGenerationTests("PHP", generate, {
  target: "php", pluginArgument: "php-plugin", pluginKey: "php_plugin",
  outputFlag: "--php_out=", extension: ".php",
  metadata: ["composer.json", "LICENSE"], pluginMismatch: /admitted host pin/,
});
