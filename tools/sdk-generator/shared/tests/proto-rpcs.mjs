import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { compatibilityArtifacts } from "../../../../scripts/generated-bindings.mjs";

export function canonicalRPCs() {
  return ["actors", "stream"].flatMap(family => {
    const source = readFileSync(new URL("../../../../" + compatibilityArtifacts[family].schemaDigest, import.meta.url), "utf8");
    const packageName = source.match(/^package\s+([\w.]+);/m)?.[1];
    assert.ok(packageName, `missing ${family} package`);
    const services = [...source.matchAll(/^service\s+(\w+)\s*\{([\s\S]*?)^\}/gm)];
    assert.equal(services.length, 1, `${family} must name one maintained service`);
    const [, service, body] = services[0];
    const methods = [...body.matchAll(/^\s*rpc\s+(\w+)\s*\(\s*(stream\s+)?[\w.]+\s*\)\s*returns\s*\(\s*(stream\s+)?[\w.]+\s*\)/gm)];
    assert.ok(methods.length, `${family} must expose RPCs`);
    return methods.map(([, name, clientStreaming, serverStreaming]) => ({
      name, path: `${packageName}.${service}/${name}`,
      clientStreaming: Boolean(clientStreaming), serverStreaming: Boolean(serverStreaming),
    }));
  });
}