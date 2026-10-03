import { expect, test } from "bun:test";
import {
  ACTORS_METHODS,
  ACTORS_REMOTE_POLICY,
  ACTORS_SOURCE,
  fromEnv as actorsFromEnv,
} from "../../../typescript/packages/actors/src/index.js";
import {
  WORKERS_METHODS,
  WORKERS_REMOTE_POLICY,
  WORKERS_SOURCE,
  fromEnv as workersFromEnv,
} from "../../../typescript/packages/workers/src/index.js";
import {
  OBJECTS_METHODS,
  OBJECTS_REMOTE_POLICY,
  OBJECTS_SOURCE,
  fromEnv as objectsFromEnv,
} from "../../../typescript/packages/objects/src/index.js";

test("Actors, Workers, and Objects factories default without a transport flag", async () => {
  const [actors, workers, objects] = await Promise.all([
    actorsFromEnv({ endpoint: "https://actors.example", token: "fixture" }),
    workersFromEnv({ endpoint: "https://workers.example", token: "fixture" }),
    objectsFromEnv({ endpoint: "https://objects.example", token: "fixture" }),
  ]);
  expect(actors).toBeDefined();
  expect(workers).toBeDefined();
  expect(objects).toBeDefined();
  expect(ACTORS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(WORKERS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(OBJECTS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
});

test("generated family metadata covers every modeled RPC and preserves required request policy", () => {
  const families = [
    [ACTORS_SOURCE, ACTORS_METHODS],
    [WORKERS_SOURCE, WORKERS_METHODS],
    [OBJECTS_SOURCE, OBJECTS_METHODS],
  ] as const;
  for (const [source, methods] of families) {
    expect(source.httpProjection).toBe(true);
    expect(Object.keys(methods)).toHaveLength(source.modeledOperations);
    for (const method of Object.values(methods)) {
      expect(method.httpMethod).toBe("POST");
      expect(method.requestEncoding).toBe("protobuf-json");
      expect(method.responseEncoding).toBe("protobuf-json");
      expect(method.auth).toBe("bearer");
      expect(method.credentialPolicy).toBe("bearer-no-crlf");
      expect(method.responseLimitPolicy).toBe("bounded-cumulative-utf8");
    }
  }
});

test("factory configuration still requires an explicit endpoint while transport remains optional", async () => {
  const [actorsSource, workersSource, objectsSource] = await Promise.all([
    import("../../../typescript/packages/actors/src/client.js"),
    import("../../../typescript/packages/workers/src/client.js"),
    import("../../../typescript/packages/objects/src/v2-client.js"),
  ]);
  expect(actorsSource.fromEnv.toString()).toContain("environment.endpoint");
  expect(workersSource.fromEnv.toString()).toContain("environment.endpoint");
  expect(objectsSource.fromEnv.toString()).toContain("environment.endpoint");
  expect(actorsSource.fromEnv.toString()).toContain("environment.transport");
  expect(workersSource.fromEnv.toString()).toContain("environment.transport");
  expect(objectsSource.fromEnv.toString()).toContain("environment.transport");
  expect(actorsSource.fromEnv.toString()).toContain("options[0]");
  expect(workersSource.fromEnv.toString()).toContain("options[0]");
  expect(objectsSource.fromEnv.toString()).toContain("options[0]");
});
