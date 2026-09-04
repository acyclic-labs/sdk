import {
  DEFAULT_OBJECT_CACHE_OPTIONS,
  openMemoryFs,
} from "../dist/memory-node.js";
import { exerciseWorkspace } from "./workspace-composition.mjs";

const engine = await openMemoryFs({
  maximumObjectBytes: 1024 * 1024,
  maximumMemoryBytes: 64 * 1024 * 1024,
  objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
});
try {
  await exerciseWorkspace(engine);
} finally {
  engine.close();
}
