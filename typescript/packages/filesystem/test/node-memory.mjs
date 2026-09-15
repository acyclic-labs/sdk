import {
  DEFAULT_OBJECT_CACHE_OPTIONS,
  openMemoryFs,
  portableVolumeOptions,
} from "@acyclic-labs/fs/memory";
import { exerciseWorkspace } from "./workspace-composition.mjs";

const engine = await openMemoryFs({
  maximumObjectBytes: 1024 * 1024,
  maximumMemoryBytes: 64 * 1024 * 1024,
  objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
});
try {
  await exerciseWorkspace(engine);
  const volume = await engine.createVolume(portableVolumeOptions("ephemeral"));
  const checkout = await volume.checkout({
    access: "read-write",
    consistency: "pinned",
    mutationMode: "private-cow",
  });
  await checkout.createDirectory("/typed");
  const created = await checkout.createFile("/typed/data", new TextEncoder().encode("head"));
  await checkout.writeFile("/typed/data", 4n, new TextEncoder().encode("-tail"));
  const lookup = await checkout.lookupNoFollow("/typed/data");
  if (lookup.fileId?.byteLength !== 16) throw new Error(`public volume checkout lost file identity: create=${created.fileId?.byteLength}, exists=${lookup.exists}, resolved=${lookup.resolvedComponents}`);
  const checkpoint = await checkout.checkpoint();
  if (checkpoint.generationId.byteLength !== 32) {
    throw new Error("public volume checkout returned a malformed generation identity");
  }
  const read = await checkout.readFileRange("/typed/data", 0n, 9n);
  if (new TextDecoder().decode(read.bytes) !== "head-tail") {
    throw new Error("public volume checkout did not preserve authored mutations");
  }
  const manifest = await checkout.exportManifest();
  const batch = await engine.exportGenerationBatch(manifest, 0n, 32, 1024n * 1024n);
  if (batch.objects.length === 0 || manifest.objects.length === 0) {
    throw new Error("public volume export omitted canonical objects");
  }
} finally {
  engine.close();
}
