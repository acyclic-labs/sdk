import { taskFixture } from "./browser-task-fixture.mjs";
let runtime;
self.onmessage = async ({ data }) => {
  try {
    if (data.kind === "open") {
      const fixture = await taskFixture(data.session);
      runtime = await fixture.open();
      self.postMessage({ id: data.id, kind: "ready" });
    } else {
      const tick = await runtime.workerTick({ id: "web-worker", available: {}, labels: {} }, null, 128, 4);
      self.postMessage({ id: data.id, tick });
    }
  } catch (error) { self.postMessage({ id: data.id, error: String(error.message ?? error) }); }
};
