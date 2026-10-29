import { expect, test } from "bun:test";
import { validateDeploymentBinding } from "../src/client.js";

test("the public Wasm validator preserves admission selectors and refuses mixed authority", async () => {
  for (const guestAdmission of ["platformAuthority", "hostAccess"]) {
    const binding = {
      name: guestAdmission, capability: "", resource: "",
      targetAccount: new Uint8Array(), stateTransaction: false, guestAdmission,
    };
    await validateDeploymentBinding(binding);
    for (const forged of [
      { ...binding, capability: "forged" },
      { ...binding, resource: "identity:/harness" },
      { ...binding, guestAdmission: "unknown" },
      { ...binding, stateTransaction: true },
    ]) await expect(validateDeploymentBinding(forged)).rejects.toBeDefined();
  }
  await validateDeploymentBinding({
    name: "originalState", capability: "", resource: "",
    targetAccount: new Uint8Array(), stateTransaction: true,
  });
});
