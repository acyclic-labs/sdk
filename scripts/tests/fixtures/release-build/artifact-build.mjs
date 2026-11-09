import { writeFileSync } from "node:fs";

export async function artifactBuild(options) {
  writeFileSync(options.env.RELEASE_TEST_INVOCATION, JSON.stringify(options));
  if (options.env.RELEASE_TEST_RESULT === "throw") throw new Error("artifact fixture failed");
  return Number(options.env.RELEASE_TEST_RESULT);
}
