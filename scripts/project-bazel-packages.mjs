import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { projectBindings } from "./project-bindings.mjs";

const packages = new Set(JSON.parse(process.argv[2]));
const paths = JSON.parse(readFileSync(process.argv[3], "utf8"));
const tools = new Map(Object.entries(paths).map(([label, path]) => [label, resolve(process.env.SDK_EXEC_ROOT, path)]));
await projectBindings(packages, { tools, families: [...packages], sourceCommit: "bazel", protobuf: false });
