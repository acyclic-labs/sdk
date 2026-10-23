import * as binding from "@acyclic-labs/workers/addressing";
import { resourceIdentityConformance } from "../../../../conformance/addressing/shared-vectors.js";
await resourceIdentityConformance(binding, "@acyclic-labs/workers/addressing");
