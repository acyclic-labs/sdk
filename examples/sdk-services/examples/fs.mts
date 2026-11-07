import { createFilesystemGrpcClient } from "@acyclic-labs/fs/grpc";

const client = createFilesystemGrpcClient({
  endpoint: process.env.FS_ENDPOINT!, token: process.env.ACYCLIC_API_KEY!,
});
try {
  console.log(await client.filesystem.openWorkspace({
    selector: { case: "name", value: process.env.FS_WORKSPACE! },
  }));
} finally {
  client.close();
}
