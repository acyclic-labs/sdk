/** Own accepted connections so fixture shutdown never depends on client GC.
 * @param {import("node:net").Server} server
 * @returns {() => Promise<void>}
 */
export function ownFixtureServer(server) {
  const sockets = new Set();
  let closing = false;
  server.on("connection", socket => {
    if (closing) socket.destroy();
    else {
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
    }
  });
  return () => new Promise((resolve, reject) => {
    closing = true;
    server.close(error => error && !("code" in error && error.code === "ERR_SERVER_NOT_RUNNING") ? reject(error) : resolve());
    for (const socket of sockets) socket.destroy();
  });
}