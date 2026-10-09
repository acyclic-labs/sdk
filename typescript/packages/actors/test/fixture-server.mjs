// Fixtures own their servers and connections, including persistent HTTP/2
// channels opened by native clients that expose no explicit close operation.
export function fixtureServer(server) {
  const sockets = new Set();
  const sessions = new Set();
  server.on("connection", socket => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  server.on("session", session => {
    sessions.add(session);
    session.once("close", () => sessions.delete(session));
  });
  return async () => {
    const closed = new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    for (const session of sessions) session.destroy();
    for (const socket of sockets) socket.destroy();
    await closed;
  };
}
