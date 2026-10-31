import { createServer } from "node:net";

// Admit the native endpoint before launching its selected executable. Framing,
// report outcomes and process retirement belong to the existing caller.
export async function listenReportPipe(generation) {
  if (process.platform !== "win32" || !/^[0-9a-f]{32}$/.test(generation)) {
    throw new Error("step=report-pipe-admission; recovery=use a Windows selected launch with its original 128-bit generation");
  }
  const path = `\\\\.\\pipe\\acyclic-report-${generation}`;
  const server = createServer();
  let socket, stopped, closing, resolveConnection, rejectConnection;
  const connected = new Promise((resolve, reject) => {
    resolveConnection = resolve;
    rejectConnection = reject;
  });
  // The owner may close before any consumer asks for the accepted channel.
  connected.catch(() => {});
  const stopListening = () => stopped ??= new Promise((resolve, reject) => {
    server.close(error => {
      if (error && error.code !== "ERR_SERVER_NOT_RUNNING") reject(error);
      else resolve();
    });
  });
  const close = () => {
    if (!closing) {
      rejectConnection(Object.assign(new Error("step=report-pipe-connect; recovery=retain the original selected process owner until its report pipe settles"), {
        code: "REPORT_PIPE_CLOSED",
      }));
      socket?.destroy();
      closing = stopListening();
    }
    return closing;
  };
  server.on("error", rejectConnection);
  server.on("connection", accepted => {
    if (socket || closing) { accepted.destroy(); return; }
    socket = accepted;
    // Stop accepting immediately. The same listener's close also joins the
    // accepted channel, so the original owner retains its physical cleanup.
    stopListening().catch(rejectConnection);
    resolveConnection(accepted);
  });
  try {
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(path, () => {
        server.removeListener("error", reject);
        resolve();
      });
    });
  } catch (error) {
    await close();
    throw error;
  }
  return {
    environment: { ACYCLIC_REPORT_PIPE: path, ACYCLIC_REPORT_OWNER_PID: String(process.pid) },
    connected,
    close,
  };
}
