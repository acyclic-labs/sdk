import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = fileURLToPath(new URL('../../../actors-root-review-20261007/installed-current-5/node_modules/@acyclic-labs/actors/', import.meta.url));
const bufRoot = fileURLToPath(new URL('../actors-types-consumer-20261007/node_modules/@bufbuild/protobuf/', import.meta.url));
let requests = 0;

function typeFor(path) {
  return { '.js': 'text/javascript', '.wasm': 'application/wasm', '.html': 'text/html' }[extname(path)] ?? 'application/octet-stream';
}
async function serveFile(root, relative, response) {
  const safe = normalize(relative);
  if (safe.startsWith('..')) { response.writeHead(403); response.end(); return; }
  try {
    const path = join(root, safe);
    const body = await readFile(path);
    response.writeHead(200, { 'content-type': typeFor(path), 'access-control-allow-origin': '*' });
    response.end(body);
  } catch {
    response.writeHead(404); response.end();
  }
}
const server = http.createServer(async (request, response) => {
  const path = new URL(request.url, 'http://127.0.0.1').pathname;
  response.setHeader('access-control-allow-origin', '*');
  response.setHeader('access-control-allow-headers', 'content-type,x-grpc-web,grpc-timeout,x-user-agent,authorization');
  response.setHeader('access-control-expose-headers', 'grpc-status,grpc-message');
  if (request.method === 'OPTIONS') { response.writeHead(204); response.end(); return; }
  if (request.method === 'GET' && path === '/') return serveFile(fileURLToPath(new URL('./', import.meta.url)), 'installed-invalid.html', response);
  if (request.method === 'GET' && path.startsWith('/pkg/')) return serveFile(packageRoot, path.slice('/pkg/'.length), response);
  if (request.method === 'GET' && path.startsWith('/buf/')) return serveFile(bufRoot, path.slice('/buf/'.length), response);
  if (request.method === 'GET' && path === '/metrics') { response.writeHead(200, { 'content-type': 'application/json' }); response.end(JSON.stringify({ requests })); return; }
  if (request.method === 'POST' && path.includes('/v1/actors/')) {
    requests += 1;
    const trailers = Buffer.from('grpc-status: 0\r\ngrpc-message: \r\n');
    const trailerFrame = Buffer.alloc(5 + trailers.length);
    trailerFrame[0] = 0x80;
    trailerFrame.writeUInt32BE(trailers.length, 1);
    trailers.copy(trailerFrame, 5);
    response.writeHead(200, { 'content-type': 'application/grpc-web+proto' });
    response.end(trailerFrame);
    return;
  }
  response.writeHead(404); response.end();
});
server.listen(0, '127.0.0.1', () => console.log(`http://127.0.0.1:${server.address().port}/`));
