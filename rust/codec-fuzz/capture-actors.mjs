// Recapture the original Runtime deliveries, then admit all current Actor writers.
import {spawn} from 'node:child_process';
import {readFile, writeFile} from 'node:fs/promises';
import {createInterface} from 'node:readline';
import {fileURLToPath} from 'node:url';
import {join, resolve} from 'node:path';

const directory = fileURLToPath(new URL('.', import.meta.url));
const repository = resolve(directory, '../..');
async function cargo(args, cwd, observe = () => {}) {
  const child = spawn('cargo', args, {cwd, stdio: ['ignore', 'pipe', 'inherit']});
  const ended = new Promise(accept => {
    child.once('error', error => accept({error}));
    child.once('exit', (code, signal) => accept({code, signal}));
  });
  for await (const line of createInterface({input: child.stdout})) {
    process.stdout.write(line + '\n');
    observe(line);
  }
  const {error, code, signal} = await ended;
  if (error) throw error;
  if (code !== 0) throw Error(`cargo ${args.join(' ')} failed: ${signal ?? code}`);
}

const source = join(repository, 'rust/crates/actors/src/conformance/sockets.rs');
const original = await readFile(source);
const boundary = '            let checkpoint = checkpoint.ok_or_else(|| guest("missing journal"))?;';
const observer = `            if let Some(socket) = delivery.socket()? {
                println!("ACTOR_SOCKET_CAPTURE {}", serde_json::json!({
                    "bytes": crate::guest::wire::encode_delivery(&delivery, checkpoint),
                    "connection": socket.connection_id,
                    "event": socket.event,
                }));
            }
`;
const text = original.toString('utf8');
if (text.split(boundary).length !== 2) throw Error('SocketPackage delivery boundary changed');
const captures = new Map();
const names = ['open', 'text', 'binary', 'close-peer', 'close-actor', 'close-runtime'];
try {
  await writeFile(source, text.replace(boundary, observer + boundary));
  await cargo(['test', '--locked', '-p', 'acyclic-actors', '--lib',
    'local::sockets::tests::socket_conformance_passes_on_local_runtime', '--', '--exact', '--nocapture'],
  repository, line => {
    const marker = 'ACTOR_SOCKET_CAPTURE ';
    const start = line.indexOf(marker);
    if (start < 0) return;
    const capture = JSON.parse(line.slice(start + marker.length));
    const event = capture.event;
    const kind = event.type === 'message' ? ('text' in event.payload ? 'text' : 'binary') :
      event.type === 'close' ? `close-${event.initiator}` : event.type;
    if (names.includes(kind) && !captures.has(kind)) captures.set(kind, capture);
  });
} finally {
  await writeFile(source, original);
}
for (const name of names) if (!captures.has(name)) throw Error(`Runtime did not deliver ${name}`);
await writeFile(join(directory, 'captures/socket-deliveries.capture'),
  JSON.stringify(Object.fromEntries(names.map(name => [`socket-input-${name}-cuts-v1`, captures.get(name)]))) + '\n');
await cargo(['run', '--locked', '--example', 'capture', '--', '--only-actors', '--extend-goldens'], directory);
await cargo(['run', '--locked', '--example', 'replay', '--', 'actor_codecs'], directory);
