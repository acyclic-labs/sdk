import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
const path = "rust/crates/objects/src/v1/grpc/batch/reply.rs";
const controls = [
  ["ordinary-native-storage", "let metadata = batch_vector(native_count, meter)?;", "let metadata = batch_vector(sizes.value.len(), meter)?;", ["test", "--locked", "-p", "acyclic-objects", "--features", "local,http,test-support", "--lib", "batch_reply_ordinary_exchange_has_no_native_storage"]],
  ["oversized-text", "let capacity = text.len();", "let capacity = if self.meter.is_some() { TEXT_BYTES } else { text.len() };", ["test", "--locked", "-p", "acyclic-objects", "--features", "local,http,test-support", "--lib", "batch_reply_copies_only_final_string_values"]],
  ["metadata-handoff", "metadata.value.push(info.metadata);", "metadata.value.push(None);", ["test", "--locked", "-p", "acyclic-objects", "--lib", "batch_exchange_models_metadata_transfer_decode_settlement_and_drop"]],
  ["late-cancellation", "codec.record(bytes.len() as u64, 0)?;", "self.meter.as_ref().unwrap().admit_codec(bytes.len() as u64, 0, 0)?;", ["test", "--locked", "-p", "acyclic-objects", "--features", "local,http,test-support", "--lib", "batch_reply_completion_survives_a_genuinely_cancelled_meter"]],
  ["rejected-item-charge", "reply.failure.get_or_insert(error);", "reply.response._allocation.as_mut().unwrap().absorb(item_credit.as_mut().unwrap())?; reply.failure.get_or_insert(error);", ["test", "--locked", "-p", "acyclic-objects", "--features", "local,http,test-support", "--lib", "batch_reply_rejected_item_retires_capacity_before_the_next_item"]],
  ["lost-reply-result", "reply.response.value.results.push(result);", "reply.response.value.results.push(wire::PutObjectResult::default());", ["bash", "scripts/fuzz-objects-batch-reply.sh", "-runs=0"]],
];
export async function withMutationCopy(root, run) {
  const directory = mkdtempSync(join(tmpdir(), "sdk-objects-mutants-"));
  const controller = new AbortController();
  const terminate = signal => {
    process.exitCode = signal === "SIGINT" ? 130 : 143;
    controller.abort(new Error(`mutation qualification stopped by ${signal}`));
  };
  const interrupt = () => terminate("SIGINT"), stop = () => terminate("SIGTERM");
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", stop);
  try {
    cpSync(root, directory, { recursive: true, filter: file =>
      !relative(root, file).split(sep).some(part => [".git", "target", "node_modules", "managed-agents"].includes(part)) });
    return await run(directory, controller.signal);
  } catch (error) {
    if (!controller.signal.aborted) throw error;
  } finally {
    process.removeListener("SIGINT", interrupt);
    process.removeListener("SIGTERM", stop);
    rmSync(directory, { recursive: true, force: true });
  }
}

async function execute(command, args, options) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { ...options, stdio: ["ignore", "pipe", "pipe"] });
    let output = "", overflow = false;
    const collect = bytes => {
      if (output.length + bytes.length > 8 * 1024 * 1024) { overflow = true; child.kill(); }
      else output += bytes.toString();
    };
    child.stdout.on("data", collect);
    child.stderr.on("data", collect);
    child.once("error", reject);
    child.once("close", status => resolve({ status, output, overflow }));
  });
}

export async function qualify(root, selected) {
  if (selected.some(name => !controls.some(control => control[0] === name))) throw new Error("unknown production mutant");
  return withMutationCopy(root, async (directory, signal) => {
    const file = join(directory, path);
    const source = readFileSync(file, "utf8");
    const env = { ...process.env, CARGO_TARGET_DIR: process.env.CARGO_TARGET_DIR ?? join(root, "target") };
    for (const [name, before, after, args] of controls) {
      if (selected.length && !selected.includes(name)) continue;
      if (source.split(before).length !== 2) throw new Error(`${name}: mutation site must be unique`);
      writeFileSync(file, source.replace(before, after));
      const command = args[0] === "bash" ? args[0] : "cargo";
      const commandArgs = command === "bash" ? args.slice(1) : args;
      const mutantEnv = name === "metadata-handoff" ? { ...env, RUSTFLAGS: `${env.RUSTFLAGS ?? ""} --cfg objects_loom` } : env;
      const { status, output, overflow } = await execute(command, commandArgs, { cwd: directory, env: mutantEnv, signal, timeout: 120000 });
      const executed = name === "lost-reply-result" ? /assertion.*failed|panicked at/.test(output) : /test result: FAILED/.test(output);
      if (status === 0 || overflow || !executed || /could not compile/.test(output)) throw new Error(`${name}: mutant was not rejected by its executing oracle\n${output}`);
      console.log(`production mutant rejected: ${name}`);
      writeFileSync(file, source);
    }
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) await qualify(process.cwd(), process.argv.slice(2));
