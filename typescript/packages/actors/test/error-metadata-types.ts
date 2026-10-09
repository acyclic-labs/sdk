import type { ActorsErrorMetadata } from "../src/index.js";

function diagnostic(metadata: ActorsErrorMetadata): void {
  const grpcCode: number | null | undefined = metadata.grpcCode;
  const serviceCode: number | null | undefined = metadata.serviceCode;
  const original: string = metadata.message;
  const decoded: string | null | undefined = metadata.serviceMessage;
  const bytes: number[] = metadata.rawDetails ? Array.from(metadata.rawDetails) : [];
  void [grpcCode, serviceCode, original, decoded, bytes];
  // @ts-expect-error diagnostics are readonly at the public boundary
  metadata.message = "replacement";
  // @ts-expect-error raw status bytes expose a readonly byte projection
  metadata.rawDetails?.fill(0);
  // @ts-expect-error unknown future service codes remain numeric, not string
  const wrong: string | null | undefined = metadata.serviceCode;
  void wrong;
}
void diagnostic;
