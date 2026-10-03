package dev.acyclic.embedded;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Consumer compiled only against the installed acyclic-embedded-jna artifact. */
public final class InstalledPackageConsumer {
  private static void check(boolean ok, String message) {
    if (!ok) throw new AssertionError(message);
  }

  public static void main(String[] args) {
    try (RustEmbedded sdk = new RustEmbedded()) {
      check(sdk.abiVersion() == RustEmbedded.ABI_VERSION, "ABI version");
      RustEmbedded.Append first = sdk.append("installed/package", bytes("first"));
      check(first.status() == RustEmbedded.OK && first.start() == 0 && first.end() == 1
          && first.tail() == 1, "first append");
      try (RustEmbedded.Reader reader = sdk.openReader("installed/package", 0, 8, false)) {
        RustEmbedded.Item item = reader.next();
        check(item.status() == RustEmbedded.OK && item.sequence() == 0
            && Arrays.equals(item.value(), bytes("first")), "finite read");
        check(reader.next().status() == RustEmbedded.END, "finite end");
      }
      RustEmbedded.Append second = sdk.append("installed/package", bytes("second"));
      check(second.status() == RustEmbedded.OK && second.start() == 1 && second.end() == 2
          && second.tail() == 2, "recovery append");
      try (RustEmbedded.Reader live = sdk.openReader("installed/package", 2, 8, true)) {
        live.cancel();
        check(live.next().status() == RustEmbedded.CANCELLED, "live cancellation");
      }
      System.out.println("installed Java package checks passed: append/read/release/recovery/cancel");
    }
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}



