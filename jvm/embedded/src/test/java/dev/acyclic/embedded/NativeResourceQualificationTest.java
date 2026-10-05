package dev.acyclic.embedded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Installed-package gate for the Rust native resource layout. The test is
 * enabled only by the release package lane, after Rust assets are staged into
 * the Maven resource root.
 */
@EnabledIfSystemProperty(named = "acyclic.embedded.native.qualification", matches = "true")
class NativeResourceQualificationTest {
  private static final String[] RESOURCES = {
      "/native/win-x86_64/acyclic_sdk_embedded_prototype.dll",
      "/native/win-aarch64/acyclic_sdk_embedded_prototype.dll",
      "/native/linux-x86_64-gnu/libacyclic_sdk_embedded_prototype.so",
      "/native/linux-aarch64-gnu/libacyclic_sdk_embedded_prototype.so",
      "/native/linux-x86_64-musl/libacyclic_sdk_embedded_prototype.so",
      "/native/linux-aarch64-musl/libacyclic_sdk_embedded_prototype.so",
      "/native/osx-x86_64/libacyclic_sdk_embedded_prototype.dylib",
      "/native/osx-aarch64/libacyclic_sdk_embedded_prototype.dylib"
  };

  @Test
  void loadsHostRustAssetFromClasspathWithoutOverride() throws Exception {
    assertNull(System.getProperty("acyclic.embedded.native.path"),
        "the installed consumer must use classpath RID selection");
    for (String resource : RESOURCES) {
      assertNotNull(RustEmbedded.class.getResource(resource),
          "missing Rust native resource " + resource);
    }
    try (InputStream manifest = RustEmbedded.class.getResourceAsStream("/native/native-manifest.json")) {
      assertNotNull(manifest, "missing Rust native provenance manifest");
      String manifestText = new String(manifest.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(manifestText.contains("acyclic.sdk.jvm.embedded.native-manifest.v1"));
      assertTrue(manifestText.contains("source_revision"));
    }

    try (RustEmbedded engine = new RustEmbedded()) {
      assertEquals(RustEmbedded.ABI_VERSION, engine.abiVersion());
      RustEmbedded.Append appended = engine.append(
          "jvm-installed", "rust-rid".getBytes(StandardCharsets.UTF_8));
      try (RustEmbedded.Reader reader = engine.openReader("jvm-installed", appended.start(), 1, false)) {
        RustEmbedded.Item item = reader.next();
        assertEquals(RustEmbedded.OK, item.status());
        assertEquals("rust-rid", new String(item.value(), StandardCharsets.UTF_8));
      }
    }
  }
}
