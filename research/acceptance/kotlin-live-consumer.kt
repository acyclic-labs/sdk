/**
 * Kotlin entrypoint for the Rust-owned JVM live scenario runner.
 *
 * The request bytes, expected fields, authority descriptors, and endpoint are
 * all supplied to JvmLiveConsumer. Kotlin therefore shares the exact Rust
 * scenario corpus instead of maintaining a second request or assertion set.
 */
object KotlinLiveConsumer {
    @JvmStatic
    fun main(args: Array<String>) {
        JvmLiveConsumer.main(args)
    }
}
