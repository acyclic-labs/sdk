/**
 * Scala entrypoint for the Rust-owned JVM live scenario runner.
 *
 * JvmLiveConsumer owns descriptor loading, request decoding, transport calls,
 * response assertions, and raw evidence. Scala only supplies the launch
 * surface so it cannot drift into a separately authored wire contract.
 */
object ScalaLiveConsumer {
  def main(args: Array[String]): Unit = JvmLiveConsumer.main(args)
}
