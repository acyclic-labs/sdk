"""Run the generated UniFFI Python binding against the canonical stream probe."""

from __future__ import annotations

import importlib
import pathlib
import shutil
import sys


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: uniffi_python_consumer.py <generated-dir> <library>")

    generated_dir = pathlib.Path(sys.argv[1]).resolve()
    library = pathlib.Path(sys.argv[2]).resolve()
    module_name = "acyclic_sdk_embedded_prototype"

    shutil.copy2(library, generated_dir / library.name)
    sys.path.insert(0, str(generated_dir))
    module = importlib.import_module(module_name)

    probe = module.UniFfiStreamProbe()
    receipt = probe.append("events.log", b"hello")
    assert (receipt.start, receipt.end, receipt.tail) == (0, 1, 1)

    reader = probe.open_reader("events.log", 0, False)
    record = reader.next()
    assert record.is_RECORD()
    assert (record.sequence, record.value) == (0, b"hello")
    assert reader.next().is_END()
    assert reader.next().is_END()

    cancelled = probe.open_reader("events.log", 0, True)
    assert cancelled.next().is_RECORD()
    cancelled.cancel()
    assert cancelled.next().is_CANCELLED()
    assert cancelled.next().is_CANCELLED()

    del cancelled, reader, probe


if __name__ == "__main__":
    main()
