#!/usr/bin/env python3
"""Admit a captured real host export; reject missing/stale producer Cargo bytes."""
import hashlib, io, json, os, sys, tarfile, tempfile, time, types
from pathlib import Path

def main():
    started=time.monotonic()
    archive=Path(sys.argv[1]); expected=sys.argv[2]
    api_path=Path(sys.argv[3]) if len(sys.argv)>3 else Path(__file__).with_name("windows-artifacts.py")
    api=types.ModuleType("admission"); api.__file__=str(api_path.resolve())
    exec(compile(api_path.read_bytes(),str(api_path),"exec"),api.__dict__)
    assert api.digest(archive)==expected, "captured producer archive identity changed"
    with tempfile.TemporaryDirectory(dir=os.environ.get("CARGO_TARGET_DIR")) as scratch:
        root=Path(scratch); rows=[]
        for label in ("complete", "missing", "stale"):
            selected=archive
            if label!="complete":
                selected=root/(label+".tar.gz")
                before=time.monotonic()
                with tarfile.open(archive,"r|gz") as source, tarfile.open(selected,"w:gz",compresslevel=1) as target:
                    for member in source:
                        data=source.extractfile(member) if member.isfile() else None
                        if member.name=="logs/cargo.jsonl":
                            assert data is not None
                            if label=="missing": continue
                            raw=data.read()+b"\n"; member.size=len(raw);data=io.BytesIO(raw)
                        target.addfile(member,data)
                rows.append(dict(case=label+"-fixture-preparation",seconds=time.monotonic()-before))
            records=root/(label+"-records");records.mkdir();before=time.monotonic()
            try:
                admitted=api.admit_archive(records,selected,api.digest(selected))
            except ValueError as error:
                assert label!="complete", str(error)
                assert not list(records.iterdir()), "rejected before materialization"
                outcome=str(error)
            else:
                assert label=="complete", "accepted invalid real Cargo log"
                outcome="admitted "+str(admitted.directory)
            seconds=time.monotonic()-before
            assert seconds<10, (label,seconds)
            rows.append(dict(case=label,seconds=seconds,outcome=outcome))
        seconds=time.monotonic()-started
        print(json.dumps(dict(archive_sha256=expected,api_sha256=api.digest(api_path),seconds=seconds,rows=rows)),flush=True)
        assert seconds<10, ("complete captured-export controls including fixture preparation",seconds)

if __name__=="__main__":
    try: main()
    except Exception as error:
        raise RuntimeError("Export step=host-cargo-log-controls: "+str(error)+"; recovery=python3 scripts/windows-artifacts-host-log-test.py CAPTURED_ARCHIVE SHA256") from error
