import json, hashlib
from pathlib import Path
p=Path('.')
def h(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest().upper()
pkg='out-combined/Actors.Stream.0.1.0-c3d82376-combined.nupkg'
receipt={
  'schema':'research-csharp-stream-qualification/v1',
  'date':'2026-10-07',
  'status':'ASSEMBLED_CSHARP_STREAM_COMBINED_NUGET_MACOS_RUNTIME_VERIFIED_WINDOWS_LINUX_ASSET_INHERITANCE',
  'scope':'combined parent package assembly and fresh macOS external consumer qualification; no publish, merge, registry, or deployment',
  'producer':{
    'path':'Q:/sdk/work/actors-stream-uniffi-kotlin-prototype-20261007',
    'git_revision':'c3d82376d5b5e32a7886a4af7f8a65de8fe60795',
    'source':{
      'lib_sha256':'407A338212CF2B208D99280B0804C3F83FB4AE01315519CB671C3167B12F58D9',
      'cargo_toml_sha256':'2C186AAE94FCFF2D99774EF453507CE0DD861C314C939828E2AB9A014720B9BD',
      'cargo_lock_sha256':'7B2A751A8F5774031B5B9C9D71AF4670A1D8B46F77C5958CD66575E7B92FBCCB',
      'uniffi_toml_sha256':'ADAB4F663D8E89E40167715922CBA7EAD525F9E110C9C55C7960E7C69A8CE824'
    },
    'dependency':{
      'path':'Q:/sdk/work/sdkgen-actors-c8-minimal/rust/crates/stream',
      'lib_sha256':'36FB4673272FF6DAFE77DDDCCE616A22CB0DF751FCA7257E9E918E47DC8DAC12',
      'cargo_toml_sha256':'72E0B44159FF74E8418B6F9882FC8E0309BB91830A7A197043467B723274B358'
    }
  },
  'generator':{
    'path':'Q:/sdk/work/uniffi-bindgen-cs-sourcefix-20261007',
    'git_revision':'e10ce410eb3a10cc19c7928b93ea8d84e038c034',
    'binary_sha256':'86AA185FFAE9507372DBD914459D7355183493456DD55EC651C6B398E3D8AC26',
    'generated_source_sha256':'934F4618FC3BF0A70FA1C1A115AF450BA43AA3C9B35A0A42192BC847BCF60D67'
  },
  'adapter':{
    'path':'src/StreamAsyncEnumerable.cs',
    'sha256':'DC9BFB6EE5B6B42AAC9FB01318629119F6E6B4A1CB85D4DFB775407B6B5A6D83',
    'contract':'IAsyncEnumerable over generated RecordCursor/ChildCursor; Next receives caller CancellationToken; finally calls generated CloseCursor(CancellationToken.None)',
    'semantics':'thin adapter only; no ordering, recovery, transport, buffering, or validation logic'
  },
  'native':{
    'toolchain':'rust 1.98.1',
    'win_x64_sha256':'AD102FB905B9996480BCA69F66248B042C8112FDEAB520155573FBC740AB49A5',
    'linux_x64_sha256':'66E29E4EAED05CC838BCCC82E13E128C10A4CC30DAF0F400958C346976A966F2',
    'osx_arm64_sha256':'EF2E516A0630004DFB5C4470D96631035891E601BDB0B9C0EAB99FC8837485F6'
  },
  'package':{
    'id':'Actors.Stream',
    'version':'0.1.0-c3d82376-combined',
    'path':str(Path(pkg).resolve()),
    'sha256':h(pkg),
    'size_bytes':Path(pkg).stat().st_size,
    'layout_raw_evidence':'evidence/combined-package-layout.raw.txt',
    'layout_raw_sha256':h('evidence/combined-package-layout.raw.txt'),
    'default_rid_assets':['win-x64','linux-x64','osx-arm64'],
    'managed_assembly_sha256':'419C6D1850DD9B30B75A01E1763BCCB5195D8C9D34C424B8151057F7776DF670'
  },
  'fresh_macos_consumer':{
    'host':'ivar', 'platform':'macOS 15.6 arm64', 'dotnet':'8.0.425',
    'package_cache':'ivar:/tmp/nuget-csharp-stream-mac-combined',
    'restore_log':'evidence/combined-macos/consumer-restore.log',
    'restore_log_sha256':h('evidence/combined-macos/consumer-restore.log'),
    'build_log':'evidence/combined-macos/consumer-build.log',
    'build_log_sha256':h('evidence/combined-macos/consumer-build.log'),
    'local_run_log':'evidence/combined-macos/consumer-local-run.log',
    'local_run_log_sha256':h('evidence/combined-macos/consumer-local-run.log'),
    'remote_run_log':'evidence/combined-macos/consumer-remote-run.log',
    'remote_run_log_sha256':h('evidence/combined-macos/consumer-remote-run.log'),
    'installed_native_hashes':{
      'win-x64':'AD102FB905B9996480BCA69F66248B042C8112FDEAB520155573FBC740AB49A5',
      'linux-x64':'66E29E4EAED05CC838BCCC82E13E128C10A4CC30DAF0F400958C346976A966F2',
      'osx-arm64':'EF2E516A0630004DFB5C4470D96631035891E601BDB0B9C0EAB99FC8837485F6'
    },
    'passes':['CSHARP_STREAM_LOCAL_ORDERED_TYPED_U64_PASS','CSHARP_STREAM_LOCAL_CANCELLATION_CLOSE_RECOVERY_PASS','CSHARP_STREAM_REMOTE_TLS_ORDERED_TYPED_U64_PASS','CSHARP_STREAM_REMOTE_CANCELLATION_SERVER_CLOSE_RECOVERY_PASS'],
    'server_markers':['follow-open','follow-closed','child-open','child-closed'],
    'full_width':'ulong.MaxValue and 0x8000000000000000UL reached Rust provider; expected out_of_range boundary accepted'
  },
  'cross_platform_boundary':{
    'windows_linux':'existing c3d82376 receipts qualify the same managed/native source cohort; combined package was not relabeled as a fresh Windows/Linux consumer run',
    'macos':'fresh combined package restore/build/runtime above',
    'required_next_step':'fresh Windows and Linux external SDK consumers against this combined package before all-platform status is promoted'
  },
  'a370_artifact_size':{
    'commit':'a37048fb1443a4674735f529b9359d68c62f1ddd',
    'tracked_tree_bytes':23218653,
    'method':'git ls-tree -r --long sum for stream-csharp-c3d82376-20261007'
  },
  'history':{
    'previous_packages':'out/ and out-macos-arm64 preserved',
    'previous_receipts':'receipt.json and receipt-macos-arm64.json preserved and not relabeled',
    'remote_source_history':'not rewritten'
  }
}
Path('receipt-combined.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt,indent=2))
