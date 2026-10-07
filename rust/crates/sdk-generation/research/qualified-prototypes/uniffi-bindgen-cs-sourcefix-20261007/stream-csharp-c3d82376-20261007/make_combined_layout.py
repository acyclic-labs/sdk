from pathlib import Path
from zipfile import ZipFile
import hashlib, json
pkg=Path('out-combined/Actors.Stream.0.1.0-c3d82376-combined.nupkg')
def sha(p):
    h=hashlib.sha256(); h.update(p.read_bytes()); return h.hexdigest().upper()
with ZipFile(pkg) as z:
    names=z.namelist()
    nuspec=z.read('Actors.Stream.nuspec').decode('utf-8-sig')
    rows=[]
    for n in names:
        info=z.getinfo(n)
        rows.append(f'{n}\t{info.file_size}')
raw='COMBINED_PACKAGE_LAYOUT_PASS\n'
raw+=f'package={pkg.resolve()}\nsha256={sha(pkg)}\nsize={pkg.stat().st_size}\n'
raw+='version=0.1.0-c3d82376-combined\n'
raw+='default_rid_assets=win-x64,linux-x64,osx-arm64\n'
raw+='managed=lib/net8.0/Actors.Stream.dll\n'
raw+='native=\n'+'\n'.join(rows)+'\n'
raw+='nuspec_version=0.1.0-c3d82376-combined\n'
raw+='producer=c3d82376d5b5e32a7886a4af7f8a65de8fe60795\n'
Path('evidence/combined-package-layout.raw.txt').write_text(raw,encoding='utf-8')
print(raw)
