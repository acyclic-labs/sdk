from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
src=Path('out-macos-arm64/Actors.Stream.0.1.0-c3d82376-macos-arm64.nupkg')
out=Path('out-combined/Actors.Stream.0.1.0-c3d82376-combined.nupkg')
out.parent.mkdir(exist_ok=True)
old='0.1.0-c3d82376-macos-arm64'
new='0.1.0-c3d82376-combined'
with ZipFile(src) as zin, ZipFile(out,'w',ZIP_DEFLATED) as zout:
    for info in zin.infolist():
        data=zin.read(info.filename)
        if info.filename=='Actors.Stream.nuspec':
            data=data.replace(old.encode(),new.encode())
        zout.writestr(info,data)
print(out, out.stat().st_size)
