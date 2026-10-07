from pathlib import Path
import os
import re

from setuptools import find_packages, setup
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


CARGO_MANIFEST = Path(
    os.environ.get(
        "ACYCLIC_ACTORS_CARGO_MANIFEST",
        Path(__file__).resolve().parents[1] / "Cargo.toml",
    )
)
_VERSION_RE = re.compile(r'^version\s*=\s*"([^"]+)"\s*$', re.MULTILINE)


def cargo_version() -> str:
    match = _VERSION_RE.search(CARGO_MANIFEST.read_text(encoding="utf-8"))
    if match is None:
        raise RuntimeError(f"no package version in {CARGO_MANIFEST}")
    return match.group(1)


class BinaryWheel(_bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self):
        _python, _abi, platform = super().get_tag()
        return ("py3", "none", platform.replace("-", "_"))


setup(
    name="acyclic-actors-uniffi",
    version=cargo_version(),
    description="Rust-owned Actors UniFFI Python facade",
    long_description=Path("README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    packages=find_packages("src"),
    package_data={
        "acyclic_actors_uniffi": ["*.dll", "*.so", "*.dylib", "py.typed"],
    },
    include_package_data=True,
    python_requires=">=3.9",
    license="Apache-2.0",
    cmdclass={"bdist_wheel": BinaryWheel},
)
