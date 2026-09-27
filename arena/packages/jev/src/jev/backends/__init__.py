from .base import Backend
from .mock import MockBackend

__all__ = ["Backend", "MockBackend", "get_backend"]


def get_backend(name: str, **kw) -> Backend:
    """Construct a backend by name: "mock", "space", "local", "jev[:<model>]", "openrouter[:<model>]", "http" / "http://host:port", or "replay:<log path>"."""
    if name == "mock":
        return MockBackend(**kw)
    if name == "space":
        from .space import SpaceBackend
        return SpaceBackend(**kw)
    if name == "local":
        from .local import LocalBackend
        return LocalBackend(**kw)
    if name == "jev" or name.startswith("jev:"):
        from .jev import JevBackend
        model = name.split(":", 1)[1] if ":" in name else None
        return JevBackend(**({"model": model} if model else {}), **kw)
    if name == "openrouter" or name.startswith("openrouter:"):
        from .openrouter import OpenRouterBackend
        model = name.split(":", 1)[1] if ":" in name else None
        return OpenRouterBackend(**({"model": model} if model else {}), **kw)
    if name == "http" or name.startswith("http:"):
        from .http import HttpBackend
        return HttpBackend(name if name.startswith("http:") and "//" in name else "http://127.0.0.1:8788", **kw)
    if name.startswith("replay:"):
        from ..log import DecisionLog, ReplayBackend
        return ReplayBackend(DecisionLog(name.split(":", 1)[1]), **kw)
    raise ValueError(f"unknown backend {name!r}")
