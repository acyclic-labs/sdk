from __future__ import annotations

import pytest

from acyclic_sdk.remote import Credentials, _target, _validate


def test_endpoint_without_scheme_defaults_to_secure_grpc() -> None:
    assert _target("api.acyclic.dev") == ("api.acyclic.dev", True)
    assert _target("https://api.acyclic.dev") == ("api.acyclic.dev", True)
    assert _target("grpcs://api.acyclic.dev:443") == ("api.acyclic.dev:443", True)


def test_plaintext_requires_an_explicit_local_endpoint_scheme() -> None:
    assert _target("grpc://127.0.0.1:50051") == ("127.0.0.1:50051", False)
    assert _target("http://127.0.0.1:50051") == ("127.0.0.1:50051", False)


def test_bearer_and_mtls_credentials_are_validated_before_dial() -> None:
    _validate(Credentials.bearer("token"), True)
    _validate(Credentials.mtls(b"cert", b"key", bearer_token="token"), True)
    with pytest.raises(ValueError, match="secure endpoint"):
        _validate(Credentials.bearer("token"), False)
    with pytest.raises(ValueError, match="invalid bearer"):
        _validate(Credentials(bearer_token="bad\nvalue"), True)
