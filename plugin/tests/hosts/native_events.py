"""Admit recorded native CLI initialization output for process stand-ins."""
import hashlib
import json
import pathlib


def native_event_fixture(host):
    directory = pathlib.Path(__file__).resolve().parent / 'fixtures/native'
    path = directory / (host + '.jsonl')
    receipt = next(row for row in json.loads((directory / 'provenance.json').read_text())
                   if row['host'] == host)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == receipt['fixture_sha256'], (
        'step=admit captured native event; recovery: restore ' + str(path))
    event = json.loads(path.read_text())
    identifier = event.get('thread_id') or event.get('session_id') or event.get('id')
    assert isinstance(identifier, str) and identifier, (
        'step=read captured native session; recovery: restore ' + str(path))
    return path, identifier
