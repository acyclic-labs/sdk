#!/bin/bash
set -eu
exec python3 /home/var/bin/prune-wt-sdk.py "$@"
