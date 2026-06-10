#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 -m venv wheel-test
wheel-test/bin/pip install -q --upgrade pip
wheel-test/bin/pip install -q target/wheels/*.whl
wheel-test/bin/python - <<'PY'
import kron

assert hasattr(kron, "schedule")
assert hasattr(kron, "start")
assert hasattr(kron, "shutdown")
print("kron wheel import ok")
PY
