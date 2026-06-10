# Release Checklist

Kron `0.1.x` releases are embedded-first alpha releases.

The PyPI distribution name is `kron-scheduler`; the Python module name remains
`kron`.

## Local Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

python -m venv .venv
.venv/bin/pip install -U pip maturin pytest twine
.venv/bin/maturin develop
.venv/bin/python -m pytest -q tests/python
.venv/bin/maturin build --release
.venv/bin/twine check target/wheels/*
bash scripts/check-wheel-install.sh
```

## Clean Repo Check

Do not commit generated local artifacts:

- `target/`
- `.venv/`
- `.pytest_cache/`
- `.kron/`
- `__pycache__/`
- `*.pyc`

## Publish

Publishing can be done locally:

```bash
.venv/bin/maturin publish
```

Or through GitHub Actions:

1. Create the PyPI project `kron-scheduler`.
2. Create a GitHub environment named `pypi`.
3. Configure PyPI Trusted Publishing for `BuildByNexora/Kron`, workflow
   `.github/workflows/publish.yml`, environment `pypi`.
4. Create and push a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

5. Run the `Publish` workflow manually and pass `v0.1.0`.

After publishing, create a GitHub release with the same version tag and note that distributed mode is experimental.
