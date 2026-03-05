# tortoise-oxigraph

This repository is installable and publishable from the repository root.

## Local install

```bash
pip install .
```

For development:

```bash
pip install -e .[dev]
```

## Build distribution artifacts

```bash
python -m build
```

This produces:

- `dist/*.tar.gz` (sdist)
- `dist/*.whl` (wheel)

## Publish to PyPI

```bash
python -m twine check dist/*
python -m twine upload dist/*
```

For TestPyPI first:

```bash
python -m twine upload --repository testpypi dist/*
```

Package source and backend usage docs are in `tortoise-oxigraph/README.md`.
