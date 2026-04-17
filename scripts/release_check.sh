#!/usr/bin/env bash
set -euo pipefail

python -m unittest discover -s tests -p 'test_*.py'
ruff check .
python -m build
python -m twine check dist/*
