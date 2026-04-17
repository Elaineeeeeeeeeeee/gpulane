.PHONY: test lint package release-check

test:
	python -m unittest discover -s tests -p 'test_*.py'

lint:
	ruff check .

package:
	python -m build
	python -m twine check dist/*

release-check:
	bash scripts/release_check.sh
