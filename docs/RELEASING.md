# Releasing `gpulane`

This repository is not publishing to PyPI yet, but it now has the minimum structure needed to do so safely.

## Preconditions

- Pick and add a `LICENSE` file.
- Ensure `CHANGELOG.md` has an `[Unreleased]` section and a versioned section for the release.
- Confirm `pyproject.toml` has the target version.
- Confirm GitHub Actions `ci.yml` is green on the release commit.

## Local release checklist

1. Update `pyproject.toml` version.
2. Move notable entries from `CHANGELOG.md` `[Unreleased]` into a dated version section.
3. Run:

```bash
python -m unittest discover -s tests -p 'test_*.py'
python -m build
python -m twine check dist/*
```

Or run the repo shortcut:

```bash
make release-check
```

4. Tag the release commit:

```bash
git tag v0.1.2
git push origin v0.1.2
```

5. Create a GitHub Release using the generated notes as a starting point.

## Non-publish package verification

If you want release confidence without uploading to PyPI:

- run `make release-check` locally
- trigger the `Package` GitHub Actions workflow manually, or push a `v*` tag
- download the built `dist/` artifacts from the workflow run

## First PyPI publish

Preferred path:

1. Create a PyPI project for `gpulane`.
2. Configure a trusted publisher from GitHub Actions, or use a scoped API token stored as a repository secret.
3. Add a publish workflow only after the license and release ownership model are finalized.

Suggested publish command for a manual dry run:

```bash
python -m twine upload --repository testpypi dist/*
```

## Versioning policy

- Use semver.
- Patch: bug fixes and internal refactors with no intentional CLI contract changes.
- Minor: new commands, manifest fields, or scheduling behavior additions.
- Major: breaking manifest or CLI contract changes.

## Release notes

Each release note should answer:

- What user-visible problem was solved?
- What changed in the manifest contract or scheduler behavior?
- Are there migration steps for existing repos?
