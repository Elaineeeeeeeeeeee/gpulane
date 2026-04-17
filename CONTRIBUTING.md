# Contributing

Thanks for contributing to `gpulane`.

This project is still evolving quickly. The goal of these guidelines is to keep contributions easy to review and safe to merge.

## Before You Start

- Check for an existing issue or pull request before starting work.
- Open an issue first for large changes, behavior changes, or architectural work.
- Keep pull requests narrowly scoped. Small, focused changes review faster and regress less.

## Development Expectations

- Preserve existing behavior unless the change explicitly intends to modify it.
- Add or update tests when changing user-facing behavior, CLI behavior, or manifest validation.
- Prefer clear error messages and predictable CLI output.
- Avoid unrelated refactors in the same pull request.

## Pull Request Process

1. Fork the repo and create a branch with a descriptive name.
2. Make the smallest change that solves one problem well.
3. Run relevant tests locally.
4. Update docs or examples if the user-facing workflow changed.
5. Submit a pull request using the provided template.

## Commit Guidance

- Use clear commit messages that explain intent, not just file changes.
- Squash noisy fixup commits before review when possible.

## What Reviewers Look For

- Correctness and regression risk
- Backward compatibility for existing manifests and CLI flows
- Test coverage for changed behavior
- Documentation for any new command, flag, or config requirement

## Reporting Bugs

When filing a bug, include:

- What you ran
- What you expected
- What happened instead
- Relevant logs or terminal output
- Environment details such as OS, Python version, Ray version, and Redis availability if relevant

## Suggesting Features

Feature requests are most helpful when they include:

- The user problem
- Why current behavior is insufficient
- A concrete example workflow
- Any constraints or tradeoffs already considered

## Code of Conduct

By participating in this project, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
