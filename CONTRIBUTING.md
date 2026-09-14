# Contributing

## Development setup

Install [uv](https://docs.astral.sh/uv/), then run from the repository root:

```bash
uv sync
uv run pytest
uv run pre-commit run --all-files
uv run --group docs mkdocs serve
```

The local Iceberg and MinIO demo also needs Docker Compose. See the README's
[local demo](README.md#local-demo) section for details.

## Commits and releases

Pull request titles must use [Conventional Commits](https://www.conventionalcommits.org/),
using one of the types checked by the repository workflow: `feat`, `fix`,
`docs`, `test`, `ci`, `refactor`, `perf`, `chore`, or `revert`.

After a conventional commit is merged to `main`, release-please opens or
updates a release pull request. Merging that pull request creates the version
tag and GitHub release. Publishing the release triggers the PyPI workflow,
which verifies the tag and publishes with OIDC trusted publishing. PyPI
publishing only runs for releases targeting `main`; it does not publish
arbitrary branches or rewrite the package version.
