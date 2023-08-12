# Contributor's guide

## Dependency management

Dependencies are managed with `poetry`. Make sure you have poetry
[installed on your system](https://python-poetry.org/docs/#installation),
and then install package dependencies:

```bash
poetry install
```

## Code verification

A short-hand to run all verification tasks is as follows:

```bash
poetry run verify-all
```

To only run the tests:

```bash
poetry run test
```

To only run the type checks:

```bash
poetry run typecheck 
```

To only run the linting checks:

```bash
poetry run lint 
```

## Code style

All code formatting is handled by the
[`black`](https://black.readthedocs.io/) auto-formatter. You can
auto-format your code changes with:

```bash
poetry run reformat
```

CI will reject changes that do not comply with `black`.

Additional lint checks are handled by a few linting tools, including
`pylint`, `pycodestyle`, and `pydocstyle`.

Type hints are strongly encouraged, and are checked during CI.

## Code coverage

Coverage reports are uploaded to `codecov.io`
[here](https://codecov.io/gh/asuiu/sparkorm). The `test` task
generates coverage reports to the `reports` directory. CI will
record coverage reports, and also upload them to `codecov.io`.
The badge is configured
[here](https://codecov.io/gh/asuiu/sparkorm/settings/badge).

## Versioning and tracking changes

This library uses [Semantic Versioning](https://semver.org/) (semver).

To track changes to the library (major, minor, patch), this library
uses a version of
[Conventional Commits](https://woile.github.io/commitizen/tutorials/writing_commits/#conventional-commits):

- The change is a PATCH change if the message title begins with `fix:`.
- The change is a MINOR change if the message title begins with `feat:`.
- The change is a MAJOR change if the message includes `BREAKING CHANGE`.
- If the commit history does not match any of the above, no new
  release will happen.

`commitizen` is used to identify releasable changes and the resulting
version bump.

A note on squashed or rebased commits: If commit histories are squashed before
merge/rebase, the resulting commit message must be compliant with
these conventions. If using the Github UI to merge, the `fix:`/`feat:`
lines must be put in the commit title field, and/or `BREAKING CHANGE`
put in the commit message body.

## Change types (`CHANGE_TYPE`)

As stated above, the following change types are used to indicate a
version change:

- `feat`: Introduces a new feature to the codebase (this correlates
  with MINOR in semantic versioning).
- `fix`: Patches a bug in the codebase (this correlates with PATCH
  in semantic versioning).

In addition, this project adopts the
[Angular](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#type)
change types:

- `build`: Changes that affect the build system or external
  dependencies.
- `ci`: Changes to CI configuration files and scripts.
- `docs`: Documentation only changes.
- `perf`: A code change that improves performance.
- `refactor`: A code change that neither fixes a bug nor adds a feature.
- `style`: Changes that do not affect the meaning of the code
  (white-space, formatting, etc).
- `test`: Adding missing tests or correcting existing tests.

## Branch naming

Branches should be named:

```
<CHANGE_TYPE>/<CHANGE_SUMMARY>
```

For example:

- `fix/broken-arg-type`
- `docs/diagram-for-inheritance`

## CI runners

Ideally, this project's CI would run entirely via Github Actions. Previously,
the project was part-migrated to Actions (see commit history). However, in
the interim, the project was migrated back to CircleCI exclusively.

- [CircleCI](https://circleci.com/gh/asuiu/workflows/sparkorm/tree/master):
  A utility workflow to support releasing a new version of the library.

## Creating a new release

If you have accumulated releasable changes that you would like to
publish, the following instructions explain how to make a new release.

### Making a new release via CI

_(Via CircleCI...)_ A CircleCI workflow named `release_workflow` is executed on each
merge to `master`. To make a release, approve the `request_release`
hold step via the
[CircleCI interface](https://circleci.com/gh/asuiu/workflows/sparkorm/tree/master).

#### CI setup

The release process requires the following permissions and secrets.

**Github repo**: The CI runner (i.e., CircleCI) needs read and write access to the upstream
Github repo. This is currently implemented using a read-write deploy key. For more info, refer
to the project's
[CircleCI ssh page](https://app.circleci.com/settings/project/github/asuiu/sparkorm/ssh)
and the CircleCI docs on
[Setting up new SSH keys](https://circleci.com/docs/github-integration/#create-additional-github-ssh-keys)

**Publishing to PyPI**: Two env vars should be set up. Via the CircleCI
[env vars page](https://app.circleci.com/settings/project/github/asuiu/sparkorm/environment-variables)
for the
project.

- `PYPI_USERNAME`: Set to `__token__`. (Because this is using token-based auth.)
- `PYPI_TOKEN`: The secret.

#### Debugging interaction between CI runner and Github

It can be useful to check that the CI runner (currently, CircleCI) can correctly
write to the Github repo. And it is useful to do this without creating a genuine
tag or release to PyPI.

To debug this, a special branch `debug-github-tag-push` has an associated debug
workflow that will attempt to push a git tag to the Github repo. For every commit,
it will push a new `debug-gh-tagger-...` tag.

It is good to clean these up from time to time. All tags can be found at on the
[tags page](https://github.com/asuiu/sparkorm/tags).

### Making a new release by hand

First, ensure you are on an up-to-date `master` branch. 

You can check whether there are any releasable changes with:

```bash
poetry run find-releasable-changes
``` 

The following command will look through the commit history to detect
whether any releasable changes have been made. If this is the case,
it will bump the project's version number (MAJOR/MINOR/PATCH, as
automatically identified from the commit history) and then create a
new commit and tag for the release. The tag will be pushed to the
upstream repository.

```bash
poetry run prepare-release
```

Finally, ensure you have PyPI credentials available and then
enact the release:

```bash
export PYPI_USERNAME="MY_USERNAME"
export PYPI_PASSWORD="MY_PASSWORD_OR_TOKEN"
poetry publish --build --username "${PYPI_USERNAME}" --password "${PYPI_TOKEN}" --no-interaction
```

## Design philosophy and conventions

- The API should favour strong type inference. This enables IDE completions
  and strong type checking. Weakly typed solutions may be provided as
  a convenience to those that benefit from them, but only as an
  alternative to the strongly typed version.
- The members of a `Struct` shall be sparkorm fields (derived from 
  `Base`). All other members (e.g., inner workings) will be private.
  This ensures clean IDE attribute completions.
