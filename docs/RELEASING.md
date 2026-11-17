# Preparing a release

The package is prepared for version 1.0.0. Ordinary CI runs do not publish.
Pushing a `v*` tag starts the release workflow; publication waits for the test
matrix and the configured `pypi` environment approval.

## Development checks

Use Python 3.12–3.15:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev,api,mcp,vis]'
python -m pytest
ruff check src tests
python -m pip check
```

The `dev` extra includes test, lint, build, and metadata-checking tools. API and
MCP tests skip when their optional dependencies are not installed. The browser
rendering regression needs Node.js (CI supplies Node 22). Tests with only `dev`
exercise the base package. The all-extras configuration exercises the complete
suite.

CI builds a source distribution, builds a wheel from that distribution, checks
package metadata, and installs that wheel into fresh environments. Tests run in a
temporary directory without the checkout's `src` directory on the import path.
The matrix tests Python 3.12, 3.13, 3.14, and 3.15 with five dependency profiles:
the base package, API dependencies, FastMCP 2.14.7, FastMCP 3.x, and all extras
with FastMCP 4.x. An additional Python 3.12 job checks the minimum Pydantic 2.8.0
dependency, for 21 installed-wheel test jobs. Newer interpreters resolve compatible
Pydantic versions within the declared range. Artifacts are built once on Python
3.12 and the same wheel is tested across the matrix. Every matrix job must pass
before the release workflow can publish.

## Release candidate

1. Review [the migration guide](MIGRATING_TO_1_0.md), the public API, and the
   changelog. Update `Unreleased` with the version and release date when final.
2. Set `project.version` in `pyproject.toml` and the README version to the chosen
   release version. Update the development-status classifier when declaring the
   stable release. Confirm repository links and package metadata.
3. Build into a fresh directory and check both artifacts:

   ```bash
   release_dir=$(mktemp -d)
   python -m build --outdir "$release_dir"
   python -m twine check --strict "$release_dir"/*
   ```

4. Run the full CI matrix on the exact release commit. Confirm the all-extras
   job passes without optional-test skips, and review the built artifact contents.
5. Smoke-test the checkout examples relevant to the deployment: Flask/Swagger,
   MCP stdio and HTTP, and visualization. Verify application callback behavior
   against the migration guide, including approval conflicts and runtime pause.
6. Commit and push the release changes, including both workflow files, then tag
   that exact commit. For version 1.0.0:

   ```bash
   git tag -a v1.0.0 -m "Release 1.0.0"
   git push origin v1.0.0
   ```

7. Open **Actions → Publish release to PyPI**. After validation and tests pass,
   click **Review deployments**, select `pypi`, and **Approve and deploy**.
8. Confirm the release appears on PyPI and install it in a fresh supported Python
   environment with `python -m pip install 'operation-dispatcher==1.0.0'`.

## Publishing configuration

The pending publisher on PyPI must match these values:

| Setting | Value |
| --- | --- |
| Project | `operation-dispatcher` |
| Repository owner | `JaFeKl` |
| Repository | `OperationDispatcher` |
| Workflow filename | `release.yml` |
| GitHub environment | `pypi` |

The GitHub `pypi` environment should allow release tags matching `v*` and have
the intended required reviewer. Leave **Prevent self-review** disabled if the
same maintainer triggers and approves releases. No PyPI token secret is needed;
only the publishing job receives `id-token: write` for
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

The workflow checks that the tag exactly matches `v` plus `project.version` in
`pyproject.toml`. It calls the same reusable `ci.yml` used by branch and pull
request checks, so the release runs the complete matrix against its own commit.
The publish job downloads that run's `distributions` artifact and uploads those
exact files; it does not rebuild them. No publication runs on ordinary branch
pushes or pull requests. Release candidates such as `v1.0.0rc1` also target
production PyPI when approved; this workflow does not publish to TestPyPI.

If validation or testing fails, the publish job is skipped. If publication fails,
inspect its log and PyPI before retrying: an upload may have partially succeeded.
Do not move a published release tag to different code; prepare a new version for
code changes. A GitHub Release page is optional and is not the publishing trigger.
