# Release Process

## Files to update
1. **`version/version.go`**: Update `fdwVersion` to the new version (e.g., `"2.2.0"`).
2. **`CHANGELOG.md`**: Add a new entry at the top for the release version with the current date.

## Commit
- Commit message for release changes should always be the release version number (e.g., `v2.2.0`).

## Release PRs
When creating PRs for a release, always create two:
1. Against `develop`: title should be `Merge branch '<branchname>' into develop`
2. Against `main`: title should be `Release steampipe-postgres-fdw v<version>`

## Tagging and Build
1. Create a git tag matching the version (e.g., `v2.2.0`) on the release branch commit.
2. Push the tag to origin. This triggers the **Build Draft Release** workflow (`.github/workflows/buildimage.yml`).
3. The workflow builds the FDW shared library for all four platforms:
   - Darwin x86_64 (`macos-15-intel`)
   - Darwin ARM64 (`macos-latest`)
   - Linux x86_64 (`ubuntu-22.04`)
   - Linux ARM64 (`ubuntu-22.04-arm`)
4. On success, it creates a **draft release** on GitHub with all build artifacts.
5. Verify all build jobs pass before proceeding.

## Publishing the FDW Image
1. Trigger the **Publish FDW Image** workflow (`.github/workflows/publish.yml`) manually via `workflow_dispatch`.
   - **Branch:** `develop`
   - **Input `release`:** the version tag (e.g., `v2.2.0`)
2. This workflow downloads the draft release assets and pushes the FDW image to `ghcr.io/turbot/steampipe/fdw:<version>`.
3. Non-RC versions are also tagged as `latest`.

## Testing a workflow change without releasing
1. Push a temporary test tag (e.g., `v0.0.0-test-runner`) from the branch with the workflow change.
2. Verify all build jobs pass.
3. Clean up: delete the test tag (`git push origin --delete <tag>`) and the draft release (`gh release delete <tag> --yes`).
