#!/usr/bin/env bash
# Release a new version of tileorm:
#   bump version -> commit + tag -> build -> push -> publish to PyPI -> GitHub release
set -euo pipefail

cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

# 1Password secret reference for the PyPI API token, e.g. "op://Private/PyPI/token"
# Used with `op read` when the 1Password CLI is installed and signed in.
PYPI_TOKEN_OP_REF="${PYPI_TOKEN_OP_REF:-"op://Private/PyPi TileORM/token"}"

if ! command -v gum >/dev/null 2>&1; then
    echo "gum is required: https://github.com/charmbracelet/gum" >&2
    echo "  brew install gum" >&2
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    gum log -l error "uv is required"
    exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
    gum log -l error "gh is required"
    exit 1
fi

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    gum log -l error "working tree is not clean, commit or stash changes first"
    exit 1
fi

if [[ -n "$(git status --porcelain --untracked-files=normal | grep '^??')" ]]; then
    gum log -l warn "untracked files present, they will not be included in the release commit"
    git status --porcelain --untracked-files=normal | grep '^??'
    gum confirm "Continue anyway?" || exit 1
fi

gum style --border normal --margin "1" --padding "0 2" --border-foreground 212 "tileorm release"

bump=$(gum choose --header "Version bump" major minor patch)

new_version=$(uv version --bump "$bump" --dry-run --short)

gum confirm "Bump to $new_version, commit, and tag?" || exit 1

uv version --bump "$bump" --short >/dev/null

git add pyproject.toml
git commit -m "version $new_version"
git tag "v$new_version"

gum log -l info "Building..."
rm -rf dist
uv build

gum confirm "Push commit and tag to origin?" || exit 1
git push
git push --tags --no-verify

gum log -l info "Publishing to PyPI..."
token=""
if command -v op >/dev/null 2>&1; then
    token=$(op read "$PYPI_TOKEN_OP_REF" 2>/dev/null) || {
        gum log -l warn "couldn't read $PYPI_TOKEN_OP_REF from 1Password, falling back to manual entry"
        token=""
    }
fi
if [[ -z "$token" ]]; then
    token=$(gum input --password --placeholder "PyPI token")
fi
uv publish --username __token__ --password "$token"

gum log -l info "Creating GitHub release..."
notes=$(gum write --placeholder "Release notes (markdown supported)...")

gh release create "v$new_version" dist/* \
    --title "v$new_version" \
    --notes "$notes"

gum log -l info "Released v$new_version"
