#!/usr/bin/env bash
# Install repo git hooks (run once after clone).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

if [[ ! -d "$root/.git" ]]; then
  echo "install-git-hooks: not a git checkout: $root" >&2
  exit 1
fi

mkdir -p "$root/.git/hooks"
for hook in commit-msg prepare-commit-msg pre-push; do
  src="$root/scripts/git-hooks/$hook"
  dst="$root/.git/hooks/$hook"
  cp "$src" "$dst"
  chmod +x "$dst"
  echo "installed $dst"
done
