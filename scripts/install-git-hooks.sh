#!/usr/bin/env bash
# Install repo git hooks (run once after clone).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
hook_src="$root/scripts/git-hooks/commit-msg"
hook_dst="$root/.git/hooks/commit-msg"

if [[ ! -d "$root/.git" ]]; then
  echo "install-git-hooks: not a git checkout: $root" >&2
  exit 1
fi

mkdir -p "$root/.git/hooks"
cp "$hook_src" "$hook_dst"
chmod +x "$hook_dst"
echo "installed $hook_dst"
