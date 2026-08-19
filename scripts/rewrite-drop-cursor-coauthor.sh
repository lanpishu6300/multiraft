#!/usr/bin/env bash
# Rewrite local history to drop Cursor / cursoragent Co-authored-by trailers.
# Run only when you intend to force-push and remove cursoragent from GitHub contributors.
#
# Usage:
#   ./scripts/rewrite-drop-cursor-coauthor.sh [--dry-run]
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${PATH}"
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

dry_run=0
if [[ "${1:-}" == "--dry-run" ]]; then
  dry_run=1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "not a git repository: $root" >&2
  exit 1
fi

filter='sed "/^[[:space:]]*Co-authored-by:.*cursoragent@cursor\\.com/d" \
  | sed "/^[[:space:]]*Co-authored-by:[[:space:]]*Cursor/d" \
  | sed "/^Made-with:[[:space:]]*Cursor/d" \
  | sed "/^Made with Cursor/d"'

hits=0
while read -r sha; do
  [[ -z "$sha" ]] && continue
  if git log -1 --format='%B' "$sha" | grep -Eiq \
    'Co-authored-by:.*cursoragent@cursor\.com|Co-authored-by:[[:space:]]*Cursor|Made-with:[[:space:]]*Cursor'; then
    echo "hit $sha $(git log -1 --format='%s' "$sha")"
    hits=$((hits + 1))
  fi
done < <(git rev-list --all)

if [[ "$hits" -eq 0 ]]; then
  echo "OK: no Cursor/cursoragent trailers in history"
  exit 0
fi

echo "found $hits commit(s) with Cursor/cursoragent attribution"
if [[ "$dry_run" -eq 1 ]]; then
  echo "dry-run: no rewrite performed"
  exit 0
fi

if ! command -v git-filter-repo >/dev/null 2>&1; then
  echo "install git-filter-repo (brew install git-filter-repo) and re-run" >&2
  exit 1
fi

git filter-repo --force --msg-filter "$filter"
echo "rewrote history; verify with: git log --format='%B' | grep -i cursor || echo clean"
echo "then: git push --force-with-lease origin <branch>"
