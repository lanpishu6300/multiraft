# Contributing

Thanks for helping improve **multiraft**.  
**中文：** [CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md) · **Wiki：** [docs/wiki/en](docs/wiki/en/Home.md) / [docs/wiki/zh](docs/wiki/zh/Home.md)

## Development setup

```bash
# Toolchain: recent stable Rust; always:
export PATH="$HOME/.cargo/bin:$PATH"
cargo test --workspace
./scripts/test_all.sh
```

## Branching

- Default integration branch: `main`
- One logical change per PR; keep net/store/demo changes separable when possible

## Before you open a PR

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets`
3. `cargo test --workspace`
4. If you touch failover / demo paths: `./scripts/acceptance.sh` (must print `ACCEPTANCE OK`)
5. Optional chaos: `./scripts/chaos.sh` or `CHAOS=1 ./scripts/test_all.sh`
6. Optional Jepsen (Java 17+, `lein`): `./scripts/run_jepsen.sh`

## openraft pin

Do **not** widen `openraft` / `openraft-multi` away from the exact `=` pin in the workspace `Cargo.toml` without a deliberate bump, upstream example diff, and re-run of tests + acceptance. See [docs/upstream.md](docs/upstream.md).

## Design docs

Behavioral or architectural changes should update the relevant file under `docs/specs/` (or add a dated design). Keep the repo **self-contained**. Bilingual ops docs live under `docs/` (`foo.md` + `foo.zh-CN.md`).

## Commit messages

Prefer Conventional Commits style:

- `feat(net): …`
- `fix(store): …`
- `test: …`
- `docs: …`
- `chore: …`

Do not add `Co-authored-by` (or similar) trailers for editor/AI tools (`cursoragent@cursor.com` adds Cursor Agent to GitHub Contributors). After clone:

```bash
./scripts/install-git-hooks.sh   # prepare-commit-msg / commit-msg / pre-push
./scripts/check-no-ai-traces.sh
```

If history already has Cursor attribution and you need it off Contributors: `./scripts/rewrite-drop-cursor-coauthor.sh`, then `git push --force-with-lease`.

Docs are bilingual where applicable (`foo.md` + `foo.zh-CN.md`). Keep wording concrete; avoid tool/agent attribution in tree or history you push.

## License

By contributing, you agree that your contributions are licensed under the **Apache License 2.0** (see `LICENSE`).
