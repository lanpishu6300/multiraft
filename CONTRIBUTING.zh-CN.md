# 贡献指南

感谢参与 **multiraft**。  
**English：** [CONTRIBUTING.md](CONTRIBUTING.md) · **Wiki：** [docs/wiki/zh](docs/wiki/zh/Home.md) / [docs/wiki/en](docs/wiki/en/Home.md)

## 开发环境

```bash
# 工具链：较新的 stable Rust；始终：
export PATH="$HOME/.cargo/bin:$PATH"
cargo test --workspace
./scripts/test_all.sh
```

## 分支

- 默认集成分支：`main`
- 每个 PR 尽量只做一件事；net / store / demo 变更尽量可拆分

## 提交 PR 前

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets`
3. `cargo test --workspace`
4. 若改动切主 / Demo 路径：`./scripts/acceptance.sh`（须打印 `ACCEPTANCE OK`）
5. 可选 chaos：`./scripts/chaos.sh` 或 `CHAOS=1 ./scripts/test_all.sh`
6. 可选 Jepsen（Java 17+、`lein`）：`./scripts/run_jepsen.sh`

## openraft 锁定

未经刻意升版、对照上游示例 diff，并复测测试 + acceptance，**不要**放宽 workspace `Cargo.toml` 中 `openraft` / `openraft-multi` 的精确 `=` 锁定。见 [docs/upstream.md](docs/upstream.md) · [中文](docs/upstream.zh-CN.md)。

## 设计文档

行为或架构变更应更新 `docs/specs/`（或新增带日期的设计）。仓库需**自包含**。运维类文档在 `docs/` 下双语成对（`foo.md` + `foo.zh-CN.md`）。

## 提交信息

推荐 Conventional Commits：

- `feat(net): …`
- `fix(store): …`
- `test: …`
- `docs: …`
- `chore: …`

不要在提交里加编辑器 / AI 工具的 `Co-authored-by` 等归属行。克隆后建议：

```bash
./scripts/install-git-hooks.sh
./scripts/check-no-ai-traces.sh
```

文档尽量中英成对（`foo.md` + `foo.zh-CN.md`）。表述具体即可，仓库与将推送的历史中不要留下工具 / agent 归属痕迹。

## 许可

贡献即表示同意以 **Apache License 2.0** 授权（见 `LICENSE`）。
