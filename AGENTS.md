# Project Constraints

- The NAS plugin management UI is the fnOS "fnMusic 扩展管理" window. Its frontend lives in `webui-service/static/index.html`, `webui-service/static/app.js`, and `webui-service/static/style.css`.
- UI changes for this plugin must target that NAS management UI. Do not redesign or modify an external website.
- Keep the existing NAS app shell, navigation, and settings workflow. Build the installable FPK with `packaging/fpk/build.sh`.
- `VERSION` is the fnmusic-ext plugin version. A music source script version comes from the script's `@version` metadata and must be displayed separately for each source.
- The NAS WebView may cache static assets. Every packaged management-page asset URL must include the plugin version as a cache key.

## Top 图与标签（必须遵守）

- `TOP.md`（仓库根目录）是知识点地图 / 排查入口：遇到问题先读它，按「症状 → 标签速查」
  定位标签，再 `grep -n 'TOP[<标签>]' <文件>` 直达代码。
- 关键代码上方有 `# TOP[标签] 一句话说明` 标记（JS 用 `//`）。触及新知识点时补标记；
  命名规则 `top.<域>.<子项>`，一个知识点一个标签，不重复挂。
- **每做完一个功能/修复，必须运行 `python3 tools/update_top_map.py`** 重新生成 TOP.md
  的标签索引（行号与标记同步）；如有新典型症状，在 TOP.md 速查表加一行。
  `packaging/fpk/build.sh` 打包时会自动执行该脚本。
- 校验命令：`python3 tools/update_top_map.py --check`（悬空引用/重复标签会报错）。

## GitHub 多端发布中心

- 发布仓库：https://github.com/maiga512/shuxiamusic-release（多端发布中心）。
- 用户说「上传 GitHub」时：把最新 `dist/fnmusic-ext-<VERSION>.fpk` 传到发布中心的
  **飞牛在线听插件** 位置，并同步更新该处的版本号（与仓库 `VERSION` 一致）。
- 插件版本号唯一来源是仓库根 `VERSION` 文件；每次发布前确认 FPK 文件名、发布中心
  版本号、TOP.md/CHANGELOG 三处一致。
