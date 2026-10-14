# TOP 图 — fnmusic-ext 知识点地图

> **AI/排查入口**：排查问题先读本文件 → 在「症状 → 标签」表定位标签 →
> `grep -n 'TOP[<标签>]' <文件>` 直达代码（自动生成段的「位置」列有精确行号）。
> 标签索引由 `tools/update_top_map.py` 自动生成，行号永远与代码同步。

## 架构总览

```text
[飞牛音乐客户端 Web / App / 车载]
            │
            ▼
      [飞牛 Nginx]（Unix Socket）
            │
┌─────────────────────────────────────────────────────────┐
│ fnmusic-ext 代理（宿主机 systemd，零侵入接管 Socket）     │
│   ├─ 官方 API 透传 ──► 官方后端 (upstream socket)        │
│   ├─ 在线搜索/播放/歌词/封面/收藏/歌单                    │
│   ├─ 边播边存 Tee 落盘 + 封面/歌词回补                   │
│   └─ .env 热重载（2s 检测，白名单键免重启生效）           │
└───────────────┬──────────────────────────────────────────┘
                │ 127.0.0.1（音源仅本机；WebUI 供浏览器）
┌───────────────▼──────────────────────────────────────────┐
│ Docker 单容器 fnmusic-sources（supervisor 按需加载）      │
│   ├─ musicdl  8768→8001  57 平台聚合                     │
│   ├─ musicbox 8770→8002  网易云（扫码登录）              │
│   ├─ lxmusic  8772→8003  洛雪自定义源运行时              │
│   └─ WebUI    8774→8004  插件管理页（飞牛管理员鉴权）    │
└──────────────────────────────────────────────────────────┘
```

## 关键请求链路

- **播放**：客户端 → `top.stream.open`（开流：本地缓存 → `top.stream.resolve` 取址 →
  `top.wan.direct` 外网 302 / 内网代理 Tee）→ 失败走 `top.stream.fallback` 换源。
- **搜索**：`top.search`（本地优先 + 音源窗口分页）→ `top.search.cache` 防抖缓存。
- **收藏**：`top.fav` / `top.fav.delete` → ceru 写回（`top.ceru.op` 歌单 operation +
  `top.ceru.fav` 独立存储），未注册歌按 `top.guid` 主键合成。
- **歌单**：读 `top.playlist.view`（注入 ceru/导入/推荐/网易虚拟歌单）；
  写 `top.playlist.write`（官方透传 + 本地桶 + `top.ceru.op` + `top.imp.add`）。
- **多端同步**：`top.ceru`（协议/缓存/注册表）；状态接口 `top.ceru.status`。
- **部署**：`top.takeover`（socket 接管守护）→ `top.fpk`（打包）→ VERSION 见 `top.version`。

## 症状 → 标签速查

| 症状 | 先看 |
| --- | --- |
| 外网播放慢 / 首开慢 | `TOP[top.wan.direct]`、`TOP[top.lx.prefetch]`、`TOP[top.quality]` |
| 歌曲跳过 / 不能播 / 成功率低 | `TOP[top.stream.open]`、`TOP[top.stream.fallback]`、`TOP[top.lx.health]`、`TOP[top.lxsvc]` |
| 换源后某平台反复失败 | `TOP[top.lx.health]`（平台冷却）、`TOP[top.stream.fallback]`（无损黑名单） |
| 无封面 / 封面闪烁 | `TOP[top.cover]`、`TOP[top.metadata]`（列表与播放页口径一致） |
| 无歌词 / 歌词不对 | `TOP[top.lyric]` |
| 星标/收藏不同步到其他设备 | `TOP[top.fav]`、`TOP[top.fav.delete]`、`TOP[top.ceru.op]`、`TOP[top.ceru.fav]` |
| 添加到歌单没反应 | `TOP[top.playlist.write]`、`TOP[top.imp.add]`、`TOP[top.ceru.op]` |
| 歌单列表显示异常（注入卡片） | `TOP[top.playlist.view]`、`TOP[top.imp]`、`TOP[top.recommend]`、`TOP[top.nm]` |
| 多端同步服务器连接/状态问题 | `TOP[top.ceru]`、`TOP[top.ceru.status]` |
| 洛雪源脚本行为（版本/更新） | `TOP[top.lxsvc.runtime]`、`TOP[top.webui.ui]`（源列表版本号） |
| 播放页元数据与列表不一致（星/封面闪） | `TOP[top.metadata]` |
| 音质偏好不生效 | `TOP[top.quality]`、`TOP[top.env]` |
| WebUI 打不开 / 设置不生效 | `TOP[top.webui]`、`TOP[top.webui.ui]`、`TOP[top.env]`、`TOP[top.env.merge]` |
| 内网外网显示不一致（缓存） | `TOP[top.env]`（热重载）、`TOP[top.cache.gc]`、`TOP[top.guid]` |
| 边听边存 / 下载不完整 | `TOP[top.tee]` |
| 标准音质/转码 | `TOP[top.transcode]` |
| 版本 / 缓存键 / 打包 | `TOP[top.version]`、`TOP[top.fpk]` |
| 升级后功能消失（数据根迁移） | `TOP[top.env.merge]`、install.sh `fnmusic_migrate_prev_home` |

## 模块清单

| 目录/文件 | 职责 |
| --- | --- |
| `proxy/app.py` | 主代理（约 1.1 万行）：所有音乐 API 接管、搜索/播放/收藏/歌单/封面/歌词/Tee |
| `proxy/cerusync.py` | 多端同步（ceru 服务器协议、LWW operation、guid 注册表） |
| `proxy/playlist_import.py` | 导入歌单（洛雪 JSON + 五平台链接 → 本地 JSON） |
| `proxy/recommend.py` | 热门/每日推荐歌单 |
| `proxy/nmplaylists.py` | 网易账号歌单只读注入 |
| `proxy/takeover.py` | socket 接管守护（宿主机 systemd，fail-closed） |
| `proxy/webui_gateway.py` | 飞牛桌面网关 → 本机 WebUI 转发 |
| `proxy/env_merge.py` / `proxy/version.py` | .env 安全合并 / 版本号唯一来源 |
| `proxy/transcode.py` / `proxy/cache_gc.py` | 标准音质转码 / 缓存 GC |
| `lxmusic-service/` | 洛雪音源服务（内置平台接口 + 自定义源 Node 沙箱运行时） |
| `musicbox-service/` | 网易云（musicbox 封装 + 扫码登录） |
| `musicdl-service/` | 57 平台聚合解析 |
| `webui-service/` | 插件管理页（NAS 管理窗口，静态资源带 ?v=版本 缓存键） |
| `packaging/fpk/build.sh` | FPK 打包入口 |
| `install.sh` / `extend.sh` / `restore.sh` | 安装 / 升级 / 还原官方直连 |

## 标签索引（自动生成）

<!-- TOPMAP:BEGIN -->
> 本段由 `tools/update_top_map.py` 自动生成，勿手改。
> 定位代码：`grep -n 'TOP[<标签>]' <文件>`，位置列已给出精确行号。

| 标签 | 知识点 | 位置 |
| --- | --- | --- |
| `top.cache.gc` | 缓存 GC：在线媒体/歌词/封面缓存清理 | `proxy/cache_gc.py:1` |
| `top.ceru` | 多端同步核心：同步服务器协议、歌单/收藏写回、guid 注册表、多级缓存 | `proxy/cerusync.py:1` |
| `top.ceru.fav` | /favorites 独立收藏存储写入（不与「我的喜欢」歌单联动，需配合 op） | `proxy/cerusync.py:505` |
| `top.ceru.op` | 歌单曲目 LWW 增删：operationId/deviceId/sequence，服务器广播给其他设备 | `proxy/cerusync.py:588` |
| `top.ceru.registry` | guid→歌曲注册表（内存态，重启即空；靠平台主键/歌名+歌手兜底） | `proxy/cerusync.py:725` |
| `top.ceru.status` | 多端同步状态接口（WebUI 同步页数据源） | `proxy/app.py:3848` |
| `top.cover` | 封面下载/内嵌/缓存与清理（负面缓存、内网地址判定、歌单回补） | `proxy/app.py:1209` |
| `top.env` | .env 热重载：2s 轮询，白名单键免重启生效 | `proxy/app.py:598` |
| `top.env.merge` | .env 安全合并：保留用户已有值、新键自动补齐（升级幂等） | `proxy/env_merge.py:1` |
| `top.fav` | 收藏创建：本地多用户隔离 + ceru 多端写回（未注册歌按主键合成） | `proxy/app.py:7980` |
| `top.fav.delete` | 取消收藏：主键/身份找回 → operation remove + /favorites DELETE | `proxy/app.py:8079` |
| `top.forward` | 官方 API 透传：未接管的请求原样转发官方后端（Unix socket） | `proxy/app.py:1872` |
| `top.fpk` | FPK 打包：组装应用骨架 + 仓库主体 → fnpack build → dist | `packaging/fpk/build.sh:3` |
| `top.guid` | 在线 guid 伪装/反查：客户端只见 32 位官方 id，服务端保持注册表 | `proxy/app.py:3016` |
| `top.imp` | 导入歌单：洛雪导出 JSON + 五平台公开歌单链接 → 本地 JSON → 注入音乐页 | `proxy/playlist_import.py:1` |
| `top.imp.add` | 导入歌单加歌：本地 JSON 按平台主键追加，查重幂等 | `proxy/app.py:8656` |
| `top.lx.health` | 洛雪源健康度：成功/失败统计、平台冷却、替代源元数据 | `proxy/app.py:2357` |
| `top.lx.prefetch` | 播放地址后台预取 + 下一曲预热（压低首播延迟） | `proxy/app.py:2605` |
| `top.lxsvc` | 洛雪 HTTP 服务：ID 契约 lx:<source>:<id>，搜索/歌词/封面内置、播放走源运行时 | `lxmusic-service/app.py:1` |
| `top.lxsvc.runtime` | 洛雪自定义源运行时：脚本校验 + Node 沙箱 + musicUrl 解析（含源版本号） | `lxmusic-service/source_runtime.py:1` |
| `top.lyric` | 歌词解析与回补：音源歌词 → 本地缓存 → musicdl 兜底 | `proxy/app.py:1583` |
| `top.metadata` | 播放页元数据组装：isFavorite/coverId 与列表同口径（防星标闪/封面闪） | `proxy/app.py:3475` |
| `top.musicbox` | 网易云：musicbox CLI 封装 + 扫码登录（VIP/无损、每日推荐） | `musicbox-service/app.py:1` |
| `top.musicdl` | musicdl：57 平台聚合解析（ID 契约 <source>:<id>） | `musicdl-service/app.py:1` |
| `top.nm` | 网易账号歌单只读注入（虚拟歌单，不回写网易） | `proxy/nmplaylists.py:1` |
| `top.playlist.view` | 歌单列表/曲目视图：官方歌单 + ceru/导入/推荐/网易注入 | `proxy/app.py:9804` |
| `top.playlist.write` | 歌单加歌/删歌：官方透传 + 本地桶 + ceru LWW + 导入 JSON | `proxy/app.py:8725` |
| `top.proxy` | 代理进程生命周期：启动自检、env 热重载循环、后台清扫任务 | `proxy/app.py:3652` |
| `top.quality` | 音质偏好（高音质/平衡/流畅）对质量梯的排序规则 | `proxy/app.py:2720` |
| `top.recommend` | 热门/每日推荐歌单：音源原生优先、LLM 兜底、本地随机兜底 | `proxy/recommend.py:1` |
| `top.search` | 搜索聚合主入口：本地曲库优先 + 音源窗口分页 + 可播过滤 | `proxy/app.py:3876` |
| `top.search.cache` | 搜索防抖与结果缓存（按用户×关键词 TTL，本地优先口径） | `proxy/app.py:481` |
| `top.stream.fallback` | 换源恢复：主源失败按健康度换替代源（无损黑名单防反复横跳） | `proxy/app.py:4671` |
| `top.stream.open` | 在线播放开流主链路：本地缓存 → 取址 → 直连/代理 → Tee | `proxy/app.py:4710` |
| `top.stream.resolve` | 洛雪播放地址解析：质量梯选择、死链降档、失败冷却 | `proxy/app.py:2820` |
| `top.takeover` | Socket 接管守护：抢官方 socket、故障回滚、子进程管理（fail-closed） | `proxy/takeover.py:2` |
| `top.tee` | 边听边存：Tee 落盘、收尾入库、封面内嵌、自动下载歌词 | `proxy/app.py:4454` |
| `top.transcode` | 标准音质转码：ffmpeg fMP4 HLS 会话与生命周期 | `proxy/transcode.py:1` |
| `top.version` | 版本号唯一来源：仓库根 VERSION 文件 | `proxy/version.py:1` |
| `top.wan.direct` | 外网直连：探测通过后 302 到音源 CDN（端侧直拉，NAS 只解地址） | `proxy/app.py:6199` |
| `top.webui` | 飞牛桌面网关 socket → 本机 WebUI(8774) 转发 + host-file 例外端点 | `proxy/webui_gateway.py:2` |
| `top.webui.ui` | 插件管理页前端：音源切换、洛雪源(每源版本号)、同步、音质设置 | `webui-service/static/app.js:1` |
<!-- TOPMAP:END -->

## 标签规则

- 格式：`# TOP[标签] 一句话说明`（JS 用 `//`），放在关键函数上方或模块头部第一行。
- 命名：`top.<域>.<子项>`，全小写字母数字与点；一个知识点一个标签，不重复挂。
- **每做完一个功能/修复，必须**：
  1. 若触及新知识点，补 `TOP[标签]` 标记；
  2. 运行 `python3 tools/update_top_map.py` 重新生成本文件索引；
  3. 若新增典型症状，在「症状 → 标签速查」加一行。
- FPK 打包（`packaging/fpk/build.sh`）会自动执行第 2 步，保证交付物与索引一致。
