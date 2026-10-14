#!/usr/bin/env python3
"""TOP 图自动维护：扫描代码里的 TOP[标签] 标记，重写 TOP.md 的自动生成段。

用法:
    python3 tools/update_top_map.py          # 重新生成 TOP.md 标签索引
    python3 tools/update_top_map.py --check  # 只校验（标签悬空/重复/缺失），不改文件

规则（见 AGENTS.md「Top 图与标签」）：
- 代码标记格式: `# TOP[tag] 一句话说明`（JS 用 //，位置在关键函数上方或模块头部）
- tag 命名: `top.<域>.<子项>`，全小写字母数字与点
- 每做完一个功能/修复必须重跑本脚本；FPK 打包时会自动执行
"""
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TOP_MD = REPO / "TOP.md"
BEGIN = "<!-- TOPMAP:BEGIN -->"
END = "<!-- TOPMAP:END -->"

# 扫描范围（与部署 payload 对齐；排除备份/测试缓存/旧版本快照）
SCAN_GLOBS = [
    "proxy/*.py",
    "lxmusic-service/*.py",
    "musicbox-service/*.py",
    "musicdl-service/*.py",
    "webui-service/static/*.js",
    "packaging/fpk/*.sh",
]

MARKER_RE = re.compile(r"^\s*(?:#|//)\s*TOP\[([a-z0-9]+(?:\.[a-z0-9]+)*)\]\s*(.*)$")
REF_RE = re.compile(r"TOP\[([a-z0-9]+(?:\.[a-z0-9]+)*)\]")


def scan_markers():
    hits = []  # (tag, desc, relpath, lineno)
    for pat in SCAN_GLOBS:
        for f in sorted(REPO.glob(pat)):
            try:
                lines = f.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeDecodeError):
                continue
            for i, line in enumerate(lines, 1):
                m = MARKER_RE.match(line)
                if m:
                    hits.append((m.group(1), m.group(2).strip(), f.relative_to(REPO).as_posix(), i))
    return hits


def render_table(hits):
    out = [
        "> 本段由 `tools/update_top_map.py` 自动生成，勿手改。",
        "> 定位代码：`grep -n 'TOP[<标签>]' <文件>`，位置列已给出精确行号。",
        "",
        "| 标签 | 知识点 | 位置 |",
        "| --- | --- | --- |",
    ]
    for tag, desc, rel, ln in sorted(hits, key=lambda h: (h[0].split(".", 1)[1], h[0])):
        out.append("| `%s` | %s | `%s:%d` |" % (tag, desc, rel, ln))
    return "\n".join(out) + "\n"


def main():
    check_only = "--check" in sys.argv
    hits = scan_markers()
    if not hits:
        print("ERROR: 未扫到任何 TOP[标签] 标记", file=sys.stderr)
        return 1

    by_tag = {}
    for tag, desc, rel, ln in hits:
        by_tag.setdefault(tag, []).append((desc, rel, ln))

    problems = []
    for tag, items in by_tag.items():
        descs = {d for d, _, _ in items}
        if len(items) > 1:
            problems.append("标签重复 %s: %s" % (tag, ", ".join("%s:%d" % (r, l) for _, r, l in items)))
        if len(descs) > 1:
            problems.append("标签 %s 描述不一致: %s" % (tag, descs))

    # 校验 TOP.md 手写段引用的标签是否都存在于代码
    if TOP_MD.exists():
        body = TOP_MD.read_text(encoding="utf-8")
        if BEGIN in body and END in body:
            hand = body.split(BEGIN)[0]
            for ref in sorted(set(REF_RE.findall(hand))):
                if ref not in by_tag:
                    problems.append("TOP.md 悬空引用: TOP[%s] 在代码里找不到标记" % ref)

    if check_only:
        if problems:
            print("TOP 标签校验失败：")
            for p in problems:
                print("  -", p)
            return 1
        print("TOP 标签校验通过：%d 个标记，%d 个标签" % (len(hits), len(by_tag)))
        return 0

    if not TOP_MD.exists():
        print("ERROR: 缺少 %s" % TOP_MD, file=sys.stderr)
        return 1
    body = TOP_MD.read_text(encoding="utf-8")
    if BEGIN not in body or END not in body:
        print("ERROR: TOP.md 缺少 TOPMAP:BEGIN/END 哨兵", file=sys.stderr)
        return 1
    head, rest = body.split(BEGIN, 1)
    tail = rest.split(END, 1)[1]
    new = head + BEGIN + "\n" + render_table(hits) + END + tail
    TOP_MD.write_text(new, encoding="utf-8")

    print("TOP.md 已更新：%d 个标记，%d 个标签" % (len(hits), len(by_tag)))
    for p in problems:
        print("  警告:", p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
