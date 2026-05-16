---
name: obsidian
description: Obsidian 知识库管理技能。用于创建和维护 Vortex 项目的 Obsidian vault——包括 wikilink 链接规范、YAML frontmatter 标准、MOC 索引页维护、Graph View 优化、标签体系管理。任何涉及 docs/ 目录下文档创建、编辑、链接维护的操作都需遵循本 skill。
author: vortex
version: 1.0.0
tags: [vortex, vortex/skill]
obsidian_links:
  - "[[README]]"
  - "[[软件架构总览]]"
  - "[[产品原型总览]]"
---

# Obsidian — Vortex 知识库管理技能

## What this skill is for

在 Vortex 项目中，`docs/` 目录被设计为 Obsidian vault。AI 在创建、编辑、重组文档时，必须遵守本 skill 定义的链接规范、元数据标准和结构约定，确保知识图谱保持清晰可导航。

本 skill 覆盖：
- 创建新文档时的 frontmatter 模板
- `[[wikilink]]` 的写法和命名约定
- MOC（Map of Content）索引页的维护规则
- 标签体系的使用规范
- Graph View 的结构优化
- 链接完整性验证

---

## 文档 Frontmatter 标准

每个 docs/ 下的 .md 文件头部必须包含以下 YAML frontmatter：

```yaml
---
tags: [vortex, <领域标签>]
aliases: [<中文全称>, <中文简称>, <英文名>]
created: YYYY-MM-DD
updated: YYYY-MM-DD
---
```

### 必填字段

| 字段 | 格式 | 说明 |
|------|------|------|
| `tags` | YAML list | 至少包含 `vortex`；按文档类型追加领域标签 |
| `aliases` | YAML list | **第一个**为 wikilink 匹配用中文全称，后续可加简称和英文名 |
| `created` | `YYYY-MM-DD` | 文档创建日期 |
| `updated` | `YYYY-MM-DD` | 最后修改日期（每次编辑需更新） |

### aliases 写入规则（重要）

aliases 的第一个值必须与文档在 wikilink 中的引用文本精确匹配。例如：

- 文件 `00-软件架构总览-v2.md` 的 aliases 首项 = `软件架构总览`
- 所有 `[[软件架构总览]]` 引用都能正确解析

**错误示例**：文档引用 `[[产品功能冻结文档]]`，但 aliases 只写了 `[产品功能冻结]` → 解析失败。

### 标签体系

```
#vortex                     — 根标签，所有文档必加
#vortex/moc                 — 索引页（如 README.md）
#vortex/architecture        — 架构设计文档（00-10 + 附录）
#vortex/product             — 产品原型文档
#vortex/frozen              — 已冻结文档
#vortex/data-domain         — 数据域相关
#vortex/research-domain     — 研究域相关
#vortex/strategy-domain     — 策略域相关
#vortex/trade-domain        — 交易域相关
#vortex/skill               — AI 技能文件
#vortex/user-manual         — 用户手册
```

标签决定了 Obsidian Graph View 中的节点颜色分组，也是 Dataview 查询的主要过滤条件。

---

## Wikilink 规范

### 基本格式

```
[[目标文档的别名首项]]
[[目标文档的别名首项|显示文本]]
```

### 命名约定

1. **优先使用 aliases 首项**作为链接目标，不用文件名
   - ✅ `[[软件架构总览]]`
   - ❌ `00-软件架构总览-v2.md`

2. **中文全称为主**，保持可读性
   - ✅ `[[数据域设计说明书]]`
   - ❌ `[[Data Domain Design]]`

3. **带别名显示**用于需要更短文本的场景
   - `[[数据域设计说明书|01-数据域]]`
   - `[[代码细节设计说明书|06-代码细节]]`

4. **章节引用**使用 `[[文档名#章节标题]]`
   - `[[用户手册#5-数据管理]]`
   - `[[软件架构总览#四-依赖规则与允许的调用方向]]`

5. **嵌入内容**使用 `![[文档名]]`
   - `![[产品功能冻结文档#三-Data 域动作规格]]`

### 什么时候必须加链接

| 场景 | 动作 |
|------|------|
| 引用其他架构设计文档 | 必须 `[[链接]]` |
| 引用产品原型文档 | 必须 `[[链接]]` |
| 引用用户手册 | 必须 `[[链接]]` |
| 引用外部 Skill 文件 | 尽可能 `[[链接]]`（跨 vault 时标注路径） |
| 引用代码文件 | 用 Markdown 链接 `[file](../path/to/file.py)` |

---

## MOC（Map of Content）索引页规范

MOC 是站在高处俯瞰知识结构的页面。Vortex 目前有 3 层 MOC：

```
docs/README.md                     — 顶层 MOC（全局入口）
├── docs/产品原型/README.md        — 产品原型索引
├── docs/产品原型/v0.2/00-产品原型总览.md  — 产品原型 MOC
└── docs/架构设计/00-软件架构总览-v2.md    — 架构设计 MOC
```

### MOC 页面必须包含

1. **一句话定位**：这个目录回答什么问题
2. **结构图**：mermaid 图或文本树
3. **分类索引**：按主题分组的文档列表（必须用 `[[wikilink]]`）
4. **标签索引**：列出该领域使用的标签
5. **快速入口**：最常见的 3-5 个入口链接

### MOC 维护规则

- 新增文档时，必须同步更新最近的 MOC 页面
- 删除文档时，必须从所有 MOC 中移除链接
- MOC 本身也必须有 frontmatter，标签包含 `#vortex/moc`

---

## Graph View 优化

为了让 Obsidian Graph View 呈现清晰的知识结构：

1. **中心枢纽突出**：让 [[软件架构总览]] 和 [[产品功能冻结文档]] 拥有最多的出链和入链
2. **星型结构**：MOC → 专题文档 → 附录/Skill，避免全连接
3. **标签着色**：在 Graph View 设置中按 `#vortex/architecture`、`#vortex/product` 等分组着色
4. **孤立节点检查**：每个文档至少有一条出链和一条入链

### Graph View 理想形态

```
        docs/README.md (顶层 MOC)
        /        |        \
   architecture  product   user-manual
   /    |    \      |    \
  00   01...10   00  06  07
  |
 appendices
```

---

## Dataview 查询（高级）

Obsidian 安装 Dataview 插件后，可在任意页面嵌入动态查询：

```dataview
TABLE aliases, tags, updated
FROM #vortex/architecture
SORT updated DESC
```

```dataview
LIST
FROM #vortex/frozen
```

```dataview
TABLE created, updated
FROM #vortex/skill
SORT created DESC
```

---

## 链接完整性验证

每次批量编辑文档后，运行以下验证：

```bash
# 1. 提取所有 wikilink 目标
cd docs/
grep -roh '\[\[[^]|]*' --include="*.md" . | sed 's/\[\[//' | sort -u > /tmp/links.txt

# 2. 检查每个链接是否有对应文件或 aliases
python3 << 'PY'
import os, re

# 收集所有文件的 aliases
aliases_map = {}
for root, _, files in os.walk('.'):
    for f in files:
        if f.endswith('.md'):
            content = open(os.path.join(root, f)).read()
            fm_match = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
            if fm_match:
                alias_match = re.search(r'aliases:\s*\[(.*?)\]', fm_match.group(1))
                if alias_match:
                    for a in alias_match.group(1).split(','):
                        a = a.strip().strip("'\"")
                        aliases_map[a] = f
            aliases_map[f.replace('.md', '')] = f

# 检查 wikilinks
for root, _, files in os.walk('.'):
    for f in files:
        if f.endswith('.md'):
            content = open(os.path.join(root, f)).read()
            links = re.findall(r'\[\[([^\]|]+)', content)
            for link in links:
                link = link.strip()
                if link not in aliases_map:
                    print(f'❌ {f}: [[{link}]] — unresolved')
PY
```

---

## 创建新文档的完整流程

```
1. 确定文档归属目录（架构设计 / 产品原型 / 根目录）
2. 按编号规范命名：{编号}-{中文短标题}-v{版本}.md
3. 添加标准 frontmatter（tags, aliases, created, updated）
4. 写入内容，对任何外部文档引用使用 [[wikilink]]
5. 更新相关 MOC 页面（顶层 + 领域层），添加新文档的 [[链接]]
6. 在关联的上下游文档中，添加反向 [[链接]]
7. 运行链接验证脚本，确保无断链
```

---

## 注意事项

1. **不要修改已冻结文档的核心内容**（标签 `#vortex/frozen` 标注的文档）
2. **aliases 追加不删除**：可以新增别名，不要删除已有别名（可能被其他文档引用）
3. **链接是方向的**：不是所有文档都要互相链接，遵循 `架构 → 域 → 附录` 的方向
4. **跨 vault 链接**：`.github/skills/` 和 `docs/` 不在同一 vault，Obsidian 中需单独打开或软链接
