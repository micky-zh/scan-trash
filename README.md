# HK Value Screener

一个面向港股价值投资研究的标准化 Python 项目。

目标不是做“低 PE 捡便宜货”脚本，而是建立一个可持续迭代的本地研究系统：

- 抓取港股股票池、行情、历史数据和财务指标
- 用可配置规则做第一轮快速过滤
- 保留规则、反例、复盘和经验教训
- 模拟“按字母翻阅”的巴菲特式广覆盖浏览

## 技术栈

- `uv` 管理环境、依赖和锁文件
- `pyproject.toml` 管理项目元数据
- `src/` 标准源码布局
- `Typer` 提供命令行入口
- `YAML` 管理可迭代筛选规则
- `CSV / Parquet / SQLite` 作为本地研究缓存的演进方向

## 快速开始

```bash
uv sync
uv run hkvs --help
uv run hkvs show-config
uv run hkvs show-rules
uv run hkvs scaffold-rules-note
uv run hkvs fetch-hk-spot-full
uv run hkvs screen-hk-spot
uv run hkvs show-blacklist
```

## 目录结构

```text
.
├── configs/
├── pyproject.toml
├── src/hk_value_screener/
├── tests/
├── docs/
│   ├── principles/
│   ├── playbooks/
│   ├── postmortems/
│   └── decisions/
├── rules/
│   ├── screening/
│   └── notes/
└── data/
    ├── raw/
    ├── processed/
    └── exports/
```

## 当前范围

第一阶段先把工程和规则体系搭稳：

- 标准项目结构
- 规则文件格式
- CLI 入口
- 本地筛选框架
- 经验教训目录

第二阶段再接入实际抓数：

- HKEX 官方证券列表
- AKShare 港股行情
- AKShare 港股财务指标
- 本地缓存和增量更新

## 最新进度

当前已经完成：

- 标准 `uv` Python 项目骨架
- CLI 基础命令
- 分层 YAML 配置入口
- 筛选准则、变更日志和更新模板
- 港股全字段现货数据源
- 黑名单机制
- 真实港股基础筛选命令
- 自动化测试通过

当前最重要的进展是：

项目已经绕过 `AKShare stock_hk_spot_em()` 对港股字段裁剪的问题，保留了价值筛选更需要的字段，包括：

- `换手率`
- `市盈率-动态`
- `市净率`
- `总市值`
- `流通市值`

当前还没完成：

- 行业分层筛选规则落地到代码
- 拼音首字母排序
- Excel “价值手册” 导出
- SQLite 本地缓存

详细状态见：

- `docs/project_status.md`

## 配置方式

现在开始，运行行为尽量走配置，不直接改代码。

默认配置：

- `configs/default.yaml`

示例：

```bash
uv run hkvs show-config
uv run hkvs fetch-hk-spot-full --config-file configs/default.yaml
uv run hkvs screen-hk-spot --config-file configs/default.yaml
uv run hkvs fetch-hk-spot-full --config-file configs/conservative.yaml
```

## 下次要做什么

下一步直接做：

1. 用真实港股全字段数据跑筛选。
2. 按拼音首字母排序。
3. 导出 Excel，多 sheet 模拟按字母翻阅。

## 研究原则

先过滤垃圾，再浏览便宜货，再深入理解业务。

具体规则和持续迭代记录见：

- `docs/principles/`
- `docs/playbooks/`
- `docs/postmortems/`
- `rules/screening/`
