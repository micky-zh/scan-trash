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
uv run hkvs show-rules
uv run hkvs scaffold-rules-note
```

## 目录结构

```text
.
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

## 研究原则

先过滤垃圾，再浏览便宜货，再深入理解业务。

具体规则和持续迭代记录见：

- `docs/principles/`
- `docs/playbooks/`
- `docs/postmortems/`
- `rules/screening/`
