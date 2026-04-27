# Project Status

## Last Updated

- Date: 2026-04-27
- Status: In Progress
- Phase: Data foundation complete, screening output pipeline pending

## Current Summary

项目已经完成标准化工程搭建，也已经打通第一条真实港股数据主干。

当前最重要的事实是：

1. 这已经不是空架子。
2. 港股全量现货数据可以真实抓取。
3. 规则体系和迭代文档已经建立。
4. 最终面向研究使用的“价值手册导出”和“真实筛选闭环”还没有完成。

## What Has Been Completed

### 1. Standard Python Project Foundation

已完成标准 `uv` 项目结构：

- `pyproject.toml`
- `uv.lock`
- `src/` 标准源码布局
- `tests/`
- CLI 入口

当前 CLI 已有这些基础命令：

- `uv run hkvs show-config`
- `uv run hkvs show-rules`
- `uv run hkvs run-sample-screen`
- `uv run hkvs scaffold-rules-note`
- `uv run hkvs fetch-hk-spot-full`
- `uv run hkvs show-blacklist`
- `uv run hkvs screen-hk-spot`

### 2. Rule System and Documentation

已完成规则与研究纪律的基础文件：

- `rules/screening/baseline.yaml`
- `docs/principles/screening_charter.md`
- `docs/principles/screening_change_log.md`
- `docs/principles/screening_update_template.md`
- `docs/playbooks/screening_workflow.md`
- `docs/playbooks/sector_screening_framework.md`
- `docs/playbooks/configuration_guide.md`

这部分已经明确了：

- 第一轮筛选的目标
- 什么是坏筛选
- 规则如何变更
- 教训如何记录
- 哪些行业需要例外处理

### 3. Full Hong Kong Spot Data Source

已完成自定义港股全字段数据源：

- `src/hk_value_screener/data_sources.py`

这一步是当前阶段最关键的技术进展。

原因是：

`AKShare stock_hk_spot_em()` 当前版本会裁掉一部分对价值筛选有用的列。

项目现在已经绕过这个限制，直接保留这些关键字段：

- `换手率`
- `市盈率-动态`
- `市净率`
- `总市值`
- `流通市值`
- `量比`
- `涨速`
- `60日涨跌幅`
- `年初至今涨跌幅`

### 4. Config Layer

已完成第一版分层 YAML 配置入口：

- `configs/default.yaml`
- `configs/conservative.yaml`

这意味着后续切换黑名单、输出路径和运行方式，不再需要改代码。

### 5. Real Screening Pipeline

已完成第一版真实港股基础筛选命令：

- `uv run hkvs screen-hk-spot`
- `rules/screening/hk_spot_baseline.yaml`

这一步已经把：

- 港股现货 CSV
- 黑名单
- 基础过滤规则
- `screened.csv` 导出

接成了一条真实工作流。

### 6. Verification

当前自动化测试已通过：

- `9 passed`

已验证的内容包括：

- 规则文件加载
- 样例筛选逻辑
- 港股全字段数据映射
- 黑名单加载和过滤
- 应用配置加载
- 真实港股基础过滤规则

## Current Gaps

以下关键能力还没完成：

1. 还没有按拼音首字母排序。
2. 还没有导出成 Excel “价值手册”。
3. 还没有建立 SQLite 本地缓存。
4. 还没有做银行、保险、周期股的行业例外规则。
5. 还没有做定时更新或增量抓取。
6. 基础过滤规则还没有根据行业进一步拆分。

## Next Steps

下一轮应该优先做这三件事：

1. 增加按拼音首字母排序与分组逻辑。
2. 导出 Excel，多 sheet 模拟 A-Z 翻阅。
3. 开始把行业分层规则接入真实候选池。

## After That

完成上面三步后，再推进：

1. SQLite 本地缓存
2. 历史行情接入
3. 财务指标接入
4. 行业特例规则
5. 规则效果复盘

## Practical Interpretation

如果按阶段划分，当前位置是：

- 阶段 1：工程底座，已完成
- 阶段 2：真实港股现货数据主干，已完成
- 阶段 3：研究输出物生成，进行中
- 阶段 4：研究闭环和长期迭代，未完成

## Decision For The Next Work Session

下一次进入开发时，不应该再继续讨论“数据从哪里来”，而应该直接实现：

- 真实筛选
- 拼音排序
- Excel 导出

这三项完成后，项目才算进入“可实际使用的第一版”。
