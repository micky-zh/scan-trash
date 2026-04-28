# HK Value Screener

一个面向港股价值投资研究的本地筛选项目。

这个项目现在有两条筛选链路：

1. `实时快筛`
用途：先把明显垃圾、极低流动性、当前亏损的小票清掉。

2. `正式财务初筛`
用途：在快筛基础上，再结合本地财务指标，筛出更接近价值投资口径的候选池。

## 你先记住这件事

这个项目不是“直接在线筛选”。

它的工作方式是：

1. 先从实时接口抓港股行情
2. 保存到本地 CSV
3. 再用本地 CSV 跑筛选

所以：

- `fetch-hk-spot-full` 用的是实时接口
- `screen-hk-spot` 用的是本地行情 CSV
- `enrich-hk-screened` 用的是本地快筛 CSV，再逐只补充港股财务与分红数据
- `screen-financial-baseline` 用的是本地行情 CSV + 本地财务 CSV

## 第一次使用

先安装依赖：

```bash
uv sync
```

看有哪些命令：

```bash
uv run hkvs --help
```

看当前配置：

```bash
uv run hkvs show-config
```

## 最常用的两条链路

### 1. 实时快筛

先抓实时港股行情：

```bash
uv run hkvs fetch-hk-spot-full
```

这条命令会做什么：

1. 从东财港股实时接口抓数据
2. 保存到本地原始文件

生成的文件：

- [hk_spot_full.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_spot_full.csv)

这个文件是干嘛的：

- 它是“本地原始行情快照”
- 后续快筛和财务初筛都会用到它
- 里面包含 `代码`、`名称`、`最新价`、`成交额`、`市盈率-动态`、`市净率`、`总市值` 等字段

然后跑实时快筛：

```bash
uv run hkvs screen-hk-spot
```

这条命令会做什么：

1. 读取本地 [hk_spot_full.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_spot_full.csv)
2. 按快筛规则过滤
3. 导出快筛结果

生成的文件：

- [hk_screened.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_screened.csv)

这个文件是干嘛的：

- 它是“实时快筛候选池”
- 只做第一轮垃圾过滤
- 不是正式价值投资候选池

如果你想把这份快筛结果补得更全，再运行：

```bash
uv run hkvs enrich-hk-screened
```

这条命令会做什么：

1. 读取本地 [hk_screened.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_screened.csv)
2. 逐只调用港股财务指标和分红接口
3. 补充股息率、每股净资产、每股收益、每股经营现金流、派息率等字段
4. 把已补过的结果写入本地缓存
5. 导出增强版结果

生成的文件：

- [hk_screened_enriched.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_screened_enriched.csv)

这个文件是干嘛的：

- 它是“增强版快筛候选池”
- 更接近 Yahoo 财经摘要页的感觉
- 适合你人工翻阅、排序、继续研究

说明：

- 这条命令现在有进度条
- 第一次跑会慢一些，因为要逐只抓数据
- 第二次再跑时，会优先使用本地缓存，明显更快
- 每处理完一只股票就会写一次缓存，中断后可以尽量续跑
- 单只股票请求现在有超时控制，避免卡住整轮

缓存文件：

- [hk_screened_enriched_cache.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_screened_enriched_cache.csv)

### 2. 正式财务初筛

这条链路除了行情 CSV，还需要一份本地财务 CSV。

如果你只是先试跑一遍，先复制样例文件：

```bash
cp data/raw/hk_financial_indicators_sample.csv data/raw/hk_financial_indicators.csv
```

然后运行：

```bash
uv run hkvs screen-financial-baseline
```

这条命令会做什么：

1. 读取本地 [hk_spot_full.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_spot_full.csv)
2. 读取本地 [hk_financial_indicators.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_financial_indicators.csv)
3. 合并行情和财务字段
4. 按正式财务初筛规则过滤
5. 导出正式候选池

生成的文件：

- [hk_financial_indicators.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_financial_indicators.csv)
- [hk_financial_screened.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_financial_screened.csv)

这两个文件是干嘛的：

- `hk_financial_indicators.csv`
用途：你本地维护的财务指标输入表

- `hk_financial_screened.csv`
用途：正式财务初筛后的候选池

这份文件现在已经是中文展示列，不再混合英文规则字段。

一句话理解：

- `hk_screened.csv` 是快筛结果
- `hk_screened_enriched.csv` 是补全了更多财务与分红字段的快筛结果
- `hk_financial_screened.csv` 才更接近你真正要研究的候选池

## 你现在最应该怎么跑

如果你只是想先得到一份候选池，按这个顺序：

```bash
uv sync
uv run hkvs fetch-hk-spot-full
uv run hkvs screen-hk-spot
uv run hkvs enrich-hk-screened
cp data/raw/hk_financial_indicators_sample.csv data/raw/hk_financial_indicators.csv
uv run hkvs screen-financial-baseline
```

跑完后你主要看这两个结果文件：

- [hk_screened.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_screened.csv)
- [hk_screened_enriched.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_screened_enriched.csv)
- [hk_financial_screened.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_financial_screened.csv)

## 每个重要文件是干嘛的

### 配置文件

- [default.yaml](/Users/zhengfan.19/work/scan-trash/configs/default.yaml)
默认运行配置。定义原始 CSV、输出 CSV、规则文件路径。

- [conservative.yaml](/Users/zhengfan.19/work/scan-trash/configs/conservative.yaml)
更保守的一套输出路径配置。

### 规则文件

- [hk_spot_baseline.yaml](/Users/zhengfan.19/work/scan-trash/rules/screening/hk_spot_baseline.yaml)
实时快筛规则。

- [baseline.yaml](/Users/zhengfan.19/work/scan-trash/rules/screening/baseline.yaml)
正式财务初筛规则。

- [default.yaml](/Users/zhengfan.19/work/scan-trash/rules/blacklists/default.yaml)
黑名单。里面的代码会被默认跳过。

### 数据文件

- [hk_spot_full.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_spot_full.csv)
实时接口抓回来的本地行情原始表。

- [hk_screened.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_screened.csv)
实时快筛结果。

- [hk_screened_enriched.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_screened_enriched.csv)
补全了更多财务和分红字段的增强版快筛结果。

- [hk_financial_indicators_sample.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_financial_indicators_sample.csv)
财务指标样例模板。

- [hk_financial_indicators.csv](/Users/zhengfan.19/work/scan-trash/data/raw/hk_financial_indicators.csv)
你实际要维护的财务指标输入表。

- [hk_financial_screened.csv](/Users/zhengfan.19/work/scan-trash/data/processed/hk_financial_screened.csv)
正式财务初筛结果。

### 代码入口

- [cli.py](/Users/zhengfan.19/work/scan-trash/src/hk_value_screener/cli.py)
命令行入口。所有 `uv run hkvs ...` 都从这里进。

- [data_sources.py](/Users/zhengfan.19/work/scan-trash/src/hk_value_screener/data_sources.py)
负责抓实时港股行情、加载本地财务 CSV、合并数据。

- [rules.py](/Users/zhengfan.19/work/scan-trash/src/hk_value_screener/rules.py)
负责加载 YAML 规则并执行筛选。

## 筛选原则在哪里

长期原则：

- [principles.md](/Users/zhengfan.19/work/scan-trash/docs/principles.md)

经验教训：

- [lessons.md](/Users/zhengfan.19/work/scan-trash/docs/lessons.md)

变更记录：

- [change_log.md](/Users/zhengfan.19/work/scan-trash/docs/change_log.md)

真正机器执行的规则在这里：

- [hk_spot_baseline.yaml](/Users/zhengfan.19/work/scan-trash/rules/screening/hk_spot_baseline.yaml)
- [baseline.yaml](/Users/zhengfan.19/work/scan-trash/rules/screening/baseline.yaml)

## 常用命令

```bash
uv run hkvs --help
uv run hkvs show-config
uv run hkvs show-rules
uv run hkvs fetch-hk-spot-full
uv run hkvs screen-hk-spot
uv run hkvs enrich-hk-screened
uv run hkvs screen-financial-baseline
uv run hkvs show-blacklist
```

## 当前边界

当前项目已经能做：

1. 抓取港股实时行情到本地
2. 跑实时快筛
3. 读取本地财务 CSV
4. 跑正式财务初筛

当前还没做完：

1. 行业分层规则自动化
2. 拼音首字母排序
3. Excel 价值手册导出
4. SQLite 本地缓存
