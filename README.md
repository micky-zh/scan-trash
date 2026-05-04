# Value Research Export

一个本地股票研究表导出工具。

主流程保持简单：

1. 抓全量行情
2. 跳过项目内置筛选
3. 补充财务数据和财务比率
4. 导出 CSV
5. 在 Excel 里自己筛选

文档：

- [变更记录](docs/CHANGELOG.md)
- [待办事项](docs/TODO.md)
- [投资研究框架](investment-research/framework/README.md)

## 安装

```bash
uv sync
```

查看命令：

```bash
uv run vr --help
```

## 快速上手

日常只想导出 Excel 研究表，跑下面三个命令之一：

```bash
uv run vr hk
uv run vr us
uv run vr cn
```

这三个命令会生成最终研究表：

- `uv run vr hk`：导出港股研究表。
- `uv run vr us`：导出美股研究表。
- `uv run vr cn`：导出 A 股研究表。

只看单只股票时，也可以加 `--symbol`：

```bash
uv run vr cn --symbol 000001
uv run vr hk --symbol 00700
uv run vr us --symbol AAPL
```

想先把底层历史财报缓存到本地，跑 `financials`：

```bash
uv run vr financials --market hk
uv run vr financials --market us
uv run vr financials --market cn
```

只更新单只股票时，可以直接加 `--symbol`：

```bash
uv run vr financials --market cn --symbol 000001
uv run vr financials --market hk --symbol 00700
uv run vr financials --market us --symbol AAPL
```

如果上一次没跑完，想断点式补跑，使用 `--missing-only`：

```bash
uv run vr financials --market cn --missing-only
```

`hk/us/cn` 研究表命令不强制依赖 `financials`。如果本地已有 `financials` 缓存，会优先使用本地缓存补充财报字段和部分长期指标；如果没有缓存，会按原逻辑联网抓取。

想缓存原始公告和申报文件，跑 `filings`：

```bash
uv run vr filings --market cn --download
```

只更新单只股票时：

```bash
uv run vr filings --market cn --symbol 000001 --download
uv run vr filings --market hk --symbol 00700 --download
uv run vr filings --market us --symbol AAPL --download
```

目前 `filings` 支持：

- A 股一季报、半年报、三季报和年报。
- 港股一季报、半年报、三季报和年报。
- 美股 `10-K`、`10-Q`、`20-F`、`6-K`，默认只保存索引和主文档，不转 PDF。
- `filings` 默认串行，支持 `--workers`，建议先从小并发开始。

## 命令区别

| 命令 | 用途 | 输出 | 什么时候用 |
| --- | --- | --- | --- |
| `uv run vr hk` | 生成港股 Excel 研究表 | `data/processed/hk_research_view.csv` | 日常看港股、导出给 Excel 筛选 |
| `uv run vr us` | 生成美股 Excel 研究表 | `data/processed/us_research_view.csv` | 日常看美股、导出给 Excel 筛选 |
| `uv run vr cn` | 生成 A 股 Excel 研究表 | `data/processed/cn_research_view.csv` | 日常看 A 股、导出给 Excel 筛选 |
| `uv run vr financials --market hk/us/cn` | 缓存结构化历史财报 | `data/raw/financials/...` | 第一次初始化、财报季更新、中断后补跑 |
| `uv run vr financials --market cn --symbol 000001` | 更新单只股票结构化历史财报 | `data/raw/financials/...` | 只想补一只股票的财报缓存 |
| `uv run vr filings --market cn` | 缓存 A 股公告索引 | `data/raw/filings/cn/{代码}/index.csv` | 想保留原始公告清单时 |
| `uv run vr filings --market cn --download` | 下载 A 股公告原始文件 | `data/raw/filings/cn/{代码}/pdfs/*.pdf` | 想保存原始公告文件时 |
| `uv run vr filings --market cn --symbol 000001 --download` | 更新单只股票公告索引和原始文件 | `data/raw/filings/cn/{代码}/...` | 只想补一只股票的公告和附件 |
| `uv run vr filings --market hk` | 缓存港股公告索引 | `data/raw/filings/hk/{代码}/index.csv` | 想保留港股原始公告清单时 |
| `uv run vr filings --market hk --download` | 下载港股公告原始文件 | `data/raw/filings/hk/{代码}/pdfs/*.pdf` | 想保存港股原始公告文件时 |
| `uv run vr filings --market us` | 缓存美股申报索引 | `data/raw/filings/us/{代码}/index.csv` | 想保留 SEC 申报清单时 |
| `uv run vr filings --market us --download` | 下载美股索引和主文档 | `data/raw/filings/us/{代码}/index.csv`、`raw/index.json`、`raw/主文档` | 想保存 SEC 主文档时 |

核心区别：

- `vr hk/us/cn` 是最终结果命令，目标是生成可以直接打开的研究表 CSV。
- `vr financials` 是底层数据缓存命令，目标是把结构化财报数据按股票保存到本地。
- `vr filings` 是原始文件命令，目标是保存公告索引和主文档。
- `vr filings` 支持 `--workers`；默认值是 `1`，大批量时可小幅提速。
- A 股、港股和美股研究表会优先用本地 `financials` 缓存计算 3/5 年 CAGR 和 5 年稳定性指标；历史不够时会留空。

依赖关系：

- `vr hk/us/cn` 不强制依赖 `financials`。
- 如果本地已有 `financials` 缓存，`vr hk/us/cn` 会优先读取缓存。
- 如果本地没有缓存，`vr hk/us/cn` 会按原逻辑联网补充数据。
- `filings` 和 `financials` 互不依赖；一个保存原始公告/申报文件，一个保存结构化财务数据。

## 推荐工作流

第一次初始化：

```bash
uv sync
uv run vr financials --market cn --workers 3
uv run vr financials --market hk --workers 3
uv run vr financials --market us --workers 3
uv run vr cn
uv run vr hk
uv run vr us
```

说明：

- `financials` 第一次会很慢，因为要逐只股票缓存历史财报。
- 如果中途断了，下次不要从头检查，直接用 `--missing-only` 续跑。
- 研究表命令 `vr cn/hk/us` 会优先使用已经缓存好的财报数据。

日常更新研究表：

```bash
uv run vr cn
uv run vr hk
uv run vr us
```

说明：

- 日常只需要这三个命令。
- 行情会重新抓取。
- 财务增强字段优先走本地缓存。
- 输出 CSV 后，在 Excel 里筛选。

每周或阶段性补齐底层财报缓存：

```bash
uv run vr financials --market cn --missing-only --workers 3
uv run vr financials --market hk --missing-only --workers 3
uv run vr financials --market us --missing-only --workers 3
```

说明：

- `--missing-only` 只补缺失的财报文件；已经完整缓存的股票会跳过。
- 适合上一次全量任务没跑完，或者本地缺了某几张 statement 文件。
- 它不会为了检查新报告期而重跑已经完整缓存的股票。

财报季更新：

```bash
uv run vr financials --market cn --workers 3
uv run vr financials --market hk --workers 3
uv run vr financials --market us --workers 3
uv run vr cn --refresh-enrich
uv run vr hk --refresh-enrich
uv run vr us --refresh-enrich
```

说明：

- 财报发布后，不要用 `--missing-only`，否则已经完整缓存的股票会被跳过。
- 普通 `financials` 会重新检查各股票是否有新报告期，并只追加本地没有的新报告期。
- `--refresh-enrich` 会强制刷新研究表里的增强字段。

下载 A 股年报 PDF：

```bash
uv run vr filings --market cn --download
```

下载 A 股半年报、一季报、三季报和年报 PDF：

```bash
uv run vr filings --market cn --category all --download
```

中断后继续下载或补索引：

```bash
uv run vr financials --market cn --missing-only --workers 3
uv run vr filings --market cn --download
```

说明：

- `financials --missing-only` 用于结构化财报缓存续跑，只补缺失的财报文件。
- `filings --download` 会跳过已经存在的文件，只下载缺失文件。

## 个股深度分析

当你已经从研究表里筛出候选股，建议这样补齐分析材料：

```bash
uv run vr financials --market hk --symbol 00700
uv run vr hk --symbol 00700
uv run vr filings --market hk --symbol 00700 --download
```

说明：

- 先跑 `financials`，把结构化历史财报补齐，长期指标会更完整。
- 再跑 `hk/us/cn --symbol`，看最终研究表里的可比字段。
- 最后按需看 `filings`，用原始公告和主文档核对管理层表述、风险事项和一次性因素。
- 这三步不是硬依赖，但组合起来最适合做个股深度分析。

### 使用 investment-analysis skill

本仓库保留了一个可持续迭代的投资分析 skill 主版本：

- `investment-research-skill/`

本地 Codex 安装副本建议放在：

- `~/.codex/skills/investment-analysis/`

如果本地还没有安装，可以用非删除式复制同步：

```bash
mkdir -p ~/.codex/skills/investment-analysis/agents
mkdir -p ~/.codex/skills/investment-analysis/references
cp investment-research-skill/SKILL.md ~/.codex/skills/investment-analysis/SKILL.md
cp investment-research-skill/agents/openai.yaml ~/.codex/skills/investment-analysis/agents/openai.yaml
cp investment-research-skill/references/*.md ~/.codex/skills/investment-analysis/references/
```

使用前推荐先准备本地数据。以港股腾讯为例：

```bash
uv run vr financials --market hk --symbol 00700
uv run vr hk --symbol 00700
uv run vr filings --market hk --symbol 00700 --download
```

然后在 Codex 里发起分析：

```text
用 investment-analysis 分析 00700 腾讯控股，基于本地 financials 和 filings 数据，生成一份带日期的初始深度分析报告。
```

报告输出建议放在：

```text
investment-research/reports/00700-腾讯控股/YYYY-MM-DD-init-深度分析.md
```

注意：

- skill 本身不负责自动下载数据；它负责执行分析框架、生成报告、记录假设和后续跟踪。
- `financials` 提供结构化财务数据，适合算长期指标和财务比率。
- `filings` 提供原始公告和主文档，适合核对年报文字、附注、风险事项和管理层表述。
- 财报季要用普通 `financials` 检查新报告期，不要用 `--missing-only`。

## 黑名单

如果某家公司已经判断为能力圈外，或者业务、口径、风险特征暂时不想继续跟踪，可以手工加入黑名单。

```bash
uv run vr blacklist add --market hk --symbol 00700 --reason "业务太复杂" --name "腾讯控股"
uv run vr blacklist list
uv run vr blacklist remove --market hk --symbol 00700
```

说明：

- 黑名单保存在本地 `data/state/blacklist.csv`。
- 默认只打标，不过滤，研究 CSV 会自动增加 `黑名单` 和 `黑名单原因` 两列。
- `remove` 只是停用，不会删除历史记录，后续还可以重新启用。

## 常见问题

为什么 `financials` 全量任务很慢？

`financials` 会逐只股票请求上游接口，A 股、港股、美股加起来数量很多。项目默认还会限速，避免请求过快被上游临时限制。第一次全量缓存慢是正常的。

什么时候用 `--missing-only`？

当全量任务中断、机器休眠、网络断开，或者你只想补缺失的财报文件时使用：

```bash
uv run vr financials --market cn --missing-only
```

什么时候不要用 `--missing-only`？

财报刚发布、你想检查已缓存股票有没有新报告期时，不要用 `--missing-only`。它会跳过已经完整缓存的股票。财报季应该跑普通命令：

```bash
uv run vr financials --market cn
```

如果需要强制重新抓取并追加一份版本，用：

```bash
uv run vr financials --market cn --refresh
```

财报更新后我该跑哪个命令？

先更新底层财报缓存，再刷新研究表增强字段：

```bash
uv run vr financials --market cn --workers 3
uv run vr cn --refresh-enrich
```

港股和美股同理，把 `cn` 换成 `hk` 或 `us`。

上游接口失败是不是 bug？

不一定。AKShare 底层数据来自不同网站，网络波动、限流、单只股票缺数据都会导致失败。建议先降低并发，例如 `--workers 1` 或 `--workers 2`，再用 `--missing-only` 补跑失败或未完成的股票。

原始文件下载失败怎么办？

`filings --download` 会把失败原因写到 `下载状态`。老公告可能上游文件已失效，例如返回 404。再次运行同一命令会跳过已存在文件，并继续尝试缺失文件：

```bash
uv run vr filings --market cn --download
```

`financials` 和 `filings` 有什么关系？

二者互不依赖。`financials` 保存结构化财务数据，给研究表补字段；`filings` 保存公告索引和原始文件，保留原始数据。日常只看 Excel 研究表时，跑 `vr hk/us/cn` 即可。

为什么有些港股产品会被跳过，或者 `补充数据状态` 失败？

为了避免把明显不是普通经营公司的证券拉进财务分析流程，`vr hk` 和 `vr financials --market hk` 会默认跳过一批非经营主体，例如 ETF、杠杆/反向产品、权证、债券型产品和类似结构性证券。像 `南方两倍做多MSTR-U` 这类产品，本身就不适合按普通公司财报口径去算长期指标。

## 港股

日常运行：

```bash
uv run vr hk
```

输出文件：

- `data/processed/hk_research_view.csv`

财报、分红、公司资料更新后，强制刷新增强字段：

```bash
uv run vr hk --refresh-enrich
```

完整重跑：

```bash
uv run vr hk --refresh-all
```

港股会生成并使用：

- `data/raw/hk_spot_full.csv`
- `data/raw/hk_enriched_cache.csv`
- `data/processed/hk_research_view.csv`

## 美股

日常运行：

```bash
uv run vr us
```

输出文件：

- `data/processed/us_research_view.csv`

财报指标更新后，强制刷新增强字段：

```bash
uv run vr us --refresh-enrich
```

完整重跑：

```bash
uv run vr us --refresh-all
```

美股会生成并使用：

- `data/raw/us_spot_full.csv`
- `data/raw/us_enriched_cache.csv`
- `data/processed/us_research_view.csv`

## A 股

日常运行：

```bash
uv run vr cn
```

输出文件：

- `data/processed/cn_research_view.csv`

财报指标更新后，强制刷新增强字段：

```bash
uv run vr cn --refresh-enrich
```

完整重跑：

```bash
uv run vr cn --refresh-all
```

A 股会生成并使用：

- `data/raw/cn_spot_full.csv`
- `data/raw/cn_enriched_cache.csv`
- `data/processed/cn_research_view.csv`

## 财报本地缓存

`financials` 用来缓存底层历史财报，不直接生成最终研究表。

缓存后的数据会被 `uv run vr hk/us/cn` 优先读取；没有缓存时，研究表命令仍会联网补充数据。

缓存 A 股历史财务指标和财务摘要：

```bash
uv run vr financials
```

缓存港股三大报表：

```bash
uv run vr financials --market hk
```

缓存美股三大报表：

```bash
uv run vr financials --market us
```

美股全量行情里包含 ETF、ETN、基金、权证、Right、Unit、SPAC 等非普通公司证券；这些标的通常没有三大报表，命令会默认跳过。

三个市场一起跑：

```bash
uv run vr financials --market all
```

并发抓取：

```bash
uv run vr financials --market us --workers 3
uv run vr financials --market all --workers 3
```

`--workers` 默认是 `1`，最稳；建议先用 `2` 到 `4`，过高容易触发上游接口限流。

先试跑 5 只：

```bash
uv run vr financials --market cn --limit 5
```

强制重新抓取并追加保存：

```bash
uv run vr financials --refresh
```

断点式补跑，只补缺失的财报文件：

```bash
uv run vr financials --market cn --missing-only
uv run vr financials --market hk --missing-only
uv run vr financials --market us --missing-only
```

`--missing-only` 适合全量任务中断后继续跑；它会跳过已经完整缓存的股票，只补缺失的 statement 文件。如果你想检查新财报是否发布，不要用这个参数，直接跑普通 `financials` 或使用 `--refresh`。

默认会控制请求频率：

- 每只股票后暂停 `1.5` 秒
- 每 `10` 只额外暂停 `5` 秒

可以手动调整：

```bash
uv run vr financials --sleep-seconds 2 --batch-size 10 --batch-sleep-seconds 8
```

缓存文件会按股票拆开保存：

- `data/raw/financials/cn/indicators/000001.csv`
- `data/raw/financials/cn/abstracts/000001.csv`
- `data/raw/financials/hk/balance/00700.csv`
- `data/raw/financials/hk/income/00700.csv`
- `data/raw/financials/hk/cashflow/00700.csv`
- `data/raw/financials/us/balance/AAPL.csv`
- `data/raw/financials/us/income/AAPL.csv`
- `data/raw/financials/us/cashflow/AAPL.csv`

更新逻辑：

- 默认只追加本地没有的新报告期。
- 自动只保留最近 5 年的数据，旧报告期会被清理。
- `--refresh` 会重新抓取并覆盖同一报告期的最新版本。

## 财报公告和原始文件

`financials` 缓存结构化财务数据；`filings` 缓存原始公告索引和原始文件。

目前 `filings` 支持 A 股、港股和美股申报文件。
`filings` 现在也支持 `--workers`，默认是 `1`，建议先从小并发开始。

缓存单只股票年报公告索引：

```bash
uv run vr filings --market cn --symbol 000001
```

缓存单只股票半年报公告索引：

```bash
uv run vr filings --market cn --symbol 000001 --category 半年报
```

下载单只股票全部定期报告 PDF：

```bash
uv run vr filings --market cn --symbol 000001 --category all --download
```

缓存全量 A 股年报公告索引：

```bash
uv run vr filings --market cn
```

全量下载 A 股年报 PDF：

```bash
uv run vr filings --market cn --download
```

全量下载 A 股全部定期报告 PDF：

```bash
uv run vr filings --market cn --category all --download
```

缓存单只港股年报公告索引：

```bash
uv run vr filings --market hk --symbol 00700
```

下载单只港股全部定期报告 PDF：

```bash
uv run vr filings --market hk --symbol 00700 --category all --download
```

缓存单只美股年报主文档：

```bash
uv run vr filings --market us --symbol AAPL --download
```

缓存美股全部常见申报类型：

```bash
uv run vr filings --market us --category all --download
```

全量下载港股年报 PDF：

```bash
uv run vr filings --market hk --download
```

先试跑 3 只：

```bash
uv run vr filings --market cn --limit 3 --download
```

强制刷新：

```bash
uv run vr filings --market cn --refresh --download
```

输出文件：

- `data/raw/filings/cn/000001/index.csv`
- `data/raw/filings/cn/000001/pdfs/*.pdf`
- `data/raw/filings/hk/00700/index.csv`
- `data/raw/filings/hk/00700/pdfs/*.pdf`
- `data/raw/filings/us/AAPL/index.csv`
- `data/raw/filings/us/AAPL/raw/*`

更新逻辑：

- 默认按公告链接增量追加。
- `index.csv` 会保存 `公告分类`，可在 Excel 里按 `年报`、`半年报`、`一季报`、`三季报` 筛选。
- 自动只保留最近 5 年的公告，旧公告和对应原始文件会被清理。
- `--download` 会下载缺失的原始文件，并更新 `下载状态`。
- `--refresh` 会重新抓取并覆盖同一公告链接的最新版本。

## 字段

打开研究 CSV 时，可以先看这几类列：

- `代码`、`名称`：股票身份。
- `最新价`、`总市值`、`市盈率`、`市净率`、`市销率`、`市现率`、`PEG`：估值快筛。
- `营业收入`、`净利润`、`经营现金流净额`、`自由现金流`：业务和现金创造能力。
- `毛利率(%)`、`销售净利率(%)`、`净资产收益率(%)`：盈利质量。
- `流动比率`、`速动比率`、`资产负债率(%)`、`有息负债率(%)`：偿债和安全边际。
- `过去3年/5年 ... CAGR(%)`、`过去5年 ... 为正年数`：长期趋势和稳定性。
- `补充数据状态`：这只股票的增强字段是否抓取成功。
- `黑名单`、`黑名单原因`：是否被你手工标记为能力圈外，以及原因。

港股研究表重点字段：

- `代码`
- `名称`
- `所属行业`
- `最新价`
- `总市值`
- `成交额`
- `换手率`
- `市盈率-动态`
- `市净率`
- `股息率TTM(%)`
- `每股净资产(元)`
- `基本每股收益(元)`
- `每股经营现金流(元)`
- `股东权益回报率(%)`
- `净资产收益率(平均)(%)`
- `总资产收益率(%)`
- `投入资本回报率(%)`
- `毛利率(%)`
- `销售净利率(%)`
- `营业收入同比增长率(%)`
- `净利润同比增长率(%)`
- `EPS同比增长率(%)`
- `经营现金流净额`
- `资本开支`
- `自由现金流`
- `FCF/净利润`
- `每股自由现金流`
- `市销率`
- `市现率`
- `PEG`
- `流动比率`
- `速动比率`
- `净现比`
- `净负债/EBITDA`
- `净债务/EBITDA`
- `有息负债率(%)`
- `利息保障倍数`
- `应收账款周转率`
- `存货周转率`
- `总资产周转率`
- `过去3年营业总收入CAGR(%)`
- `过去5年营业总收入CAGR(%)`
- `过去3年净利润CAGR(%)`
- `过去5年净利润CAGR(%)`
- `过去5年净利润为正年数`
- `过去5年经营现金流为正年数`
- `过去5年经营现金流/净利润`
- `过去5年自由现金流为正年数`

美股研究表重点字段：

- `代码`
- `名称`
- `最新价`
- `总市值`
- `成交额`
- `换手率`
- `市盈率`
- `营业收入`
- `毛利`
- `归母净利润`
- `基本每股收益`
- `稀释每股收益`
- `毛利率(%)`
- `销售净利率(%)`
- `净资产收益率(平均)(%)`
- `总资产收益率(%)`
- `营业收入同比增长率(%)`
- `净利润同比增长率(%)`
- `EPS同比增长率(%)`
- `经营现金流净额`
- `资本开支`
- `自由现金流`
- `FCF/净利润`
- `每股自由现金流`
- `市销率`
- `市现率`
- `PEG`
- `流动比率`
- `速动比率`
- `经营现金流/流动负债`
- `资产负债率(%)`
- `权益比率(%)`
- `有息负债率(%)`
- `净债务/EBITDA`
- `应收账款周转率`
- `存货周转率`
- `总资产周转率`
- `过去3年营业收入CAGR(%)`
- `过去5年营业收入CAGR(%)`
- `过去3年归母净利润CAGR(%)`
- `过去5年归母净利润CAGR(%)`
- `过去5年归母净利润为正年数`
- `过去5年经营现金流为正年数`
- `过去5年经营现金流/净利润`
- `过去5年自由现金流为正年数`
- `报告期`
- `报告日期`
- `币种`

A 股研究表重点字段：

- `代码`
- `名称`
- `最新价`
- `总市值`
- `流通市值`
- `成交额`
- `换手率`
- `市盈率-动态`
- `市净率`
- `营业总收入`
- `净利润`
- `扣非净利润`
- `基本每股收益`
- `每股净资产`
- `每股经营现金流`
- `销售毛利率(%)`
- `销售净利率(%)`
- `净资产收益率(%)`
- `加权净资产收益率(%)`
- `总资产收益率(%)`
- `营业收入同比增长率(%)`
- `净利润同比增长率(%)`
- `EPS同比增长率(%)`
- `经营现金流净额`
- `市销率`
- `市现率`
- `PEG`
- `流动比率`
- `速动比率`
- `保守速动比率`
- `现金比率(%)`
- `利息支付倍数`
- `经营现金净流量与净利润的比率(%)`
- `资产负债率(%)`
- `产权比率`
- `应收账款周转率`
- `应收账款周转天数`
- `存货周转率`
- `存货周转天数`
- `总资产周转率`
- `报告期`
- `财务指标日期`

说明：

- A 股、港股和美股的长期指标依赖本地历史财报缓存；历史不够时会留空。

## 缓存逻辑

- 行情每次重新抓。
- 财务增强字段默认优先使用缓存。
- 缓存没有的股票才联网补充。
- `--refresh-enrich` 会强制重新拉财务增强字段。
- 全市场逐只补财务字段会比较慢，第二次会明显依赖缓存。

## 配置

默认配置：

- `configs/default.yaml`
- `configs/us.yaml`
- `configs/cn.yaml`

`vr hk`、`vr us` 和 `vr cn` 不依赖内置筛选规则；如果本地已有 `financials` 缓存，会优先使用缓存补充字段，没有缓存时再联网抓取。
