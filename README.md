# Value Research Export

一个本地股票研究表导出工具。

主流程保持简单：

1. 抓全量行情
2. 跳过项目内置筛选
3. 补充财务数据和财务比率
4. 导出 CSV
5. 在 Excel 里自己筛选

## 安装

```bash
uv sync
```

查看命令：

```bash
uv run vr --help
```

## 命令怎么用

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

想先把底层历史财报缓存到本地，跑 `financials`：

```bash
uv run vr financials --market hk
uv run vr financials --market us
uv run vr financials --market cn
```

如果上一次没跑完，想断点式补跑，使用 `--missing-only`：

```bash
uv run vr financials --market cn --missing-only
```

`hk/us/cn` 研究表命令不强制依赖 `financials`。如果本地已有 `financials` 缓存，会优先使用本地缓存补充部分财报字段；如果没有缓存，会按原逻辑联网抓取。

想下载原始年报公告和 PDF，跑 `filings`：

```bash
uv run vr filings --market cn --download
```

目前 `filings` 先支持 A 股年报。

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

断点式补跑，只抓本地还没有缓存文件的股票：

```bash
uv run vr financials --market cn --missing-only
uv run vr financials --market hk --missing-only
uv run vr financials --market us --missing-only
```

`--missing-only` 适合全量任务中断后继续跑；如果你想检查新财报是否发布，不要用这个参数，直接跑普通 `financials` 或使用 `--refresh`。

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
- 不删除旧报告期。
- `--refresh` 会把重新抓到的数据追加进去，用 `抓取时间` 区分版本。

## 财报公告和 PDF

`financials` 缓存结构化财务数据；`filings` 缓存原始公告索引和 PDF 文件。

目前 `filings` 先支持 A 股年报。

缓存单只股票年报公告索引：

```bash
uv run vr filings --market cn --symbol 000001
```

下载单只股票年报 PDF：

```bash
uv run vr filings --market cn --symbol 000001 --download
```

缓存全量 A 股年报公告索引：

```bash
uv run vr filings --market cn
```

全量下载 A 股年报 PDF：

```bash
uv run vr filings --market cn --download
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

更新逻辑：

- 默认按公告链接增量追加。
- 不删除旧公告。
- `--download` 会下载缺失的 PDF，并更新 `下载状态`。
- `--refresh` 会重新抓取并追加一份新记录。

## 字段

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
