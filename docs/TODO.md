# TODO

## 当前目标

保持项目精简，围绕三类命令组织能力：

- `vr hk/us/cn`：生成最终研究表，给 Excel 使用。
- `vr financials`：缓存底层历史财务数据。
- `vr filings`：缓存原始公告索引和 PDF/申报文件。

原则：

- 默认增量追加，不删除旧数据。
- 研究表命令优先读本地缓存，没有缓存再联网抓取。
- 全量任务要支持中断后继续跑。
- 不恢复项目内置筛选逻辑，筛选交给 Excel。

## 已完成

- [x] 移除项目内置筛选主流程，研究表只导出最终数据。
- [x] 支持港股研究表：`uv run vr hk`。
- [x] 支持美股研究表：`uv run vr us`。
- [x] 支持 A 股研究表：`uv run vr cn`。
- [x] 支持港股历史三大报表缓存：`uv run vr financials --market hk`。
- [x] 支持美股历史三大报表缓存：`uv run vr financials --market us`。
- [x] 支持 A 股历史财务指标和财务摘要缓存：`uv run vr financials --market cn`。
- [x] 支持三个市场一起缓存：`uv run vr financials --market all`。
- [x] 支持财报缓存并发：`uv run vr financials --market cn --workers 3`。
- [x] 支持财报缓存断点式补跑：`uv run vr financials --market cn --missing-only`。
- [x] 修复美股非普通公司证券导致的财报缓存报错。
- [x] 修复美股财报接口 `NoneType` 报错。
- [x] 修复 A 股财报缓存参数传递错误。
- [x] 研究表命令已优先读取本地 `financials` 缓存；没有缓存时再联网抓取。
- [x] 已评估 A 股、港股、美股公告/PDF 数据源。
- [x] 已实现 A 股年报公告索引：`uv run vr filings --market cn`。
- [x] 已实现 A 股年报 PDF 下载：`uv run vr filings --market cn --download`。
- [x] A 股年报下载进度条已统一为项目自己的 Rich 进度条。
- [x] README 已补充中文命令说明。
- [x] 删除未使用的空 `rules/` 目录。
- [x] `.gitignore` 已忽略编辑器临时文件 `*.swp`。

## 下一步计划：代码和数据能力

- [ ] 扩展 `filings --market cn` 支持半年报、一季报、三季报。
- [ ] 实现港股公告/PDF 缓存：
  - 优先评估港交所披露平台公告搜索。
  - 目标输出 `data/raw/filings/hk/{代码}/index.csv`。
  - PDF 保存到 `data/raw/filings/hk/{代码}/pdfs/`。
- [ ] 实现美股 SEC EDGAR 申报文件缓存：
  - 建立 ticker 到 CIK 映射。
  - 拉取 `10-K`、`10-Q`、`20-F`、`6-K`。
  - 保存索引到 `data/raw/filings/us/{代码}/index.csv`。
  - 先保存官方 HTML/TXT/XBRL 原始文件，不强行转 PDF。
- [ ] 为 `filings` 增加 `--market all`。
- [ ] 为 `filings` 评估是否增加 `--workers`；默认仍应保守，避免上游限流。
- [ ] 从本地历史财报缓存计算更多长期指标，例如 3/5 年收入 CAGR、净利润 CAGR、自由现金流稳定性。
- [ ] 给 `financials --missing-only` 增加更细的跳过策略，例如只要任一文件缺失才补跑。

## 下一步计划：文档和使用体验

- [ ] README 增加推荐工作流：
  - 第一次初始化怎么跑。
  - 每天/每周怎么跑。
  - 财报季怎么跑。
  - 中断后怎么继续跑。
- [ ] README 增加“命令区别”说明：
  - `vr hk/us/cn` 生成研究表。
  - `vr financials` 缓存结构化财务数据。
  - `vr filings` 缓存原始公告/PDF。
- [ ] README 增加常见问题：
  - 为什么全量任务很慢。
  - 什么时候用 `--missing-only`。
  - 什么时候不能用 `--missing-only`。
  - 上游接口失败如何处理。
- [ ] 增加示例输出字段说明，减少打开 CSV 后的理解成本。
- [ ] 清理历史 README 中不准确或重复的命令说明。

## 暂不做

- [ ] 不恢复项目内置股票筛选逻辑。
- [ ] 不把 PDF 内容解析成指标。
- [ ] 不做 AI 年报摘要。
- [ ] 不把公告/PDF 下载混入 `financials` 命令。
- [ ] 不强行把美股 SEC HTML/TXT 转成 PDF；先保存官方原始文件。

## 已知事项

- 本地 commit `88a6ae2 优化财报缓存并发和美股过滤` 已完成，但此前远程 push 因 SSH 连接关闭失败。
- 当前数据缓存目录在 `.gitignore` 中，不应该提交缓存 CSV 或 PDF。
