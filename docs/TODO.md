# TODO

## 当前目标

保持项目精简，围绕三类命令组织能力：

- `vr hk/us/cn`：生成最终研究表，给 Excel 使用。
- `vr financials`：缓存底层历史财务数据。
- `vr filings`：缓存原始公告索引和主文档。

原则：

- 默认增量追加，同时只保留最近 5 年的数据。
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
- [x] 支持本地黑名单 `add/list/remove`，研究表默认只打标不过滤。
- [x] 修复美股非普通公司证券导致的财报缓存报错。
- [x] 修复美股财报接口 `NoneType` 报错。
- [x] 修复 A 股财报缓存参数传递错误。
- [x] 研究表命令已优先读取本地 `financials` 缓存；没有缓存时再联网抓取。
- [x] 已评估 A 股、港股、美股公告/PDF 数据源。
- [x] 已实现 A 股年报公告索引：`uv run vr filings --market cn`。
- [x] 已实现 A 股年报 PDF 下载：`uv run vr filings --market cn --download`。
- [x] A 股 `filings` 已支持一季报、半年报、三季报和 `--category all`。
- [x] 已实现港股公告/PDF 缓存：`uv run vr filings --market hk --download`。
- [x] 已实现美股 SEC 主文档缓存：`uv run vr filings --market us --download`。
- [x] A 股年报下载进度条已统一为项目自己的 Rich 进度条。
- [x] `financials` 已支持单股更新：`uv run vr financials --market cn --symbol 000001`。
- [x] `hk/us/cn` 已支持单股分析：`uv run vr cn --symbol 000001`。
- [x] 财报 CSV、公告索引和原始文件已自动保留最近 5 年并清理旧文件。
- [x] README 已补充中文命令说明。
- [x] README 已增加推荐工作流：第一次初始化、日常更新、财报季更新、中断后续跑。
- [x] README 已增加命令区别说明：研究表、结构化财报缓存、原始公告/申报文件。
- [x] README 已增加常见问题：全量慢、`--missing-only`、财报更新、上游失败、文件失败。
- [x] 删除未使用的空 `rules/` 目录。
- [x] `.gitignore` 已忽略编辑器临时文件 `*.swp`。

## 下一步计划：代码和数据能力

- [ ] 为 `filings` 增加 `--market all`。
- [x] 为 `filings` 增加 `--workers`；默认仍应保守，避免上游限流。
- [x] 港股和美股已支持从本地历史财报缓存计算长期指标，例如 3/5 年收入 CAGR、净利润 CAGR、自由现金流稳定性。
- [ ] A 股补齐同类长期指标。
- [x] `financials --missing-only` 已支持按文件补缺，已完整缓存的股票会跳过。

## 下一步计划：文档和使用体验

- [x] 增加示例输出字段说明，减少打开 CSV 后的理解成本。
- [ ] 继续清理历史 README 中不准确或重复的命令说明。

## 暂不做

- [ ] 不恢复项目内置股票筛选逻辑。
- [ ] 不把 PDF 内容解析成指标。
- [ ] 不做 AI 年报摘要。
- [ ] 不把公告/PDF 下载混入 `financials` 命令。
- [ ] 不强行把美股 SEC HTML/TXT 转成 PDF；先保存官方原始文件。

## 已知事项

- 本地 commit `88a6ae2 优化财报缓存并发和美股过滤` 已完成，但此前远程 push 因 SSH 连接关闭失败。
- 当前数据缓存目录在 `.gitignore` 中，不应该提交缓存 CSV 或 PDF。
