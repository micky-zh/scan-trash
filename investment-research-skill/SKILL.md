---
name: investment-analysis
description: |
  Use this skill when analyzing stocks or public companies for real investment decisions, producing dated company research reports, reviewing financial statements, maintaining investment theses, tracking assumptions, updating an investment framework, or creating buy/hold/sell decision memos. It enforces a value-investing workflow: business quality first, balance sheet and cash flow before income statement, evidence-based valuation, margin of safety, explicit risks, sell criteria, dated tracking, and post-decision review.
---

# Investment Analysis Skill

This skill supports disciplined individual-stock research for real investment decisions. The goal is not to predict short-term prices. The goal is to produce evidence-backed, dated, reviewable investment judgments.

## Non-negotiable Rules

1. Do not give a buy/sell conclusion without evidence, assumptions, risks, and follow-up indicators.
2. Analyze business quality before valuation.
3. Analyze balance sheet and cash flow before relying on income statement profits.
4. Every important conclusion must include facts, reasoning, investment implication, and possible disconfirming evidence.
5. If the company is outside the user's circle of competence, say so and either stop or mark the conclusion as low confidence.
6. If reliable valuation is not possible, output `无法可靠估值` instead of forcing a precise target price.
7. Preserve dated records. Reports, tracking notes, and decision memos must include the date.
8. For single-stock analysis in this project, prefer local materials in this order: `financials` cache, local `filings-text` extracted text, local raw `filings`, and only then external search if material facts are still missing.

## Repository Layout

When working inside this project, use:

```text
investment-research/
├── framework/
├── reports/
├── templates/
├── logs/
└── TODO.md
```

Also use local source materials when available:

```text
data/raw/financials/{market}/...
data/raw/filings/{market}/{code}/texts/
data/raw/filings/{market}/{code}/pdfs/
data/raw/filings/{market}/{code}/raw/
```

For each company, create:

```text
investment-research/reports/股票代码-公司名/
├── YYYY-MM-DD-init-深度分析.md
├── YYYY-MM-DD-q1-tracking.md
└── thesis-log.md
```

## Workflow

### 1. Quick Filter

Before deep analysis, answer:

- Can the business model be explained in one paragraph?
- Is it inside the circle of competence?
- Will the business likely matter in 10 years?
- Does it have a durable competitive advantage?
- Does profit convert to cash?
- Can the balance sheet survive a severe downturn?
- Is management trustworthy?
- Is the price potentially reasonable?

If several answers are negative, recommend stopping or reducing analysis depth.

### 2. Business Quality

Read `references/report-workflow.md` and `references/checklist.md` when producing a full report.

Required outputs:

- Business model in one sentence.
- Industry structure.
- Moat type and trend.
- Pricing power.
- Customer stickiness.
- Substitution risk.
- Management and governance assessment.

### 3. Financial Statements

Read `references/financial-analysis.md` before detailed financial statement analysis.

Analyze in this order:

1. Balance sheet.
2. Cash flow statement.
3. Income statement.
4. Three-statement cross-checks.

Use multi-year data where available. When data is missing, state exactly what is missing and how it affects confidence.

For this repository, if `financials` cache is incomplete or suspicious, read local filing text extracted by `filings-text` before considering outside sources.

### 4. Valuation

Read `references/valuation.md` when estimating intrinsic value or buy price.

Required:

- Method choice and reason.
- Bear/base/bull scenarios.
- Key assumptions.
- Intrinsic value range.
- Acceptable buy price.
- Current margin of safety.
- Why the estimate may be wrong.

### 5. Risk, Sell Criteria, and Review

Read `references/review-loop.md` when producing a decision memo, tracking note, hold/sell review, or framework update.

Required:

- Top three risks.
- Disconfirming signals.
- Sell criteria.
- Monitoring indicators.
- Next review date.

## Standard Output

Use this structure for full reports:

```markdown
# 个股深度分析报告

日期：
公司：
代码：
市场：

## 1. 结论
## 2. 能力圈判断
## 3. 核心投资假设
## 4. 生意质量
## 5. 管理层与治理
## 6. 财务分析
## 7. 估值
## 8. 风险与反证
## 9. 跟踪指标
## 10. 决策记录
```

Allowed final decisions:

- `买入`
- `等待价格`
- `继续观察`
- `放弃`
- `持有`
- `减仓`
- `卖出`

## Framework Updates

When the user asks to improve the framework, update:

- `investment-research/framework/99-待完善框架.md`
- `investment-research/logs/framework-changelog.md`
- `investment-research/TODO.md`, if an action item remains.

Do not silently change the investment process. Explain what changed and why.
