# Report Workflow

## Deep Report Sequence

1. Quick filter.
2. Circle of competence.
3. Business quality.
4. Management and governance.
5. Balance sheet.
6. Cash flow statement.
7. Income statement.
8. Three-statement cross-check.
9. Valuation and margin of safety.
10. Risks, disconfirming signals, sell criteria.
11. Monitoring plan and next review date.

At the valuation step, force two separate judgments:

1. `Is this a good business?`
2. `Is the current price giving enough margin of safety?`

Do not merge them into one vague conclusion. A company can have a moat and still fail the price test.

## Evidence Standard

Every major conclusion should follow:

```text
Fact -> Reasoning -> Investment implication -> Disconfirming signal
```

Bad:

```text
The company is high quality.
```

Good:

```text
The company has maintained ROIC above X% while generating positive free cash flow for Y years. This suggests the growth is not purely accounting-driven. The conclusion weakens if receivable days rise materially or FCF turns negative during continued revenue growth.
```

For valuation, the evidence standard must include the actual math:

```text
Base-case intrinsic value is X.
Required margin of safety is Y% because moat is [strong/medium/weak], balance sheet is [strong/medium/weak], and key uncertainty is [A].
Therefore acceptable buy price is X × (1 - Y%).
Current price is above/below that level by Z%.
```

## Report Storage

Create a company folder under `investment-research/reports/`:

```text
代码-公司名/
```

Use dated filenames:

- `YYYY-MM-DD-init-深度分析.md`
- `YYYY-MM-DD-quarter-tracking.md`
- `YYYY-MM-DD-annual-review.md`
- `YYYY-MM-DD-event-事件名.md`

Maintain `thesis-log.md` for original assumptions and later status.
