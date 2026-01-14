# CME PDF 报告格式说明

## 📋 报告类型对比

CME 提供三种交割通知报告，但格式**完全不同**：

| 报告类型 | 文件名标识 | 格式 | 是否支持 |
|---------|-----------|------|---------|
| Daily | `daily` | 汇总格式（TOTAL + MONTH TO DATE） | ✅ 支持 |
| Monthly | `monthly` 或 `mtd` | 汇总格式（TOTAL + MONTH TO DATE） | ✅ 支持 |
| Year-To-Date | `ytd` 或 `year` | 表格格式（按月明细） | ❌ 暂不支持 |

---

## 🔍 格式详解

### Daily / Monthly 报告格式（支持）

```
EXCHANGE: COMEX
CONTRACT: JANUARY 2026 ALUMINUM FUTURES
SETTLEMENT: 3,096.250000000 USD
INTENT DATE: 01/12/2026          DELIVERY DATE: 01/14/2026

FIRM  ORG  FIRM NAME                    ISSUED  STOPPED
────────────────────────────────────────────────────────
104   C    MIZUHO SECURITIES US                 14
167   U    MAREX                         4      1
690   C    ABN AMRO CLR USA LLC          10
737   C    ADVANTAGE FUTURES             1

                        TOTAL:            15     15
                  MONTH TO DATE:                134
```

**关键特征**：
- ✅ 有 `CONTRACT:` 行
- ✅ 有 `INTENT DATE:` 行
- ✅ 有 `TOTAL:` 汇总行（当日数据）
- ✅ 有 `MONTH TO DATE:` 累计行

**提取数据**：
```python
{
  'intent_date': '2026-01-12',
  'product': 'Aluminum',
  'contract_month': 'JANUARY 2026',
  'daily_total': 15,      # STOPPED 列的 TOTAL
  'cumulative': 134,      # MONTH TO DATE
  'report_type': 'Daily'
}
```

---

### YTD 报告格式（不支持）

```
EXCHANGE: COMEX
PRODUCT ALUMINUM FUTURES    ← 注意：不是 "CONTRACT:"
FIRM NBR
FIRM NAME O I/S PREV DEC | JAN | FEB | MAR | APR | MAY | JUN | JUL | AUG | SEP | OCT | NOV | DEC |

104 | | I | 0 | 66 |    ← 12个月的明细数据
    | |   |   |    |
MIZUHO SECURITIES US |C| S | 95 | 38 |
```

**关键特征**：
- ❌ 使用 `PRODUCT` 而非 `CONTRACT:`
- ❌ 没有 `INTENT DATE:`（因为是年度累计）
- ❌ 没有 `TOTAL:` 和 `MONTH TO DATE:`（是明细表格）
- ❌ 表格包含12个月的 Issued/Stopped 数据

**为什么不支持**：
1. 格式完全不同，需要单独的解析器
2. 需要解析复杂的12列表格
3. 数据价值较低（可从 Daily/Monthly 累计得出）

---

## ✅ 使用建议

### 1. 只下载 Daily 和 Monthly 报告

修改 `src/config.py` 中的 `DOWNLOAD_FILES`：

```python
DOWNLOAD_FILES = [
    {
        "id": "metal_delivery_daily",
        "name": "Metal Delivery Notices Daily",
        "keyword": "Daily",
        # ...
    },
    {
        "id": "metal_delivery_monthly",
        "name": "Metal Delivery Notices Monthly",
        "keyword": "Monthly",
        # ...
    },
    # 注释掉 YTD
    # {
    #     "id": "metal_delivery_ytd",
    #     "name": "Metal Delivery Notices YTD",
    #     "keyword": "Year-To-Date",
    #     # ...
    # },
]
```

### 2. ETL 自动跳过 YTD

已在 ETL 主程序中添加检查逻辑：

```python
if report_type == 'YTD':
    logger.warning("YTD 报告格式不同，暂不支持解析，跳过")
    # 记录为 skipped 状态
```

### 3. 测试命令

```bash
# 测试 Daily 报告（应该成功）
python test_pdf_parser.py /path/to/daily_report.pdf

# 测试 Monthly 报告（应该成功）
python test_pdf_parser.py /path/to/monthly_report.pdf

# 测试 YTD 报告（会提示不支持）
python test_pdf_parser.py /path/to/ytd_report.pdf
```

---

## 🔧 验证步骤

### 步骤 1：测试 Daily 报告

```bash
python test_pdf_parser.py data/20260113_metal_delivery_daily_MetalsIssuesAndStopsReport.pdf
```

**预期输出**：
```
提取记录: Aluminum JANUARY 2026, Intent: 2026-01-12, Daily: 15, Cumulative: 134
提取记录: Gold JANUARY 2026, Intent: 2026-01-12, Daily: 52, Cumulative: 6735
...
解析结果: 共提取 4 条记录
```

### 步骤 2：运行 ETL

```bash
# 处理所有文件（会自动跳过 YTD）
python etl_main.py --reprocess

# 查看统计
python etl_main.py --stats
```

**预期输出**：
```
============================================================
处理文件: 20260113_metal_delivery_daily_MetalsIssuesAndStopsReport.pdf
============================================================
文件类型: 交割通知
报告类型: Daily
提取记录: Aluminum JANUARY 2026, Intent: 2026-01-12, Daily: 15, Cumulative: 134
成功插入 4 条交割记录
============================================================

============================================================
处理文件: 20260113_metal_delivery_ytd_MetalsIssuesAndStopsYTDReport.pdf
============================================================
文件类型: 交割通知
报告类型: YTD
YTD 报告格式不同，暂不支持解析，跳过
============================================================

ETL 处理完成
总文件数: 5 | 成功: 4 | 失败: 0 | 跳过: 1
```

### 步骤 3：查询数据库

```bash
sqlite3 data/cme_data.db

SELECT * FROM delivery_notices
WHERE report_type = 'Daily'
ORDER BY intent_date DESC
LIMIT 5;
```

---

## 📊 数据对比

### Daily vs Monthly vs YTD

| 数据类型 | Daily | Monthly | YTD |
|---------|-------|---------|-----|
| 时间范围 | 单日 | 月度累计 | 年度累计 |
| 数据粒度 | 汇总 | 汇总 | 明细（按公司） |
| Intent Date | ✅ 有 | ✅ 有 | ❌ 无 |
| Daily Total | ✅ 有 | ✅ 有 | ❌ 无 |
| Cumulative | ✅ 有 | ✅ 有 | ❌ 无（按月明细） |

**推荐策略**：
- **数据分析**：使用 Daily + Monthly 数据
- **历史趋势**：从 Daily 数据累计得出
- **YTD 数据**：如需要，从 Daily 数据聚合

---

## ❓ 常见问题

**Q: 为什么 YTD 提取 0 条记录？**
A: YTD 报告格式与 Daily/Monthly 完全不同，没有 `CONTRACT:`、`INTENT DATE:`、`TOTAL:` 等关键字段。

**Q: 可以解析 YTD 吗？**
A: 理论上可以，但需要单独开发解析器来处理12列表格数据。由于数据可从 Daily 累计得出，暂不支持。

**Q: 如何确认我的 Daily 报告能正常解析？**
A: 运行 `python test_pdf_parser.py <daily_pdf>` 查看输出。如果看到 "提取记录: ..." 说明成功。

**Q: 如何禁用 YTD 下载？**
A: 在 `src/config.py` 中注释掉 YTD 文件配置，或在下载后手动删除 YTD 文件。

---

## 🎯 快速行动清单

- [ ] 测试 Daily PDF: `python test_pdf_parser.py <daily_pdf_path>`
- [ ] 测试 Monthly PDF: `python test_pdf_parser.py <monthly_pdf_path>`
- [ ] 运行 ETL: `python etl_main.py --reprocess`
- [ ] 查看统计: `python etl_main.py --stats`
- [ ] 查询数据: `sqlite3 data/cme_data.db`
- [ ] （可选）禁用 YTD 下载

---

**最后更新**：2024-01-14
