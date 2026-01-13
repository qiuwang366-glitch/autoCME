# CME 数据 ETL 系统文档

## 概述

这是 CME 数据自动化系统的第二阶段：**ETL（Extract-Transform-Load）数据清洗与入库**。

在第一阶段，我们实现了从 CME 官网自动下载数据报告的功能。本阶段实现了自动解析这些报告，并将结构化数据存入 SQLite 数据库，为后续的数据分析奠定基础。

---

## 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                   下载的 CME 报告文件                      │
│  (Gold Stocks, Silver Stocks, Delivery Notices PDF)    │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
         ┌────────────────┐
         │  ETL Main 主程序 │
         │  (etl_main.py)  │
         └────────┬────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
┌───────────────┐   ┌───────────────┐
│ InventoryParser│   │DeliveryNotice │
│   (CSV/XLS)    │   │  Parser (PDF) │
└───────┬────────┘   └────────┬──────┘
        │                     │
        └──────────┬──────────┘
                   ▼
          ┌────────────────┐
          │DatabaseManager │
          │   (SQLite)     │
          └────────┬────────┘
                   │
                   ▼
         ┌─────────────────┐
         │  cme_data.db    │
         │   数据库文件      │
         └─────────────────┘
```

---

## 核心模块

### 1. 数据库管理模块 (src/database.py)

**功能**：
- SQLite 数据库连接和事务管理
- 表结构创建和维护
- 数据插入和查询
- 文件处理日志记录

**数据库表结构**：

#### inventory_history（库存历史表）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| activity_date | DATE | 业务日期（主键之一） |
| product | VARCHAR(50) | 产品名称（Gold/Silver，主键之一） |
| depository | VARCHAR(200) | 仓库名称（主键之一） |
| registered | REAL | 注册仓单数量 |
| eligible | REAL | 有效货源数量 |
| total | REAL | 总库存 |
| unit | VARCHAR(50) | 单位（Troy Ounces） |
| report_date | DATE | 报告日期 |
| created_at | TIMESTAMP | 记录创建时间 |

**联合主键**：`(activity_date, product, depository)` - 确保同一天、同一产品、同一仓库只有一条记录。

#### delivery_notices（交割通知表）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| intent_date | DATE | 意向日期（主键之一） |
| product | VARCHAR(100) | 产品名称（主键之一） |
| contract_month | VARCHAR(50) | 合约月份（主键之一） |
| daily_total | INTEGER | 当日交割量 |
| cumulative | INTEGER | 累计交割量 |
| report_type | VARCHAR(20) | 报告类型（Daily/Monthly/YTD，主键之一） |
| source_file | VARCHAR(500) | 源文件名 |
| created_at | TIMESTAMP | 记录创建时间 |

**联合主键**：`(intent_date, product, contract_month, report_type)`

#### file_processing_log（文件处理日志表）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| file_path | VARCHAR(500) | 文件路径（主键） |
| file_name | VARCHAR(255) | 文件名 |
| file_type | VARCHAR(50) | 文件类型（csv/xls/pdf） |
| file_size | INTEGER | 文件大小（字节） |
| processed_at | TIMESTAMP | 处理时间 |
| status | VARCHAR(20) | 状态（success/failed/skipped） |
| records_inserted | INTEGER | 插入的记录数 |
| error_message | TEXT | 错误信息 |

**作用**：跟踪已处理的文件，实现幂等性（避免重复处理）。

---

### 2. 数据解析器模块 (src/parsers.py)

#### InventoryParser（库存报告解析器）

**处理文件**：
- `Gold_Stocks_YYYYMMDD.csv`
- `Silver_Stocks_YYYYMMDD.xls`

**解析难点与解决方案**：

1. **元数据提取**
   - **问题**：文件头部有 7-8 行元数据，不是标准 CSV 表头
   - **解决**：读取前 15 行，使用正则表达式查找 "Activity Date"

2. **日期字段选择**
   - **关键**：必须使用 "Activity Date"（业务日期），而非 "Report Date"（通常晚一天）
   - **实现**：优先匹配 "Activity Date"，如果找不到才使用 "Report Date"

3. **数字清洗**
   - **问题**：数字可能包含逗号（如 "1,234.56"）
   - **解决**：`clean_numeric_string()` 方法移除逗号和空格

4. **表头定位**
   - **问题**：真正的表头在第 7-12 行之间
   - **解决**：尝试不同的 `skiprows` 值，查找包含 "Depository" 的行

**核心方法**：
```python
# 提取元数据
metadata = parser._extract_metadata(file_path)
# {'activity_date': '2024-01-13', 'unit': 'Troy Ounces'}

# 读取数据表
df = parser._read_data_table(file_path)

# 转换为记录
records = parser._convert_to_records(df, product='Gold', metadata=metadata)
```

#### DeliveryNoticeParser（交割通知解析器）

**处理文件**：
- `Metal_Delivery_Notices_Daily.pdf`
- `Metal_Delivery_Notices_Monthly.pdf`
- `Metal_Delivery_Notices_YTD.pdf`

**解析难点与解决方案**：

1. **PDF 表格分块问题**
   - **问题**：PDF 中表格是分块的，每个合约一个表格
   - **表现**：先出现 "CONTRACT: JANUARY 2026 COMEX 100 GOLD FUTURES"，然后是数据表
   - **解决方案**：
     1. 提取页面文本，使用正则表达式识别所有 "CONTRACT:" 行
     2. 解析合约信息（月份、年份、产品）
     3. 提取页面表格
     4. 将合约信息与表格数据关联

2. **合约信息提取**
   - **正则表达式**：`r'CONTRACT:\s*([A-Z]+)\s+(\d{4})\s+COMEX\s+\d+\s+([A-Z]+)'`
   - **提取内容**：JANUARY 2026, GOLD

3. **表格数据解析**
   - 使用 `pdfplumber` 的 `extract_tables()` 方法
   - 识别表头（Intent Date, Daily Total, Cumulative）
   - 解析每一行数据

**核心方法**：
```python
# 打开 PDF
with pdfplumber.open(file_path) as pdf:
    for page in pdf.pages:
        # 提取合约信息
        contracts = parser._extract_contracts(page.extract_text())

        # 提取表格
        tables = page.extract_tables()

        # 关联合约与表格
        records = parser._parse_table(table, contract_info)
```

---

### 3. ETL 主程序 (etl_main.py)

**功能流程**：

1. **扫描文件**：遍历数据目录，找到所有 CSV、XLS、PDF 文件
2. **过滤已处理文件**：查询数据库，跳过已处理的文件（可选）
3. **分类文件**：根据文件名判断类型（库存/交割）
4. **调用解析器**：调用相应的解析器处理文件
5. **数据入库**：将解析结果插入数据库
6. **记录日志**：记录处理状态到 `file_processing_log` 表
7. **归档文件**（可选）：将处理完的文件移动到 `data/archive/` 目录

**文件分类逻辑**：
```python
if 'stock' in filename:
    type = 'inventory'
elif 'delivery' in filename:
    type = 'delivery'
elif extension in ['.csv', '.xls', '.xlsx']:
    type = 'inventory'
elif extension == '.pdf':
    type = 'delivery'
```

---

## 使用指南

### 1. 安装依赖

```bash
# 更新依赖包
pip install -r requirements.txt

# 主要新增依赖：
# - pandas: 数据处理
# - openpyxl: Excel 文件支持
# - pdfplumber: PDF 解析
```

### 2. 基本使用

#### 处理所有未处理的文件

```bash
python etl_main.py
```

输出示例：
```
2024-01-13 14:30:00 - CME_ETL - INFO - 扫描数据目录: /Users/liulu/Downloads/...
2024-01-13 14:30:00 - CME_ETL - INFO - 找到 3 个文件待处理
============================================================
处理文件: 20240113_gold_stocks_data.csv
============================================================
2024-01-13 14:30:01 - CME_ETL - INFO - 文件类型: 库存报告
2024-01-13 14:30:01 - CME_ETL - INFO - 提取到 Activity Date: 2024-01-12
2024-01-13 14:30:01 - CME_ETL - INFO - 找到数据表（skiprows=7）
2024-01-13 14:30:02 - CME_ETL - INFO - 成功解析 25 条记录
2024-01-13 14:30:02 - CME_ETL - INFO - 成功插入/更新 25 条库存记录
============================================================
ETL 处理完成
总文件数: 3
成功: 3
失败: 0
============================================================
```

#### 重新处理所有文件（包括已处理的）

```bash
python etl_main.py --reprocess
```

#### 处理后归档文件

```bash
python etl_main.py --archive
```

归档结构：
```
data/archive/
├── 2024-01/
│   ├── 20240113_gold_stocks_data.csv
│   ├── 20240113_silver_stocks_data.csv
│   └── 20240113_metal_delivery_daily.pdf
└── 2024-02/
    └── ...
```

#### 显示数据库统计信息

```bash
python etl_main.py --stats
```

输出示例：
```
============================================================
数据库统计信息
============================================================
已处理文件总数: 15
  - 成功: 14
  - 失败: 1
库存记录总数: 350
交割记录总数: 128
============================================================
```

#### 指定数据目录

```bash
python etl_main.py --data-dir /path/to/data
```

### 3. 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--data-dir` | 数据文件目录 | 从 config.py 读取 |
| `--db-path` | 数据库文件路径 | `data/cme_data.db` |
| `--archive-dir` | 归档目录 | `data/archive` |
| `--reprocess` | 重新处理已处理的文件 | False |
| `--archive` | 处理后归档文件 | False |
| `--stats` | 仅显示统计信息 | False |
| `--quiet` | 静默模式 | False |

---

## 数据查询示例

### 使用 Python API

```python
from pathlib import Path
from src.database import DatabaseManager

# 初始化数据库管理器
db = DatabaseManager(Path("data/cme_data.db"))

# 查询黄金库存（最近 30 天）
gold_inventory = db.get_inventory_summary(
    product='Gold',
    start_date='2024-01-01',
    end_date='2024-01-31'
)

# 查询交割通知
delivery_data = db.get_delivery_summary(
    product='Gold',
    report_type='Daily'
)

# 查看统计信息
stats = db.get_processing_stats()
print(f"库存记录: {stats['inventory_records']}")
print(f"交割记录: {stats['delivery_records']}")
```

### 使用 SQL 直接查询

```bash
# 打开数据库
sqlite3 data/cme_data.db

# 查询黄金库存总量（按日期分组）
SELECT activity_date, SUM(total) as total_stock
FROM inventory_history
WHERE product = 'Gold'
GROUP BY activity_date
ORDER BY activity_date DESC
LIMIT 10;

# 查询特定仓库的库存变化
SELECT activity_date, depository, registered, eligible, total
FROM inventory_history
WHERE product = 'Gold' AND depository LIKE '%BRINK%'
ORDER BY activity_date DESC;

# 查询交割量趋势
SELECT intent_date, product, SUM(daily_total) as total_delivery
FROM delivery_notices
WHERE report_type = 'Daily'
GROUP BY intent_date, product
ORDER BY intent_date DESC
LIMIT 20;

# 查看处理失败的文件
SELECT file_name, error_message
FROM file_processing_log
WHERE status = 'failed';
```

---

## 集成到自动化流程

### 完整的自动化流程

1. **下载数据**（每天 11:00）
   ```bash
   /Users/liulu/autoCME/run.sh
   ```

2. **ETL 处理**（下载完成后）
   ```bash
   /Users/liulu/autoCME/etl_run.sh
   ```

### 创建 ETL 启动脚本

创建 `etl_run.sh`：
```bash
#!/bin/bash
# ETL 处理启动脚本

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PYTHON="/usr/local/bin/python3"  # 或 /opt/homebrew/bin/python3

cd "$SCRIPT_DIR"

# 执行 ETL 处理（归档已处理文件）
"$PYTHON" "$SCRIPT_DIR/etl_main.py" --archive --quiet

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL 处理成功" >> "$SCRIPT_DIR/logs/etl_cron.log"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL 处理失败 (退出码: $EXIT_CODE)" >> "$SCRIPT_DIR/logs/etl_cron.log"
fi

exit $EXIT_CODE
```

### 配置 Crontab

```bash
# 编辑 crontab
crontab -e

# 下载数据（每天 11:00）
0 11 * * * /Users/liulu/autoCME/run.sh

# ETL 处理（每天 11:30，给下载留 30 分钟）
30 11 * * * /Users/liulu/autoCME/etl_run.sh
```

---

## 故障排查

### 1. 解析失败

**症状**：日志显示 "未提取到有效数据"

**排查步骤**：
```python
# 手动测试解析器
from src.parsers import InventoryParser
from src.logger import setup_logger

logger = setup_logger("Test")
parser = InventoryParser(logger)

# 测试单个文件
file_path = Path("/path/to/gold_stocks.csv")
records = parser.parse_file(file_path)

print(f"解析到 {len(records)} 条记录")
print(records[0])  # 查看第一条记录
```

### 2. Activity Date 提取失败

**可能原因**：
- 文件格式改变
- 日期格式不匹配

**解决方案**：
```python
# 检查元数据
metadata = parser._extract_metadata(file_path)
print(metadata)

# 如果日期格式不匹配，在 parsers.py 的 parse_date_string() 中添加新格式
```

### 3. PDF 表格提取失败

**可能原因**：
- PDF 格式特殊
- 表格结构改变

**调试方法**：
```python
import pdfplumber

with pdfplumber.open("delivery_notice.pdf") as pdf:
    page = pdf.pages[0]

    # 查看文本
    print(page.extract_text())

    # 查看表格
    tables = page.extract_tables()
    for i, table in enumerate(tables):
        print(f"\n表格 {i}:")
        for row in table:
            print(row)
```

### 4. 数据库锁定

**症状**："database is locked" 错误

**原因**：SQLite 不支持多进程并发写入

**解决方案**：
- 确保同一时间只有一个 ETL 进程运行
- 或者升级到 PostgreSQL/MySQL

---

## 性能优化

### 批量插入

数据库管理器已实现批量插入：
```python
# 一次插入多条记录
db.insert_inventory_records(records)  # 使用 executemany
```

### 避免重复处理

使用 `file_processing_log` 表跟踪已处理文件：
```python
if db.is_file_processed(file_path):
    print("文件已处理，跳过")
```

### 数据库索引

如需频繁查询，可添加索引：
```sql
CREATE INDEX idx_inventory_date ON inventory_history(activity_date);
CREATE INDEX idx_inventory_product ON inventory_history(product);
CREATE INDEX idx_delivery_date ON delivery_notices(intent_date);
```

---

## 扩展建议

### 1. 数据验证

添加数据质量检查：
```python
def validate_inventory_record(record):
    """验证库存记录的合理性"""
    # 检查总量是否等于注册+有效
    if record['total'] != record['registered'] + record['eligible']:
        logger.warning("库存数据不一致")

    # 检查数值是否为负
    if record['total'] < 0:
        logger.error("库存为负数")
```

### 2. 数据分析

基于数据库进行分析：
```python
import pandas as pd

# 从数据库读取数据
df = pd.read_sql(
    "SELECT * FROM inventory_history WHERE product = 'Gold'",
    con=db.get_connection()
)

# 计算日均变化
df['daily_change'] = df.groupby('depository')['total'].diff()

# 可视化
import matplotlib.pyplot as plt
df.plot(x='activity_date', y='total', kind='line')
```

### 3. 数据导出

导出为 Excel/CSV：
```python
# 导出库存历史
df = pd.read_sql("SELECT * FROM inventory_history", con=db.get_connection())
df.to_excel("inventory_export.xlsx", index=False)
```

---

## 常见问题 FAQ

**Q: 为什么使用 SQLite 而不是 MySQL？**
A: SQLite 轻量、无需服务器、适合个人数据仓库。如需多用户访问，可升级到 MySQL/PostgreSQL。

**Q: 如何处理 PDF 格式变化？**
A: 修改 `DeliveryNoticeParser` 中的正则表达式和表格解析逻辑。建议保留旧版 PDF 样本用于测试。

**Q: 数据库文件在哪里？**
A: 默认位置：`data/cme_data.db`，可通过 `--db-path` 参数指定。

**Q: 如何备份数据库？**
A:
```bash
# 简单备份
cp data/cme_data.db data/cme_data_backup.db

# 定期备份
sqlite3 data/cme_data.db ".backup data/cme_data_$(date +%Y%m%d).db"
```

**Q: 可以同时运行下载和 ETL 吗？**
A: 建议分开运行。下载完成后再运行 ETL，避免文件读写冲突。

---

## 更新日志

### v1.0.0 (2024-01-13)
- 实现库存报告解析器（CSV/XLS）
- 实现交割通知解析器（PDF）
- 实现数据库管理模块（SQLite）
- 实现 ETL 主程序
- 支持文件归档功能
- 支持幂等性处理

---

**祝数据分析愉快！如有问题请查看日志文件或联系技术支持。**
