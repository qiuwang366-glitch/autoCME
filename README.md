# CME 数据自动化系统

## 项目简介

这是一个完整的 CME（芝加哥商品交易所）金融数据自动化系统，包含两个阶段：

**阶段一：数据下载** - 从 CME 官网自动下载金融数据报告
**阶段二：ETL 数据清洗与入库** - 解析报告并存入数据库

适用于 macOS (Apple Silicon) 环境，支持定时任务，为后续数据分析奠定基础。

### 系统架构

```
下载 (main.py) → 文件存储 → ETL 解析 (etl_main.py) → SQLite 数据库 → 数据分析
```

### 主要特性

**阶段一：数据下载**
- ✅ 自动下载 5 类 CME 金融数据报告（PDF/XLS）
- ✅ 智能文件重命名（添加日期前缀，便于归档）
- ✅ 完整的日志记录和错误处理
- ✅ 反爬虫机制（User-Agent、重试策略）
- ✅ 支持 crontab 定时任务
- ✅ 幂等性设计（避免重复下载）

**阶段二：ETL 数据处理**（新增）
- ✅ 自动解析 CSV/XLS 库存报告（提取 Activity Date、仓库数据）
- ✅ 自动解析 PDF 交割通知（处理分块表格、合约信息提取）
- ✅ 结构化存储到 SQLite 数据库
- ✅ 数字清洗（处理 "1,234.56" 格式）
- ✅ 文件处理日志（避免重复处理）
- ✅ 可选文件归档功能

### 下载的文件

| 序号 | 文件名称 | 类型 | 说明 |
|------|---------|------|------|
| 1 | Metal Delivery Notices (Daily) | PDF | 每日金属交割通知 |
| 2 | Metal Delivery Notices (Monthly) | PDF | 月度金属交割通知 |
| 3 | Metal Delivery Notices (Year-To-Date) | PDF | 年度至今金属交割通知 |
| 4 | Gold Stocks | XLS/CSV | 黄金库存数据 |
| 5 | Silver Stocks | XLS/CSV | 白银库存数据 |

---

## 系统要求

- **操作系统**: macOS (Apple Silicon M1/M2/M4 或 Intel)
- **Python 版本**: Python 3.8 或更高
- **依赖包**: requests, beautifulsoup4, lxml（详见 requirements.txt）

---

## 安装指南

### 方法一：自动安装（推荐）

```bash
# 1. 克隆或下载项目到本地
cd ~/Downloads
git clone <repository_url> autoCME
cd autoCME

# 2. 运行安装脚本
chmod +x install.sh
./install.sh
```

安装脚本会自动完成：
- 检查 Python 环境
- 创建虚拟环境
- 安装依赖包
- 验证配置
- 测试运行

### 方法二：手动安装

```bash
# 1. 创建项目目录
mkdir -p ~/autoCME
cd ~/autoCME

# 2. 解压文件到此目录

# 3. 安装依赖
pip3 install -r requirements.txt

# 4. 设置执行权限
chmod +x run.sh
chmod +x main.py

# 5. 验证配置
python3 main.py --validate
```

---

## 配置说明

### 1. 修改保存路径

打开配置文件：

```bash
nano src/config.py
```

修改 `DATA_ROOT` 为你的实际路径：

```python
# 默认路径
DATA_ROOT = Path("/Users/liulu/Downloads/同步空间/30_Quant_Lab/01_Data_Warehouse/External_Feeds_外部数据源/cme")

# 如果你的用户名不是 liulu，请修改为实际路径
# 例如：DATA_ROOT = Path("/Users/你的用户名/Documents/cme_data")
```

### 2. 其他可配置项

在 `src/config.py` 中可以调整：

- **下载策略**: `DUPLICATE_STRATEGY` - 文件重复时的处理方式（skip/overwrite）
- **请求超时**: `REQUEST_TIMEOUT` - HTTP 请求超时时间
- **重试次数**: `MAX_RETRIES` - 下载失败时的重试次数
- **日志级别**: `LOG_LEVEL` - 日志详细程度（DEBUG/INFO/WARNING/ERROR）

---

## 使用方法

### 手动执行

```bash
# 进入项目目录
cd ~/autoCME

# 执行下载（完整输出）
python3 main.py

# 执行下载（静默模式）
python3 main.py --quiet

# 测试模式（仅解析链接，不下载）
python3 main.py --test

# 验证配置
python3 main.py --validate
```

### 使用启动脚本

```bash
./run.sh
```

---

## Crontab 定时任务配置

### 1. 编辑 run.sh 脚本

在配置 crontab 之前，请确认 `run.sh` 中的 Python 路径正确：

```bash
# 查看 Python 路径
which python3

# 常见路径：
# Homebrew (Intel): /usr/local/bin/python3
# Homebrew (Apple Silicon): /opt/homebrew/bin/python3
# 系统自带: /usr/bin/python3
```

编辑 `run.sh`，修改 `PYTHON` 变量为实际路径：

```bash
nano run.sh
```

### 2. 配置 Crontab

每天北京时间 11:00 执行（假设系统时区为本地时间）：

```bash
# 编辑 crontab
crontab -e

# 添加以下行（注意：将路径改为实际路径）
0 11 * * * /Users/liulu/autoCME/run.sh

# 保存并退出（vim: 按 ESC, 输入 :wq 回车）
```

**Crontab 时间格式说明：**

```
# ┌───────────── 分钟 (0 - 59)
# │ ┌───────────── 小时 (0 - 23)
# │ │ ┌───────────── 日期 (1 - 31)
# │ │ │ ┌───────────── 月份 (1 - 12)
# │ │ │ │ ┌───────────── 星期 (0 - 7, 0和7都代表星期日)
# │ │ │ │ │
# * * * * * 要执行的命令

0 11 * * *    # 每天 11:00
0 9,11 * * *  # 每天 9:00 和 11:00
0 11 * * 1-5  # 工作日 11:00
```

### 3. 验证 Crontab

```bash
# 查看当前 crontab
crontab -l

# 查看 cron 日志（macOS）
grep CRON /var/log/system.log

# 查看脚本日志
tail -f ~/autoCME/logs/cme_downloader.log
tail -f ~/autoCME/logs/cron.log
```

---

## macOS 权限设置

由于 macOS 的安全机制，cron 需要特定权限才能访问文件系统和执行脚本。

### 1. 授予 Full Disk Access 权限

1. 打开 **系统设置（System Settings）**
2. 进入 **隐私与安全性（Privacy & Security）**
3. 点击 **完全磁盘访问权限（Full Disk Access）**
4. 点击左下角的 **锁图标**，输入密码解锁
5. 点击 **+** 按钮，添加以下程序：
   - `/usr/sbin/cron` （cron 守护进程）
   - `/bin/bash` （shell 脚本）
   - 如果使用 Terminal.app 测试，也添加 `/Applications/Utilities/Terminal.app`

### 2. 配置 Cron 守护进程

macOS 默认启用 cron，但可以验证：

```bash
# 检查 cron 服务状态
sudo launchctl list | grep cron

# 如果没有运行，启动 cron
sudo launchctl load -w /System/Library/LaunchDaemons/com.vix.cron.plist
```

### 3. 测试权限

```bash
# 手动执行脚本测试
./run.sh

# 设置一个临时的 crontab，2分钟后执行
# 例如现在是 14:30，设置 14:32 执行
crontab -e
# 添加: 32 14 * * * /Users/liulu/autoCME/run.sh

# 等待执行后检查日志
tail -f ~/autoCME/logs/cron.log
```

### 4. 常见权限问题

**问题**: 脚本无法写入百度网盘同步目录

**解决方案**:
1. 确保 cron 有 Full Disk Access 权限
2. 如果使用云同步盘（百度网盘、iCloud 等），确保同步软件也有权限
3. 测试：手动执行 `./run.sh`，如果成功但 cron 失败，说明是权限问题

**问题**: Python 找不到依赖包

**解决方案**:
1. 在 `run.sh` 中使用绝对路径指定 Python
2. 或者激活虚拟环境：`source ~/autoCME/venv/bin/activate`

---

## 项目结构

```
autoCME/
├── src/                    # 源代码目录
│   ├── __init__.py        # 包初始化
│   ├── config.py          # 配置文件
│   ├── cme_downloader.py  # 核心下载器
│   └── logger.py          # 日志模块
├── logs/                   # 日志目录
│   ├── cme_downloader.log # 主日志文件
│   └── cron.log           # Cron 执行日志
├── data/                   # 数据目录（预留，用于数据库等）
├── main.py                 # 主执行脚本
├── run.sh                  # Crontab 启动脚本
├── install.sh              # 自动安装脚本
├── requirements.txt        # Python 依赖
├── README.md               # 本文档
└── LICENSE                 # 许可证
```

---

## 日志管理

### 日志文件

- **主日志**: `logs/cme_downloader.log` - 详细的下载日志
- **Cron 日志**: `logs/cron.log` - Crontab 执行状态记录

### 日志查看

```bash
# 查看最新日志
tail -f logs/cme_downloader.log

# 查看今天的日志
grep "$(date +%Y-%m-%d)" logs/cme_downloader.log

# 查看失败记录
grep "ERROR" logs/cme_downloader.log
grep "失败" logs/cme_downloader.log
```

### 日志轮转

日志文件会自动轮转，配置在 `src/config.py`:

- 单个日志文件最大 10MB
- 保留最近 5 个备份文件

---

## 数据库扩展（预留接口）

项目已预留数据库集成接口，便于后续建立数据仓库。

### 设计思路

1. **下载日志表** (`download_logs`):
   - 记录每次下载任务的元信息
   - 字段：日期、总文件数、成功数、失败数、耗时等

2. **文件元数据表** (`file_metadata`):
   - 记录每个文件的详细信息
   - 字段：文件ID、文件名、URL、本地路径、下载状态、时间戳等

### 扩展方法

在 `src/cme_downloader.py` 中，找到 `save_to_database()` 方法：

```python
def save_to_database(self, summary: Dict):
    """
    保存下载记录到数据库（预留接口）
    """
    # TODO: 实现数据库保存逻辑
    # 参考代码框架已在注释中提供
    pass
```

实现步骤：
1. 在 `requirements.txt` 中取消注释数据库驱动（如 SQLAlchemy）
2. 在 `src/config.py` 中配置数据库连接
3. 实现 `save_to_database()` 方法
4. 在 `main.py` 中取消注释数据库调用

示例（使用 SQLite）:

```python
import sqlite3

def save_to_database(self, summary: Dict):
    db_path = PROJECT_ROOT / "data" / "cme_data.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建表（如果不存在）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS download_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            download_date TEXT,
            total_files INTEGER,
            succeeded INTEGER,
            failed INTEGER,
            duration REAL,
            timestamp TEXT
        )
    ''')

    # 插入记录
    cursor.execute('''
        INSERT INTO download_logs
        (download_date, total_files, succeeded, failed, duration, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        self.download_date,
        summary['total'],
        summary['succeeded'],
        summary['failed'],
        summary['duration_seconds'],
        summary['end_time']
    ))

    conn.commit()
    conn.close()
```

---

## 故障排查

### 1. 下载失败

**症状**: 日志显示 "下载失败" 或 "未找到链接"

**排查步骤**:
1. 检查网络连接：`ping www.cmegroup.com`
2. 手动访问 CME 网站，确认页面结构没有改变
3. 运行测试模式查看解析结果：`python3 main.py --test`
4. 检查 User-Agent 是否被封禁（更新 `src/config.py` 中的 `USER_AGENT`）

### 2. 权限错误

**症状**: "Permission denied" 或无法写入文件

**排查步骤**:
1. 检查目标目录是否存在：`ls -la /Users/liulu/Downloads/...`
2. 检查目录权限：`stat -f "%A %N" /Users/liulu/Downloads/...`
3. 手动创建目录：`mkdir -p <目标路径>`
4. 检查 macOS Full Disk Access 权限设置

### 3. Crontab 不执行

**症状**: 手动执行成功，但定时任务不运行

**排查步骤**:
1. 确认 crontab 已保存：`crontab -l`
2. 检查 cron 服务状态：`sudo launchctl list | grep cron`
3. 检查系统日志：`grep CRON /var/log/system.log`
4. 验证脚本路径和 Python 路径是否为绝对路径
5. 检查 Full Disk Access 权限

### 4. Python 依赖问题

**症状**: "ModuleNotFoundError" 或 "ImportError"

**排查步骤**:
1. 确认依赖已安装：`pip3 list`
2. 重新安装依赖：`pip3 install -r requirements.txt`
3. 检查 Python 路径：`which python3`
4. 如果使用虚拟环境，确认已激活

---

## 维护建议

1. **定期检查日志**：每周查看一次 `logs/cme_downloader.log`
2. **监控磁盘空间**：数据文件会累积，定期清理旧文件
3. **更新依赖**：每季度运行 `pip3 install --upgrade -r requirements.txt`
4. **测试运行**：修改配置后，先运行 `python3 main.py --test` 验证

---

## 常见问题 FAQ

**Q: 可以修改下载时间吗？**
A: 可以，编辑 crontab 修改时间。例如改为每天 09:00：`0 9 * * * /path/to/run.sh`

**Q: 文件重复下载怎么办？**
A: 在 `src/config.py` 中，`DUPLICATE_STRATEGY` 设置为 "skip" 可跳过已存在文件。

**Q: 如何添加下载更多文件？**
A: 在 `src/config.py` 的 `DOWNLOAD_FILES` 列表中添加新的文件配置。

**Q: 支持 Windows 或 Linux 吗？**
A: 代码兼容跨平台，但安装和 crontab 配置需要相应调整。

**Q: CME 网站改版了怎么办？**
A: 运行 `python3 main.py --test` 查看解析结果，可能需要调整关键词匹配逻辑。

**Q: 可以发送邮件通知吗？**
A: 可以在 `main.py` 中添加邮件发送功能，或使用系统 cron 邮件通知。

---

## 技术支持

- **问题反馈**: 请在 GitHub 创建 Issue
- **功能建议**: 欢迎提交 Pull Request
- **文档更新**: 2024-01-13

---

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

---

## ETL 数据处理（第二阶段）

### 快速开始

完成下载系统安装后，开始使用 ETL 功能：

1. **安装 ETL 依赖**
```bash
# 已包含在 requirements.txt 中
pip3 install pandas openpyxl pdfplumber
```

2. **手动运行 ETL**
```bash
# 处理所有未处理的文件
python3 etl_main.py

# 重新处理所有文件
python3 etl_main.py --reprocess

# 处理后归档文件
python3 etl_main.py --archive

# 查看统计信息
python3 etl_main.py --stats
```

3. **配置自动化 ETL**
```bash
# 给脚本添加执行权限
chmod +x etl_run.sh

# 编辑 crontab
crontab -e

# 添加定时任务（下载完成 30 分钟后执行 ETL）
0 11 * * * /Users/liulu/autoCME/run.sh          # 下载数据
30 11 * * * /Users/liulu/autoCME/etl_run.sh     # ETL 处理
```

4. **查询数据**
```bash
# 使用 SQLite 命令行
sqlite3 data/cme_data.db

# 查询黄金库存
SELECT activity_date, SUM(total) as total_stock
FROM inventory_history
WHERE product = 'Gold'
GROUP BY activity_date
ORDER BY activity_date DESC
LIMIT 10;

# 查询交割量
SELECT intent_date, product, SUM(daily_total) as total_delivery
FROM delivery_notices
WHERE report_type = 'Daily'
GROUP BY intent_date, product
ORDER BY intent_date DESC
LIMIT 10;
```

### 数据库结构

**inventory_history（库存历史表）**
- 主键：(activity_date, product, depository)
- 字段：registered（注册仓单）、eligible（有效货源）、total（总库存）

**delivery_notices（交割通知表）**
- 主键：(intent_date, product, contract_month, report_type)
- 字段：daily_total（当日交割量）、cumulative（累计交割量）

**file_processing_log（文件处理日志）**
- 主键：file_path
- 用途：跟踪已处理文件，避免重复处理

### 详细文档

完整的 ETL 使用指南请参考：**[ETL_README.md](ETL_README.md)**

包含：
- 架构设计详解
- 解析器工作原理
- 数据库查询示例
- 故障排查指南
- 性能优化建议
- 扩展开发指南

---

## 更新日志

### v2.0.0 (2024-01-13) - ETL 功能
- 新增 ETL 数据处理模块
- 新增库存报告解析器（CSV/XLS）
- 新增交割通知解析器（PDF，使用 pdfplumber）
- 新增 SQLite 数据库管理模块
- 新增文件处理日志功能（幂等性）
- 新增文件归档功能
- 新增数据库查询 API
- 详细的 ETL 文档（ETL_README.md）

### v1.0.0 (2024-01-13) - 下载功能
- 初始版本发布
- 支持 5 类 CME 数据文件自动下载
- 完整的日志记录和错误处理
- 预留数据库接口
- 适配 macOS (Apple Silicon)

---

**祝使用愉快！如有问题请查看日志文件或联系技术支持。**
