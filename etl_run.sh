#!/bin/bash
# CME 数据 ETL 处理启动脚本
# 适用于 macOS crontab 定时任务

# 获取脚本所在目录（绝对路径）
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Python 路径（请根据实际环境修改）
PYTHON="/usr/local/bin/python3"

# 如果使用 Homebrew 安装的 Python (Apple Silicon)
# PYTHON="/opt/homebrew/bin/python3"

# 如果使用虚拟环境
# source "$SCRIPT_DIR/venv/bin/activate"
# PYTHON="$SCRIPT_DIR/venv/bin/python"

# 设置工作目录
cd "$SCRIPT_DIR"

# 执行 ETL 处理
# --archive: 处理后归档文件
# --quiet: 静默模式，仅记录到日志文件
"$PYTHON" "$SCRIPT_DIR/etl_main.py" --archive --quiet

# 记录退出状态
EXIT_CODE=$?

# 创建 ETL 日志目录
mkdir -p "$SCRIPT_DIR/logs"

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL 处理成功" >> "$SCRIPT_DIR/logs/etl_cron.log"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL 处理失败 (退出码: $EXIT_CODE)" >> "$SCRIPT_DIR/logs/etl_cron.log"
fi

exit $EXIT_CODE
