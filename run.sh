#!/bin/bash
# CME 数据下载器启动脚本
# 适用于 macOS crontab 定时任务

# 获取脚本所在目录（绝对路径）
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 激活虚拟环境（如果使用）
# source "$SCRIPT_DIR/venv/bin/activate"

# 或者使用系统 Python3
# 确保使用绝对路径，避免 crontab 环境变量问题
PYTHON="/usr/local/bin/python3"

# 如果使用 Homebrew 安装的 Python
# PYTHON="/opt/homebrew/bin/python3"

# 设置工作目录
cd "$SCRIPT_DIR"

# 执行下载任务
# --quiet 选项使输出仅记录到日志文件
"$PYTHON" "$SCRIPT_DIR/main.py" --quiet

# 记录退出状态
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] CME 数据下载成功" >> "$SCRIPT_DIR/logs/cron.log"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] CME 数据下载失败 (退出码: $EXIT_CODE)" >> "$SCRIPT_DIR/logs/cron.log"
fi

exit $EXIT_CODE
