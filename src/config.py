#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置文件 - CME 数据下载器
包含所有可配置项，便于维护和扩展
"""

import os
from pathlib import Path

# ==================== 基础路径配置 ====================
# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据保存根目录（请根据实际环境修改）
# macOS 环境下的默认路径
DATA_ROOT = Path("/Users/liulu/Downloads/同步空间/30_Quant_Lab/01_Data_Warehouse/External_Feeds_外部数据源/cme")

# 日志文件路径
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "cme_downloader.log"

# ==================== CME 网站配置 ====================
# CME 主页面 URL
CME_BASE_URL = "https://www.cmegroup.com"
CME_DELIVERY_NOTICES_URL = "https://www.cmegroup.com/solutions/clearing/operations-and-deliveries/nymex-delivery-notices.html"

# ==================== 下载文件配置 ====================
# 文件定义：包含名称、关键词、文件类型等信息
# 结构设计支持后续数据库扩展（可以直接映射到数据库表）
DOWNLOAD_FILES = [
    {
        "id": "metal_delivery_daily",
        "name": "Metal Delivery Notices Daily",
        "keyword": "Daily",
        "section": "COMEX & NYMEX Metal Delivery Notices",
        "file_type": "pdf",
        "prefix": "metal_delivery_daily",
        "description": "每日金属交割通知"
    },
    {
        "id": "metal_delivery_monthly",
        "name": "Metal Delivery Notices Monthly",
        "keyword": "Monthly",
        "section": "COMEX & NYMEX Metal Delivery Notices",
        "file_type": "pdf",
        "prefix": "metal_delivery_monthly",
        "description": "月度金属交割通知"
    },
    {
        "id": "metal_delivery_ytd",
        "name": "Metal Delivery Notices YTD",
        "keyword": "Year-To-Date",
        "section": "COMEX & NYMEX Metal Delivery Notices",
        "file_type": "pdf",
        "prefix": "metal_delivery_ytd",
        "description": "年度至今金属交割通知"
    },
    {
        "id": "gold_stocks",
        "name": "Gold Stocks",
        "keyword": "Gold Stocks",
        "section": "Warehouse & Depository Stocks",
        "file_type": "xls",  # 可能是 xls 或 csv
        "prefix": "gold_stocks",
        "description": "黄金库存数据"
    },
    {
        "id": "silver_stocks",
        "name": "Silver Stocks",
        "keyword": "Silver Stocks",
        "section": "Warehouse & Depository Stocks",
        "file_type": "xls",  # 可能是 xls 或 csv
        "prefix": "silver_stocks",
        "description": "白银库存数据"
    }
]

# ==================== HTTP 请求配置 ====================
# User-Agent 配置（反爬虫）
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# 请求头
REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0"
}

# 请求超时设置（秒）
REQUEST_TIMEOUT = 30

# 重试配置
MAX_RETRIES = 3
RETRY_DELAY = 2  # 秒

# ==================== 文件处理配置 ====================
# 日期格式（用于文件名前缀）
DATE_FORMAT = "%Y%m%d"

# 文件名重复处理策略
# "skip": 跳过已存在的文件
# "overwrite": 覆盖已存在的文件
# "append": 添加序号（如 _1, _2）
DUPLICATE_STRATEGY = "overwrite"

# ==================== 日志配置 ====================
# 日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = "INFO"

# 日志格式
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 日志文件最大大小（字节）- 10MB
LOG_MAX_BYTES = 10 * 1024 * 1024

# 日志备份文件数量
LOG_BACKUP_COUNT = 5

# ==================== 数据库配置（预留接口）====================
# 是否启用数据库记录
ENABLE_DATABASE = False

# 数据库配置（示例，实际使用时需要修改）
DATABASE_CONFIG = {
    "type": "sqlite",  # 可选: sqlite, mysql, postgresql
    "sqlite": {
        "path": PROJECT_ROOT / "data" / "cme_data.db"
    },
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "",
        "database": "cme_data"
    },
    "postgresql": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "",
        "database": "cme_data"
    }
}

# 数据库表名（预留）
DB_TABLE_DOWNLOAD_LOG = "download_logs"
DB_TABLE_FILE_METADATA = "file_metadata"

# ==================== 其他配置 ====================
# 是否显示进度信息
SHOW_PROGRESS = True

# 是否进行文件完整性验证
VERIFY_FILE_INTEGRITY = True

# 最小文件大小（字节），小于此大小认为下载失败
MIN_FILE_SIZE = 1024  # 1KB


def validate_config():
    """
    验证配置是否有效
    用于在程序启动时检查配置项
    """
    errors = []

    # 检查必要的路径
    if not DATA_ROOT:
        errors.append("DATA_ROOT 未配置")

    # 检查 URL 配置
    if not CME_DELIVERY_NOTICES_URL:
        errors.append("CME_DELIVERY_NOTICES_URL 未配置")

    # 检查下载文件配置
    if not DOWNLOAD_FILES:
        errors.append("DOWNLOAD_FILES 配置为空")

    if errors:
        raise ValueError(f"配置验证失败: {', '.join(errors)}")

    return True


def get_download_file_by_id(file_id):
    """
    根据 ID 获取下载文件配置
    便于后续数据库查询和管理
    """
    for file_config in DOWNLOAD_FILES:
        if file_config["id"] == file_id:
            return file_config
    return None


def get_all_file_ids():
    """
    获取所有文件 ID 列表
    """
    return [file_config["id"] for file_config in DOWNLOAD_FILES]


if __name__ == "__main__":
    # 配置验证
    try:
        validate_config()
        print("✓ 配置验证通过")
        print(f"数据保存路径: {DATA_ROOT}")
        print(f"日志保存路径: {LOG_FILE}")
        print(f"配置文件数量: {len(DOWNLOAD_FILES)}")
    except ValueError as e:
        print(f"✗ {e}")
