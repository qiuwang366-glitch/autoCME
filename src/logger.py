#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志配置模块
提供统一的日志记录功能
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from config import (
    LOG_DIR,
    LOG_FILE,
    LOG_LEVEL,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    LOG_MAX_BYTES,
    LOG_BACKUP_COUNT
)


def setup_logger(name: str = "CMEDownloader", log_to_console: bool = True) -> logging.Logger:
    """
    配置并返回日志记录器

    Args:
        name: 日志记录器名称
        log_to_console: 是否同时输出到控制台

    Returns:
        配置好的 logger 对象
    """
    # 创建日志目录
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # 创建 logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))

    # 避免重复添加 handler
    if logger.handlers:
        return logger

    # 创建格式化器
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # 文件 handler - 使用 RotatingFileHandler 自动轮转日志
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, LOG_LEVEL))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 控制台 handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, LOG_LEVEL))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def get_logger(name: str = "CMEDownloader") -> logging.Logger:
    """
    获取日志记录器
    如果已经存在则直接返回，否则创建新的

    Args:
        name: 日志记录器名称

    Returns:
        logger 对象
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger


if __name__ == "__main__":
    # 测试日志功能
    logger = setup_logger("TestLogger")

    logger.debug("这是一条 DEBUG 消息")
    logger.info("这是一条 INFO 消息")
    logger.warning("这是一条 WARNING 消息")
    logger.error("这是一条 ERROR 消息")
    logger.critical("这是一条 CRITICAL 消息")

    print(f"\n日志文件已保存到: {LOG_FILE}")
