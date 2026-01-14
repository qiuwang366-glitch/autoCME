#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF 解析器测试脚本
用于调试和验证 PDF 解析逻辑

使用方法：
    python test_pdf_parser.py <pdf_file_path>
"""

import sys
from pathlib import Path

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.logger import setup_logger
from src.parsers import DeliveryNoticeParser
import pdfplumber


def test_pdf_extraction(pdf_path: Path):
    """
    测试 PDF 提取功能

    Args:
        pdf_path: PDF 文件路径
    """
    logger = setup_logger("PDFTest")

    logger.info(f"="*80)
    logger.info(f"测试 PDF: {pdf_path.name}")
    logger.info(f"="*80)

    # 1. 查看原始文本
    logger.info("\n--- 步骤1: 提取原始文本 ---")
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages[:3], 1):  # 只查看前3页
            logger.info(f"\n第 {page_num} 页内容：")
            text = page.extract_text()
            if text:
                # 只显示前1000个字符
                logger.info(text[:1000])
                logger.info("...")
            else:
                logger.warning("页面没有文本内容")

    # 2. 测试解析器
    logger.info("\n--- 步骤2: 使用解析器 ---")
    parser = DeliveryNoticeParser(logger)

    # 确定报告类型
    filename = pdf_path.name.lower()
    if 'daily' in filename:
        report_type = 'Daily'
    elif 'monthly' in filename or 'mtd' in filename:
        report_type = 'Monthly'
    elif 'ytd' in filename or 'year' in filename:
        report_type = 'YTD'
    else:
        report_type = 'Daily'

    logger.info(f"报告类型: {report_type}")

    # 解析文件
    records = parser.parse_file(pdf_path, report_type)

    logger.info(f"\n解析结果: 共提取 {len(records)} 条记录")

    if records:
        logger.info("\n前5条记录：")
        for i, record in enumerate(records[:5], 1):
            logger.info(f"{i}. {record}")
    else:
        logger.warning("未提取到任何记录")

    logger.info(f"\n{"="*80}")


def main():
    if len(sys.argv) < 2:
        print("使用方法: python test_pdf_parser.py <pdf_file_path>")
        print("\n示例:")
        print("  python test_pdf_parser.py data/20260113_metal_delivery_daily.pdf")
        sys.exit(1)

    pdf_path = Path(sys.argv[1])

    if not pdf_path.exists():
        print(f"错误: 文件不存在: {pdf_path}")
        sys.exit(1)

    if not pdf_path.suffix.lower() == '.pdf':
        print(f"错误: 不是 PDF 文件: {pdf_path}")
        sys.exit(1)

    test_pdf_extraction(pdf_path)


if __name__ == "__main__":
    main()
