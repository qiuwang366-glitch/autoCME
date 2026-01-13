#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CME 数据解析器模块
负责解析 CSV、XLS 和 PDF 格式的 CME 报告
"""

import re
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime

import pandas as pd
import pdfplumber


class BaseParser:
    """
    解析器基类
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化解析器

        Args:
            logger: 日志记录器
        """
        self.logger = logger or logging.getLogger(__name__)

    @staticmethod
    def clean_numeric_string(value: str) -> Optional[float]:
        """
        清洗数字字符串，转换为浮点数

        示例：
            "1,234.56" -> 1234.56
            "1,234" -> 1234.0
            "" -> None
            "N/A" -> None

        Args:
            value: 数字字符串

        Returns:
            浮点数或 None
        """
        if pd.isna(value) or value == '' or value is None:
            return None

        # 如果已经是数字类型，直接返回
        if isinstance(value, (int, float)):
            return float(value)

        # 转换为字符串并清理
        value_str = str(value).strip()

        # 处理特殊值
        if value_str.upper() in ['N/A', 'NA', '-', 'NULL', 'NONE']:
            return None

        # 移除逗号和空格
        value_str = value_str.replace(',', '').replace(' ', '')

        try:
            return float(value_str)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def parse_date_string(date_str: str) -> Optional[str]:
        """
        解析日期字符串，返回标准格式 YYYY-MM-DD

        支持格式：
            - "January 13, 2024"
            - "01/13/2024"
            - "2024-01-13"
            - "13-Jan-2024"

        Args:
            date_str: 日期字符串

        Returns:
            标准格式日期字符串或 None
        """
        if pd.isna(date_str) or not date_str:
            return None

        date_str = str(date_str).strip()

        # 尝试多种日期格式
        date_formats = [
            "%B %d, %Y",      # January 13, 2024
            "%m/%d/%Y",       # 01/13/2024
            "%Y-%m-%d",       # 2024-01-13
            "%d-%b-%Y",       # 13-Jan-2024
            "%d/%m/%Y",       # 13/01/2024
            "%Y/%m/%d",       # 2024/01/13
        ]

        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None


class InventoryParser(BaseParser):
    """
    库存报告解析器（CSV/XLS）

    处理 Gold Stocks 和 Silver Stocks 文件
    """

    def parse_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        解析库存报告文件

        Args:
            file_path: 文件路径

        Returns:
            解析后的记录列表
        """
        self.logger.info(f"开始解析库存文件: {file_path.name}")

        try:
            # 判断产品类型
            product = self._detect_product(file_path.name)

            # 读取文件并提取元数据
            metadata = self._extract_metadata(file_path)

            if not metadata or not metadata.get('activity_date'):
                self.logger.error(f"无法提取 Activity Date: {file_path.name}")
                return []

            # 读取数据表格
            df = self._read_data_table(file_path)

            if df is None or df.empty:
                self.logger.warning(f"文件中没有有效数据: {file_path.name}")
                return []

            # 转换为记录列表
            records = self._convert_to_records(df, product, metadata)

            self.logger.info(f"成功解析 {len(records)} 条记录")
            return records

        except Exception as e:
            self.logger.error(f"解析文件失败 {file_path.name}: {e}", exc_info=True)
            return []

    def _detect_product(self, filename: str) -> str:
        """
        从文件名检测产品类型

        Args:
            filename: 文件名

        Returns:
            产品名称（Gold 或 Silver）
        """
        filename_lower = filename.lower()

        if 'gold' in filename_lower:
            return 'Gold'
        elif 'silver' in filename_lower:
            return 'Silver'
        else:
            return 'Unknown'

    def _extract_metadata(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """
        从文件头部提取元数据

        重点：提取 Activity Date（不是 Report Date）

        Args:
            file_path: 文件路径

        Returns:
            元数据字典
        """
        try:
            # 读取前 15 行（元数据通常在前 10 行）
            if file_path.suffix.lower() in ['.xls', '.xlsx']:
                df_header = pd.read_excel(file_path, header=None, nrows=15)
            else:
                df_header = pd.read_csv(file_path, header=None, nrows=15)

            metadata = {}

            # 遍历每一行，查找关键字
            for idx, row in df_header.iterrows():
                # 转换为字符串并合并所有列
                row_text = ' '.join([str(cell) for cell in row if pd.notna(cell)])

                # 提取 Activity Date（优先）
                if 'Activity Date' in row_text or 'activity date' in row_text.lower():
                    # 查找日期部分
                    date_match = re.search(r':\s*(.+)', row_text)
                    if date_match:
                        date_str = date_match.group(1).strip()
                        parsed_date = self.parse_date_string(date_str)
                        if parsed_date:
                            metadata['activity_date'] = parsed_date
                            self.logger.info(f"提取到 Activity Date: {parsed_date}")

                # 提取 Report Date（备用）
                if 'Report Date' in row_text or 'report date' in row_text.lower():
                    date_match = re.search(r':\s*(.+)', row_text)
                    if date_match:
                        date_str = date_match.group(1).strip()
                        parsed_date = self.parse_date_string(date_str)
                        if parsed_date:
                            metadata['report_date'] = parsed_date

                # 提取单位
                if 'unit' in row_text.lower() or 'troy ounce' in row_text.lower():
                    if 'Troy Ounce' in row_text:
                        metadata['unit'] = 'Troy Ounces'

            return metadata if metadata else None

        except Exception as e:
            self.logger.error(f"提取元数据失败: {e}", exc_info=True)
            return None

    def _read_data_table(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        读取数据表格（跳过元数据行）

        Args:
            file_path: 文件路径

        Returns:
            DataFrame
        """
        try:
            # 尝试不同的 skiprows 值，找到表头
            for skip in range(5, 15):
                try:
                    if file_path.suffix.lower() in ['.xls', '.xlsx']:
                        df = pd.read_excel(file_path, skiprows=skip)
                    else:
                        df = pd.read_csv(file_path, skiprows=skip)

                    # 检查是否找到正确的表头
                    # 库存表通常包含 Depository, Registered, Eligible, Total 等列
                    columns_lower = [str(col).lower() for col in df.columns]

                    if 'depository' in columns_lower or 'warehouse' in columns_lower:
                        self.logger.info(f"找到数据表（skiprows={skip}）")
                        # 清理列名
                        df.columns = df.columns.str.strip()
                        return df

                except Exception:
                    continue

            self.logger.warning("未找到有效的数据表")
            return None

        except Exception as e:
            self.logger.error(f"读取数据表失败: {e}", exc_info=True)
            return None

    def _convert_to_records(self, df: pd.DataFrame, product: str,
                           metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        将 DataFrame 转换为记录列表

        Args:
            df: DataFrame
            product: 产品名称
            metadata: 元数据

        Returns:
            记录列表
        """
        records = []

        # 查找列名（不区分大小写）
        columns_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'depository' in col_lower or 'warehouse' in col_lower:
                columns_map['depository'] = col
            elif 'registered' in col_lower:
                columns_map['registered'] = col
            elif 'eligible' in col_lower:
                columns_map['eligible'] = col
            elif 'total' in col_lower:
                columns_map['total'] = col

        if not columns_map.get('depository'):
            self.logger.error("未找到 Depository 列")
            return []

        # 遍历每一行
        for idx, row in df.iterrows():
            depository = str(row[columns_map['depository']]).strip()

            # 跳过空行或汇总行
            if pd.isna(row[columns_map['depository']]) or \
               depository in ['', 'Total', 'TOTAL', 'Grand Total']:
                continue

            # 跳过表头重复行
            if 'depository' in depository.lower():
                continue

            record = {
                'activity_date': metadata.get('activity_date'),
                'product': product,
                'depository': depository,
                'registered': self.clean_numeric_string(
                    row.get(columns_map.get('registered'))
                ) if columns_map.get('registered') else None,
                'eligible': self.clean_numeric_string(
                    row.get(columns_map.get('eligible'))
                ) if columns_map.get('eligible') else None,
                'total': self.clean_numeric_string(
                    row.get(columns_map.get('total'))
                ) if columns_map.get('total') else None,
                'unit': metadata.get('unit', 'Troy Ounces'),
                'report_date': metadata.get('report_date')
            }

            records.append(record)

        return records


class DeliveryNoticeParser(BaseParser):
    """
    交割通知解析器（PDF）

    处理 Metal Delivery Notices (Daily, Monthly, YTD)
    """

    def parse_file(self, file_path: Path, report_type: str = 'Daily') -> List[Dict[str, Any]]:
        """
        解析交割通知 PDF

        Args:
            file_path: PDF 文件路径
            report_type: 报告类型（Daily, Monthly, YTD）

        Returns:
            解析后的记录列表
        """
        self.logger.info(f"开始解析交割通知: {file_path.name}")

        try:
            with pdfplumber.open(file_path) as pdf:
                all_records = []

                # 遍历每一页
                for page_num, page in enumerate(pdf.pages, 1):
                    self.logger.info(f"处理第 {page_num}/{len(pdf.pages)} 页")

                    # 提取页面文本和表格
                    records = self._parse_page(page, file_path.name, report_type)
                    all_records.extend(records)

                self.logger.info(f"成功解析 {len(all_records)} 条记录")
                return all_records

        except Exception as e:
            self.logger.error(f"解析 PDF 失败 {file_path.name}: {e}", exc_info=True)
            return []

    def _parse_page(self, page, source_file: str, report_type: str) -> List[Dict[str, Any]]:
        """
        解析 PDF 单页

        关键逻辑：
        1. 先提取页面文本，识别 "CONTRACT:" 标题
        2. 提取表格数据
        3. 将 CONTRACT 信息与表格数据关联

        Args:
            page: pdfplumber Page 对象
            source_file: 源文件名
            report_type: 报告类型

        Returns:
            记录列表
        """
        records = []

        try:
            # 提取页面文本
            text = page.extract_text()

            if not text:
                return []

            # 提取所有 CONTRACT 标题
            contracts = self._extract_contracts(text)

            if not contracts:
                self.logger.warning("未找到 CONTRACT 标题")
                return []

            # 提取表格
            tables = page.extract_tables()

            if not tables:
                self.logger.warning("未找到表格数据")
                return []

            # 关联 CONTRACT 与表格
            # 策略：根据 CONTRACT 在文本中的位置，判断它对应哪个表格
            for contract_info in contracts:
                self.logger.info(f"处理合约: {contract_info['product']} {contract_info['contract_month']}")

                # 查找对应的表格（简化处理：假设每个 CONTRACT 后紧跟一个表格）
                for table in tables:
                    table_records = self._parse_table(
                        table,
                        contract_info,
                        source_file,
                        report_type
                    )
                    records.extend(table_records)

        except Exception as e:
            self.logger.error(f"解析页面失败: {e}", exc_info=True)

        return records

    def _extract_contracts(self, text: str) -> List[Dict[str, str]]:
        """
        从文本中提取 CONTRACT 信息

        示例文本：
        "CONTRACT: JANUARY 2026 COMEX 100 GOLD FUTURES"
        "CONTRACT: FEBRUARY 2026 COMEX 5000 SILVER FUTURES"

        Args:
            text: 页面文本

        Returns:
            合约信息列表
        """
        contracts = []

        # 使用正则表达式匹配 CONTRACT 行
        pattern = r'CONTRACT:\s*([A-Z]+)\s+(\d{4})\s+COMEX\s+\d+\s+([A-Z]+)'

        matches = re.finditer(pattern, text)

        for match in matches:
            month = match.group(1)  # JANUARY
            year = match.group(2)    # 2026
            product = match.group(3) # GOLD

            contract_info = {
                'contract_month': f"{month} {year}",
                'product': product.title(),  # Gold, Silver
                'full_text': match.group(0)
            }

            contracts.append(contract_info)
            self.logger.debug(f"找到合约: {contract_info}")

        return contracts

    def _parse_table(self, table: List[List[str]], contract_info: Dict[str, str],
                    source_file: str, report_type: str) -> List[Dict[str, Any]]:
        """
        解析表格数据

        表格通常包含列：
        - Intent Date (或 Date)
        - Daily Total (或 Daily)
        - Cumulative (或 Cum.)

        Args:
            table: 表格数据（二维列表）
            contract_info: 合约信息
            source_file: 源文件名
            report_type: 报告类型

        Returns:
            记录列表
        """
        if not table or len(table) < 2:
            return []

        records = []

        # 第一行通常是表头
        header = [str(cell).strip().lower() if cell else '' for cell in table[0]]

        # 查找列索引
        date_col_idx = None
        daily_col_idx = None
        cumulative_col_idx = None

        for idx, col_name in enumerate(header):
            if 'intent' in col_name or 'date' in col_name:
                date_col_idx = idx
            elif 'daily' in col_name:
                daily_col_idx = idx
            elif 'cumulative' in col_name or 'cum' in col_name:
                cumulative_col_idx = idx

        if date_col_idx is None:
            self.logger.warning("未找到日期列")
            return []

        # 解析数据行（跳过表头）
        for row in table[1:]:
            if not row or len(row) <= date_col_idx:
                continue

            # 提取日期
            date_str = str(row[date_col_idx]).strip() if row[date_col_idx] else None
            if not date_str or date_str == '':
                continue

            intent_date = self.parse_date_string(date_str)
            if not intent_date:
                continue

            # 提取数值
            daily_total = None
            if daily_col_idx is not None and len(row) > daily_col_idx:
                daily_total = self.clean_numeric_string(row[daily_col_idx])

            cumulative = None
            if cumulative_col_idx is not None and len(row) > cumulative_col_idx:
                cumulative = self.clean_numeric_string(row[cumulative_col_idx])

            record = {
                'intent_date': intent_date,
                'product': contract_info['product'],
                'contract_month': contract_info['contract_month'],
                'daily_total': int(daily_total) if daily_total is not None else None,
                'cumulative': int(cumulative) if cumulative is not None else None,
                'report_type': report_type,
                'source_file': source_file
            }

            records.append(record)

        return records


if __name__ == "__main__":
    # 测试代码
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from src.logger import setup_logger

    logger = setup_logger("ParserTest")

    # 测试 CSV 解析器
    print("\n=== 测试库存解析器 ===")
    inventory_parser = InventoryParser(logger)

    # 测试 PDF 解析器
    print("\n=== 测试交割通知解析器 ===")
    delivery_parser = DeliveryNoticeParser(logger)

    logger.info("解析器测试完成")
