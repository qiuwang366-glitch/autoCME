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

        实际PDF结构：
        1. CONTRACT: JANUARY 2026 ALUMINUM FUTURES
        2. SETTLEMENT: XXX, INTENT DATE: 01/12/2026, DELIVERY DATE: 01/14/2026
        3. 表格（公司列表）
        4. TOTAL: 15 (issued), 15 (stopped)
        5. MONTH TO DATE: 134

        提取策略：
        - 从文本提取 CONTRACT、INTENT DATE
        - 从文本提取 TOTAL 和 MONTH TO DATE 汇总数据

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

            # 按行分割文本
            lines = text.split('\n')

            # 解析每个合约块
            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # 查找 CONTRACT 行
                if line.startswith('CONTRACT:'):
                    # 提取合约信息
                    contract_info = self._extract_contract_from_line(line)

                    if contract_info:
                        # 查找 INTENT DATE（通常在 CONTRACT 后面几行）
                        intent_date = None
                        daily_issued = None
                        daily_stopped = None
                        cumulative = None

                        # 向前查找相关信息（最多查找20行）
                        for j in range(i + 1, min(i + 20, len(lines))):
                            next_line = lines[j].strip()

                            # 提取 INTENT DATE
                            if 'INTENT DATE:' in next_line:
                                date_match = re.search(r'INTENT DATE:\s*(\d{2}/\d{2}/\d{4})', next_line)
                                if date_match:
                                    intent_date = self.parse_date_string(date_match.group(1))

                            # 提取 TOTAL（Daily数据）
                            if next_line.startswith('TOTAL:'):
                                # 格式: "TOTAL: 15 15" 或 "TOTAL: 15"
                                numbers = re.findall(r'\d+', next_line)
                                if len(numbers) >= 2:
                                    daily_issued = int(numbers[0])
                                    daily_stopped = int(numbers[1])
                                elif len(numbers) == 1:
                                    daily_stopped = int(numbers[0])

                            # 提取 MONTH TO DATE（Cumulative）
                            if 'MONTH TO DATE:' in next_line:
                                # 格式: "MONTH TO DATE: 134"
                                numbers = re.findall(r'\d+', next_line)
                                if numbers:
                                    cumulative = int(numbers[-1])  # 取最后一个数字

                            # 如果遇到下一个 CONTRACT，停止查找
                            if next_line.startswith('CONTRACT:') or next_line.startswith('EXCHANGE:'):
                                break

                        # 如果找到了有效数据，创建记录
                        if intent_date and (daily_stopped is not None or cumulative is not None):
                            record = {
                                'intent_date': intent_date,
                                'product': contract_info['product'],
                                'contract_month': contract_info['contract_month'],
                                'daily_total': daily_stopped,  # 使用 STOPPED 作为 daily_total
                                'cumulative': cumulative,
                                'report_type': report_type,
                                'source_file': source_file
                            }

                            records.append(record)
                            self.logger.info(f"提取记录: {contract_info['product']} {contract_info['contract_month']}, "
                                           f"Intent: {intent_date}, Daily: {daily_stopped}, Cumulative: {cumulative}")

                i += 1

        except Exception as e:
            self.logger.error(f"解析页面失败: {e}", exc_info=True)

        return records

    def _extract_contract_from_line(self, line: str) -> Optional[Dict[str, str]]:
        """
        从 CONTRACT 行提取合约信息

        支持多种格式：
        - "CONTRACT: JANUARY 2026 ALUMINUM FUTURES"
        - "CONTRACT: JANUARY 2026 COMEX 100 GOLD FUTURES"
        - "CONTRACT: JANUARY 2026 COMEX COPPER FUTURES"
        - "CONTRACT: JANUARY 2026 COMEX 5000 SILVER FUTURES"

        Args:
            line: CONTRACT 行文本

        Returns:
            合约信息字典，或 None
        """
        # 正则表达式策略：
        # 1. 匹配月份（JANUARY等）
        # 2. 匹配年份（2026）
        # 3. 可选的 COMEX 和数字
        # 4. 匹配产品名（ALUMINUM, GOLD, COPPER, SILVER等）

        # 先尝试匹配带 COMEX 的格式
        pattern1 = r'CONTRACT:\s*([A-Z]+)\s+(\d{4})\s+COMEX\s+(?:\d+\s+)?([A-Z]+)\s+FUTURES'
        match = re.search(pattern1, line)

        if match:
            month = match.group(1)
            year = match.group(2)
            product = match.group(3)

            return {
                'contract_month': f"{month} {year}",
                'product': product.title(),
                'full_text': line
            }

        # 再尝试匹配不带 COMEX 的格式
        pattern2 = r'CONTRACT:\s*([A-Z]+)\s+(\d{4})\s+([A-Z]+)\s+FUTURES'
        match = re.search(pattern2, line)

        if match:
            month = match.group(1)
            year = match.group(2)
            product = match.group(3)

            return {
                'contract_month': f"{month} {year}",
                'product': product.title(),
                'full_text': line
            }

        self.logger.warning(f"无法解析 CONTRACT 行: {line}")
        return None

    def _parse_table(self, table: List[List[str]], contract_info: Dict[str, str],
                    source_file: str, report_type: str) -> List[Dict[str, Any]]:
        """
        解析表格数据（已弃用，保留用于兼容性）

        注：当前实现直接从文本提取汇总数据，不再使用表格解析

        Args:
            table: 表格数据（二维列表）
            contract_info: 合约信息
            source_file: 源文件名
            report_type: 报告类型

        Returns:
            空列表（不再使用表格解析）
        """
        # 新的解析逻辑直接从文本提取汇总数据
        # 此方法保留仅为向后兼容
        return []


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
