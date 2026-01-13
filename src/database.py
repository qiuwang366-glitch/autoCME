#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理模块
负责 SQLite 数据库的连接、表创建和数据操作
"""

import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime
from contextlib import contextmanager


class DatabaseManager:
    """
    SQLite 数据库管理器

    功能：
    - 数据库连接管理
    - 表结构创建和维护
    - 数据插入和查询
    - 事务管理
    """

    def __init__(self, db_path: Path, logger: Optional[logging.Logger] = None):
        """
        初始化数据库管理器

        Args:
            db_path: 数据库文件路径
            logger: 日志记录器
        """
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)

        # 确保数据库目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 初始化数据库
        self._initialize_database()

    @contextmanager
    def get_connection(self):
        """
        上下文管理器：获取数据库连接

        使用示例：
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM ...")
        """
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            conn.close()

    def _initialize_database(self):
        """
        初始化数据库，创建所有必要的表
        """
        self.logger.info(f"初始化数据库: {self.db_path}")

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 创建库存历史表
            cursor.execute(self._get_inventory_table_schema())

            # 创建交割通知表
            cursor.execute(self._get_delivery_notices_table_schema())

            # 创建文件处理记录表（用于跟踪已处理的文件）
            cursor.execute(self._get_file_processing_log_schema())

            self.logger.info("数据库表结构创建完成")

    def _get_inventory_table_schema(self) -> str:
        """
        库存历史表结构

        字段说明：
        - activity_date: 业务日期（从文件头部 Activity Date 提取）
        - product: 产品名称（Gold 或 Silver）
        - depository: 仓库名称
        - registered: 注册仓单数量
        - eligible: 有效货源数量
        - total: 总库存数量
        - unit: 单位（如 Troy Ounces）
        - created_at: 记录创建时间

        联合主键：(activity_date, product, depository)
        确保同一天、同一产品、同一仓库只有一条记录
        """
        return """
        CREATE TABLE IF NOT EXISTS inventory_history (
            activity_date DATE NOT NULL,
            product VARCHAR(50) NOT NULL,
            depository VARCHAR(200) NOT NULL,
            registered REAL,
            eligible REAL,
            total REAL,
            unit VARCHAR(50),
            report_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (activity_date, product, depository)
        )
        """

    def _get_delivery_notices_table_schema(self) -> str:
        """
        交割通知表结构

        字段说明：
        - intent_date: 意向日期
        - product: 产品名称（从 CONTRACT 标题提取）
        - contract_month: 合约月份（如 JANUARY 2026）
        - daily_total: 当日交割量
        - cumulative: 累计交割量
        - report_type: 报告类型（Daily, Monthly, YTD）
        - created_at: 记录创建时间

        联合主键：(intent_date, product, contract_month, report_type)
        """
        return """
        CREATE TABLE IF NOT EXISTS delivery_notices (
            intent_date DATE NOT NULL,
            product VARCHAR(100) NOT NULL,
            contract_month VARCHAR(50) NOT NULL,
            daily_total INTEGER,
            cumulative INTEGER,
            report_type VARCHAR(20) NOT NULL,
            source_file VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (intent_date, product, contract_month, report_type)
        )
        """

    def _get_file_processing_log_schema(self) -> str:
        """
        文件处理记录表

        用途：
        - 跟踪哪些文件已经被处理过
        - 避免重复处理
        - 记录处理结果和错误信息
        """
        return """
        CREATE TABLE IF NOT EXISTS file_processing_log (
            file_path VARCHAR(500) PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL,
            file_type VARCHAR(50) NOT NULL,
            file_size INTEGER,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            records_inserted INTEGER DEFAULT 0,
            error_message TEXT,
            UNIQUE(file_path)
        )
        """

    def insert_inventory_records(self, records: List[Dict[str, Any]]) -> int:
        """
        批量插入库存记录

        Args:
            records: 记录列表，每条记录是一个字典

        Returns:
            成功插入的记录数
        """
        if not records:
            self.logger.warning("没有库存记录需要插入")
            return 0

        with self.get_connection() as conn:
            cursor = conn.cursor()

            sql = """
            INSERT OR REPLACE INTO inventory_history
            (activity_date, product, depository, registered, eligible, total, unit, report_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """

            insert_data = []
            for record in records:
                insert_data.append((
                    record.get('activity_date'),
                    record.get('product'),
                    record.get('depository'),
                    record.get('registered'),
                    record.get('eligible'),
                    record.get('total'),
                    record.get('unit'),
                    record.get('report_date')
                ))

            cursor.executemany(sql, insert_data)
            count = cursor.rowcount

            self.logger.info(f"成功插入/更新 {count} 条库存记录")
            return count

    def insert_delivery_records(self, records: List[Dict[str, Any]]) -> int:
        """
        批量插入交割通知记录

        Args:
            records: 记录列表

        Returns:
            成功插入的记录数
        """
        if not records:
            self.logger.warning("没有交割记录需要插入")
            return 0

        with self.get_connection() as conn:
            cursor = conn.cursor()

            sql = """
            INSERT OR REPLACE INTO delivery_notices
            (intent_date, product, contract_month, daily_total, cumulative, report_type, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            insert_data = []
            for record in records:
                insert_data.append((
                    record.get('intent_date'),
                    record.get('product'),
                    record.get('contract_month'),
                    record.get('daily_total'),
                    record.get('cumulative'),
                    record.get('report_type'),
                    record.get('source_file')
                ))

            cursor.executemany(sql, insert_data)
            count = cursor.rowcount

            self.logger.info(f"成功插入/更新 {count} 条交割记录")
            return count

    def log_file_processing(self, file_path: str, file_name: str, file_type: str,
                           file_size: int, status: str, records_inserted: int = 0,
                           error_message: str = None):
        """
        记录文件处理状态

        Args:
            file_path: 文件完整路径
            file_name: 文件名
            file_type: 文件类型（csv, xls, pdf）
            file_size: 文件大小（字节）
            status: 处理状态（success, failed, skipped）
            records_inserted: 插入的记录数
            error_message: 错误信息（如果有）
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            sql = """
            INSERT OR REPLACE INTO file_processing_log
            (file_path, file_name, file_type, file_size, status, records_inserted, error_message, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """

            cursor.execute(sql, (
                file_path,
                file_name,
                file_type,
                file_size,
                status,
                records_inserted,
                error_message,
                datetime.now().isoformat()
            ))

            self.logger.info(f"文件处理日志已记录: {file_name} ({status})")

    def is_file_processed(self, file_path: str) -> bool:
        """
        检查文件是否已经被处理过

        Args:
            file_path: 文件路径

        Returns:
            True 如果文件已处理，False 否则
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status FROM file_processing_log WHERE file_path = ? AND status = 'success'",
                (file_path,)
            )
            result = cursor.fetchone()
            return result is not None

    def get_inventory_summary(self, product: str = None, start_date: str = None,
                             end_date: str = None) -> List[Dict]:
        """
        查询库存汇总数据

        Args:
            product: 产品名称（可选）
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）

        Returns:
            查询结果列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            sql = "SELECT * FROM inventory_history WHERE 1=1"
            params = []

            if product:
                sql += " AND product = ?"
                params.append(product)

            if start_date:
                sql += " AND activity_date >= ?"
                params.append(start_date)

            if end_date:
                sql += " AND activity_date <= ?"
                params.append(end_date)

            sql += " ORDER BY activity_date DESC, product, depository"

            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]

            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))

            return results

    def get_delivery_summary(self, product: str = None, report_type: str = None) -> List[Dict]:
        """
        查询交割通知汇总数据

        Args:
            product: 产品名称（可选）
            report_type: 报告类型（可选）

        Returns:
            查询结果列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            sql = "SELECT * FROM delivery_notices WHERE 1=1"
            params = []

            if product:
                sql += " AND product LIKE ?"
                params.append(f"%{product}%")

            if report_type:
                sql += " AND report_type = ?"
                params.append(report_type)

            sql += " ORDER BY intent_date DESC, product"

            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]

            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))

            return results

    def get_processing_stats(self) -> Dict:
        """
        获取处理统计信息

        Returns:
            统计信息字典
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 总文件数
            cursor.execute("SELECT COUNT(*) FROM file_processing_log")
            total_files = cursor.fetchone()[0]

            # 成功处理的文件数
            cursor.execute("SELECT COUNT(*) FROM file_processing_log WHERE status = 'success'")
            success_files = cursor.fetchone()[0]

            # 失败的文件数
            cursor.execute("SELECT COUNT(*) FROM file_processing_log WHERE status = 'failed'")
            failed_files = cursor.fetchone()[0]

            # 库存记录总数
            cursor.execute("SELECT COUNT(*) FROM inventory_history")
            inventory_records = cursor.fetchone()[0]

            # 交割记录总数
            cursor.execute("SELECT COUNT(*) FROM delivery_notices")
            delivery_records = cursor.fetchone()[0]

            return {
                'total_files': total_files,
                'success_files': success_files,
                'failed_files': failed_files,
                'inventory_records': inventory_records,
                'delivery_records': delivery_records
            }


if __name__ == "__main__":
    # 测试代码
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from src.logger import setup_logger

    logger = setup_logger("DatabaseTest")
    db_path = Path(__file__).parent.parent / "data" / "cme_data.db"

    # 创建数据库管理器
    db_manager = DatabaseManager(db_path, logger)

    # 测试插入库存记录
    test_inventory = [
        {
            'activity_date': '2024-01-13',
            'product': 'Gold',
            'depository': 'TEST WAREHOUSE',
            'registered': 1000.0,
            'eligible': 2000.0,
            'total': 3000.0,
            'unit': 'Troy Ounces',
            'report_date': '2024-01-14'
        }
    ]

    db_manager.insert_inventory_records(test_inventory)

    # 测试查询
    stats = db_manager.get_processing_stats()
    logger.info(f"统计信息: {stats}")

    logger.info("数据库测试完成")
