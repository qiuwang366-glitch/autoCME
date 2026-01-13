#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CME 数据 ETL 主程序
Extract-Transform-Load: 提取、转换、加载

功能：
1. 扫描下载目录中的 CME 报告文件
2. 根据文件类型调用相应的解析器
3. 将清洗后的数据存入 SQLite 数据库
4. 可选：将已处理的文件移动到归档目录

使用方法：
    python etl_main.py                    # 处理所有未处理的文件
    python etl_main.py --reprocess        # 重新处理所有文件（包括已处理的）
    python etl_main.py --archive          # 处理后移动文件到归档目录
    python etl_main.py --stats            # 显示数据库统计信息

作者：自动化脚本
创建日期：2024-01-13
"""

import sys
import argparse
import shutil
from pathlib import Path
from typing import List

# 将 src 目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.logger import setup_logger
from src.database import DatabaseManager
from src.parsers import InventoryParser, DeliveryNoticeParser
from src.config import DATA_ROOT


class CMEDataETL:
    """
    CME 数据 ETL 处理器

    职责：
    - 文件扫描和分类
    - 调用解析器处理文件
    - 数据入库
    - 文件归档（可选）
    """

    def __init__(self, data_dir: Path, db_path: Path, archive_dir: Path = None,
                 logger=None):
        """
        初始化 ETL 处理器

        Args:
            data_dir: 数据文件目录
            db_path: 数据库文件路径
            archive_dir: 归档目录（可选）
            logger: 日志记录器
        """
        self.data_dir = Path(data_dir)
        self.db_path = Path(db_path)
        self.archive_dir = Path(archive_dir) if archive_dir else None
        self.logger = logger or setup_logger("CME_ETL")

        # 初始化数据库管理器
        self.db_manager = DatabaseManager(self.db_path, self.logger)

        # 初始化解析器
        self.inventory_parser = InventoryParser(self.logger)
        self.delivery_parser = DeliveryNoticeParser(self.logger)

        # 创建归档目录
        if self.archive_dir:
            self.archive_dir.mkdir(parents=True, exist_ok=True)

    def scan_files(self, reprocess: bool = False) -> List[Path]:
        """
        扫描数据目录，找到需要处理的文件

        Args:
            reprocess: 是否重新处理已处理过的文件

        Returns:
            文件路径列表
        """
        self.logger.info(f"扫描数据目录: {self.data_dir}")

        if not self.data_dir.exists():
            self.logger.error(f"数据目录不存在: {self.data_dir}")
            return []

        # 支持的文件格式
        patterns = ['*.csv', '*.xls', '*.xlsx', '*.pdf']
        all_files = []

        for pattern in patterns:
            files = list(self.data_dir.glob(pattern))
            all_files.extend(files)

        # 过滤已处理的文件
        if not reprocess:
            unprocessed_files = []
            for file_path in all_files:
                if not self.db_manager.is_file_processed(str(file_path)):
                    unprocessed_files.append(file_path)
            all_files = unprocessed_files

        self.logger.info(f"找到 {len(all_files)} 个文件待处理")
        return sorted(all_files)

    def classify_file(self, file_path: Path) -> str:
        """
        根据文件名判断文件类型

        Args:
            file_path: 文件路径

        Returns:
            文件类型：'inventory' 或 'delivery'
        """
        filename_lower = file_path.name.lower()

        # 库存报告
        if 'stock' in filename_lower or 'stocks' in filename_lower:
            return 'inventory'

        # 交割通知
        if 'delivery' in filename_lower or 'notice' in filename_lower:
            return 'delivery'

        # 默认：尝试根据文件扩展名判断
        if file_path.suffix.lower() in ['.csv', '.xls', '.xlsx']:
            return 'inventory'
        elif file_path.suffix.lower() == '.pdf':
            return 'delivery'

        return 'unknown'

    def detect_report_type(self, filename: str) -> str:
        """
        检测交割通知的报告类型

        Args:
            filename: 文件名

        Returns:
            报告类型：'Daily', 'Monthly', 或 'YTD'
        """
        filename_lower = filename.lower()

        if 'daily' in filename_lower:
            return 'Daily'
        elif 'monthly' in filename_lower:
            return 'Monthly'
        elif 'ytd' in filename_lower or 'year' in filename_lower:
            return 'YTD'

        return 'Daily'  # 默认

    def process_file(self, file_path: Path) -> bool:
        """
        处理单个文件

        Args:
            file_path: 文件路径

        Returns:
            成功返回 True，失败返回 False
        """
        self.logger.info("="*60)
        self.logger.info(f"处理文件: {file_path.name}")
        self.logger.info("="*60)

        file_type = self.classify_file(file_path)
        file_size = file_path.stat().st_size

        try:
            records = []

            # 调用相应的解析器
            if file_type == 'inventory':
                self.logger.info("文件类型: 库存报告")
                records = self.inventory_parser.parse_file(file_path)

                if records:
                    # 插入数据库
                    count = self.db_manager.insert_inventory_records(records)
                    self.logger.info(f"成功插入 {count} 条库存记录")

            elif file_type == 'delivery':
                self.logger.info("文件类型: 交割通知")
                report_type = self.detect_report_type(file_path.name)
                self.logger.info(f"报告类型: {report_type}")

                records = self.delivery_parser.parse_file(file_path, report_type)

                if records:
                    # 插入数据库
                    count = self.db_manager.insert_delivery_records(records)
                    self.logger.info(f"成功插入 {count} 条交割记录")

            else:
                self.logger.warning(f"未知文件类型: {file_path.name}")
                self.db_manager.log_file_processing(
                    str(file_path),
                    file_path.name,
                    'unknown',
                    file_size,
                    'skipped',
                    error_message="未知文件类型"
                )
                return False

            # 记录处理日志
            if records:
                self.db_manager.log_file_processing(
                    str(file_path),
                    file_path.name,
                    file_type,
                    file_size,
                    'success',
                    records_inserted=len(records)
                )
                return True
            else:
                self.db_manager.log_file_processing(
                    str(file_path),
                    file_path.name,
                    file_type,
                    file_size,
                    'failed',
                    error_message="未提取到有效数据"
                )
                return False

        except Exception as e:
            self.logger.error(f"处理文件失败: {e}", exc_info=True)
            self.db_manager.log_file_processing(
                str(file_path),
                file_path.name,
                file_type,
                file_size,
                'failed',
                error_message=str(e)
            )
            return False

    def archive_file(self, file_path: Path):
        """
        归档已处理的文件

        Args:
            file_path: 文件路径
        """
        if not self.archive_dir:
            return

        try:
            # 创建归档子目录（按年月分类）
            from datetime import datetime
            year_month = datetime.now().strftime("%Y-%m")
            archive_subdir = self.archive_dir / year_month
            archive_subdir.mkdir(parents=True, exist_ok=True)

            # 移动文件
            dest_path = archive_subdir / file_path.name

            # 如果目标文件已存在，添加序号
            if dest_path.exists():
                base_name = dest_path.stem
                suffix = dest_path.suffix
                counter = 1
                while dest_path.exists():
                    dest_path = archive_subdir / f"{base_name}_{counter}{suffix}"
                    counter += 1

            shutil.move(str(file_path), str(dest_path))
            self.logger.info(f"文件已归档: {dest_path}")

        except Exception as e:
            self.logger.error(f"归档文件失败: {e}")

    def process_all(self, reprocess: bool = False, archive: bool = False):
        """
        处理所有文件

        Args:
            reprocess: 是否重新处理已处理过的文件
            archive: 是否归档已处理的文件
        """
        self.logger.info("开始 ETL 处理任务")

        # 扫描文件
        files = self.scan_files(reprocess=reprocess)

        if not files:
            self.logger.info("没有文件需要处理")
            return

        # 处理每个文件
        success_count = 0
        failed_count = 0

        for file_path in files:
            success = self.process_file(file_path)

            if success:
                success_count += 1

                # 归档文件
                if archive:
                    self.archive_file(file_path)
            else:
                failed_count += 1

        # 显示统计
        self.logger.info("="*60)
        self.logger.info("ETL 处理完成")
        self.logger.info(f"总文件数: {len(files)}")
        self.logger.info(f"成功: {success_count}")
        self.logger.info(f"失败: {failed_count}")
        self.logger.info("="*60)

        # 显示数据库统计
        self.show_stats()

    def show_stats(self):
        """
        显示数据库统计信息
        """
        stats = self.db_manager.get_processing_stats()

        self.logger.info("\n" + "="*60)
        self.logger.info("数据库统计信息")
        self.logger.info("="*60)
        self.logger.info(f"已处理文件总数: {stats['total_files']}")
        self.logger.info(f"  - 成功: {stats['success_files']}")
        self.logger.info(f"  - 失败: {stats['failed_files']}")
        self.logger.info(f"库存记录总数: {stats['inventory_records']}")
        self.logger.info(f"交割记录总数: {stats['delivery_records']}")
        self.logger.info("="*60)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="CME 数据 ETL 处理工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
    python etl_main.py                      # 处理所有未处理的文件
    python etl_main.py --reprocess          # 重新处理所有文件
    python etl_main.py --archive            # 处理后归档文件
    python etl_main.py --stats              # 显示统计信息
    python etl_main.py --data-dir /path     # 指定数据目录

数据库位置：
    data/cme_data.db

归档目录：
    data/archive/
        """
    )

    parser.add_argument(
        '--data-dir',
        type=str,
        default=str(DATA_ROOT),
        help='数据文件目录（默认从 config.py 读取）'
    )

    parser.add_argument(
        '--db-path',
        type=str,
        default=str(Path(__file__).parent / "data" / "cme_data.db"),
        help='数据库文件路径'
    )

    parser.add_argument(
        '--archive-dir',
        type=str,
        default=str(Path(__file__).parent / "data" / "archive"),
        help='归档目录'
    )

    parser.add_argument(
        '--reprocess',
        action='store_true',
        help='重新处理已处理过的文件'
    )

    parser.add_argument(
        '--archive',
        action='store_true',
        help='处理后将文件移动到归档目录'
    )

    parser.add_argument(
        '--stats',
        action='store_true',
        help='仅显示数据库统计信息'
    )

    parser.add_argument(
        '--quiet',
        action='store_true',
        help='静默模式：不输出到控制台'
    )

    args = parser.parse_args()

    # 配置日志
    logger = setup_logger("CME_ETL", log_to_console=not args.quiet)

    # 创建 ETL 处理器
    etl = CMEDataETL(
        data_dir=args.data_dir,
        db_path=args.db_path,
        archive_dir=args.archive_dir if args.archive else None,
        logger=logger
    )

    # 仅显示统计信息
    if args.stats:
        etl.show_stats()
        return 0

    # 执行 ETL 处理
    try:
        etl.process_all(reprocess=args.reprocess, archive=args.archive)
        return 0

    except KeyboardInterrupt:
        logger.warning("任务被用户中断")
        return 130

    except Exception as e:
        logger.error(f"ETL 任务失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
