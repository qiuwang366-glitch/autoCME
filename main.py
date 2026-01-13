#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CME 数据自动下载主程序
用于定时下载 CME（芝商所）金融数据报告

使用方法：
    python main.py              # 执行下载任务
    python main.py --test       # 测试模式（仅解析链接，不下载）
    python main.py --validate   # 验证配置

作者：自动化脚本
创建日期：2024-01-13
"""

import sys
import argparse
from pathlib import Path

# 将 src 目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.logger import setup_logger
from src.cme_downloader import CMEDownloader
from src.config import validate_config, DATA_ROOT, LOG_FILE, DOWNLOAD_FILES


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description="CME 数据自动下载工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
    python main.py                  # 执行下载任务
    python main.py --test           # 测试模式
    python main.py --validate       # 验证配置

配置文件：
    src/config.py                   # 主配置文件

日志文件：
    logs/cme_downloader.log         # 运行日志
        """
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='测试模式：仅解析链接，不实际下载文件'
    )
    parser.add_argument(
        '--validate',
        action='store_true',
        help='验证配置文件是否正确'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='静默模式：不输出到控制台'
    )

    args = parser.parse_args()

    # 配置日志
    logger = setup_logger(log_to_console=not args.quiet)

    # 验证配置模式
    if args.validate:
        try:
            validate_config()
            logger.info("✓ 配置验证通过")
            logger.info(f"数据保存路径: {DATA_ROOT}")
            logger.info(f"日志保存路径: {LOG_FILE}")
            logger.info(f"配置文件数量: {len(DOWNLOAD_FILES)}")
            print("\n配置的文件列表:")
            for i, file_config in enumerate(DOWNLOAD_FILES, 1):
                print(f"{i}. {file_config['name']} ({file_config['description']})")
            return 0
        except Exception as e:
            logger.error(f"✗ 配置验证失败: {e}")
            return 1

    # 执行下载任务
    try:
        logger.info("CME 数据下载任务启动")

        # 验证配置
        validate_config()

        # 创建下载器
        downloader = CMEDownloader(logger=logger)

        if args.test:
            # 测试模式：仅解析链接
            logger.info("运行在测试模式 - 仅解析链接，不下载文件")
            html_content = downloader.fetch_page_content()
            if html_content:
                download_links = downloader.parse_download_links(html_content)
                logger.info("\n解析结果:")
                for file_id, url in download_links.items():
                    status = "✓" if url else "✗"
                    logger.info(f"{status} {file_id}: {url or '未找到'}")
            else:
                logger.error("无法获取页面内容")
                return 1
        else:
            # 正常模式：下载文件
            summary = downloader.download_all()

            # 显示结果摘要
            if not args.quiet:
                print("\n" + "="*60)
                print("下载结果摘要")
                print("="*60)
                for result in summary['results']:
                    status = "✓" if result['success'] else "✗"
                    name = result['file_name']
                    info = result.get('filepath', result.get('error', '未知错误'))
                    print(f"{status} {name}")
                    if not result['success']:
                        print(f"  错误: {info}")
                print("="*60)
                print(f"总计: {summary['total']} | "
                      f"成功: {summary['succeeded']} | "
                      f"失败: {summary['failed']} | "
                      f"用时: {summary['duration_seconds']:.2f}秒")
                print("="*60)

            # 如果启用数据库，保存记录
            # downloader.save_to_database(summary)

            # 返回状态码
            return 0 if summary['success'] else 1

    except KeyboardInterrupt:
        logger.warning("任务被用户中断")
        return 130
    except Exception as e:
        logger.error(f"任务执行失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
