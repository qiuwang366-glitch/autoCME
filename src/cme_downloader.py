#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CME 数据下载器核心模块
负责从 CME 网站下载金融数据报告

设计特点：
1. 面向对象设计，易于扩展
2. 完整的错误处理和日志记录
3. 支持重试机制
4. 预留数据库接口
5. 文件命名规范化
"""

import os
import re
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# 导入配置
from config import (
    CME_BASE_URL,
    CME_DELIVERY_NOTICES_URL,
    DATA_ROOT,
    REQUEST_HEADERS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_DELAY,
    DATE_FORMAT,
    DUPLICATE_STRATEGY,
    MIN_FILE_SIZE,
    DOWNLOAD_FILES
)


class CMEDownloader:
    """
    CME 数据下载器主类

    功能：
    - 解析 CME 网页，提取下载链接
    - 下载文件并规范化命名
    - 记录下载日志
    - 支持重试机制
    - 预留数据库接口
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化下载器

        Args:
            logger: 日志记录器，如果不提供则使用默认logger
        """
        self.logger = logger or logging.getLogger(__name__)
        self.session = self._create_session()
        self.download_date = datetime.now().strftime(DATE_FORMAT)
        self.download_results = []  # 存储下载结果，便于后续数据库插入

    def _create_session(self) -> requests.Session:
        """
        创建并配置 requests session
        包含反爬虫设置
        """
        session = requests.Session()
        session.headers.update(REQUEST_HEADERS)
        return session

    def _retry_request(self, url: str, max_retries: int = MAX_RETRIES) -> Optional[requests.Response]:
        """
        带重试机制的 HTTP 请求

        Args:
            url: 请求 URL
            max_retries: 最大重试次数

        Returns:
            响应对象，失败返回 None
        """
        for attempt in range(max_retries):
            try:
                self.logger.info(f"正在请求 URL: {url} (尝试 {attempt + 1}/{max_retries})")
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"请求失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))  # 指数退避
                else:
                    self.logger.error(f"请求最终失败: {url}")
        return None

    def fetch_page_content(self) -> Optional[str]:
        """
        获取 CME 主页面内容

        Returns:
            页面 HTML 内容，失败返回 None
        """
        response = self._retry_request(CME_DELIVERY_NOTICES_URL)
        if response:
            return response.text
        return None

    def parse_download_links(self, html_content: str) -> Dict[str, Optional[str]]:
        """
        解析页面，提取下载链接

        Args:
            html_content: 页面 HTML 内容

        Returns:
            字典，key 为文件 ID，value 为下载 URL
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        download_links = {}

        for file_config in DOWNLOAD_FILES:
            file_id = file_config["id"]
            keyword = file_config["keyword"]
            file_type = file_config["file_type"]

            self.logger.info(f"正在查找文件: {file_config['name']} (关键词: {keyword})")

            # 查找所有包含关键词的链接
            # 优先查找 <a> 标签中文本包含关键词的链接
            link = None

            # 策略1: 查找 <a> 标签文本直接匹配
            for a_tag in soup.find_all('a', href=True):
                link_text = a_tag.get_text(strip=True)
                if keyword.lower() in link_text.lower():
                    href = a_tag['href']
                    # 检查是否是预期的文件类型
                    if file_type in href.lower() or href.endswith(('.pdf', '.xls', '.xlsx', '.csv')):
                        link = href
                        self.logger.info(f"找到链接: {link_text} -> {link}")
                        break

            # 策略2: 如果策略1失败，尝试更宽松的匹配
            if not link:
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    link_text = a_tag.get_text(strip=True)
                    # 检查 href 或 link_text 是否包含关键词
                    if (keyword.lower() in link_text.lower() or keyword.lower() in href.lower()):
                        if file_type in href.lower() or href.endswith(('.pdf', '.xls', '.xlsx', '.csv')):
                            link = href
                            self.logger.info(f"找到链接（宽松匹配）: {link_text} -> {link}")
                            break

            if link:
                # 如果是相对路径，转换为绝对路径
                if not link.startswith('http'):
                    link = urljoin(CME_BASE_URL, link)
                download_links[file_id] = link
            else:
                self.logger.warning(f"未找到文件: {file_config['name']}")
                download_links[file_id] = None

        return download_links

    def _generate_filename(self, file_config: Dict, original_url: str) -> str:
        """
        生成规范化的文件名
        格式: YYYYMMDD_prefix_original_name.ext

        Args:
            file_config: 文件配置字典
            original_url: 原始下载 URL

        Returns:
            规范化的文件名
        """
        # 从 URL 提取原始文件名
        parsed_url = urlparse(original_url)
        original_filename = os.path.basename(parsed_url.path)

        # 如果 URL 中没有文件名，使用默认命名
        if not original_filename or '.' not in original_filename:
            ext = file_config['file_type']
            original_filename = f"{file_config['prefix']}.{ext}"

        # 移除文件名中的特殊字符
        original_filename = re.sub(r'[^\w\-_\.]', '_', original_filename)

        # 组合新文件名: 日期_前缀_原文件名
        # 例如: 20240113_metal_delivery_daily_report.pdf
        prefix = file_config['prefix']
        new_filename = f"{self.download_date}_{prefix}_{original_filename}"

        return new_filename

    def download_file(self, url: str, file_config: Dict) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        下载单个文件

        Args:
            url: 下载 URL
            file_config: 文件配置字典

        Returns:
            (成功标志, 保存路径, 错误信息)
        """
        try:
            # 确保目标目录存在
            DATA_ROOT.mkdir(parents=True, exist_ok=True)

            # 生成文件名
            filename = self._generate_filename(file_config, url)
            filepath = DATA_ROOT / filename

            # 检查文件是否已存在
            if filepath.exists():
                if DUPLICATE_STRATEGY == "skip":
                    self.logger.info(f"文件已存在，跳过: {filename}")
                    return True, str(filepath), None
                elif DUPLICATE_STRATEGY == "overwrite":
                    self.logger.info(f"文件已存在，将覆盖: {filename}")

            # 下载文件
            self.logger.info(f"正在下载: {filename}")
            response = self._retry_request(url)

            if not response:
                return False, None, "下载请求失败"

            # 验证文件大小
            content_length = len(response.content)
            if content_length < MIN_FILE_SIZE:
                error_msg = f"文件大小异常: {content_length} 字节 (最小要求: {MIN_FILE_SIZE} 字节)"
                self.logger.error(error_msg)
                return False, None, error_msg

            # 保存文件
            with open(filepath, 'wb') as f:
                f.write(response.content)

            self.logger.info(f"下载成功: {filename} ({content_length} 字节)")
            return True, str(filepath), None

        except Exception as e:
            error_msg = f"下载失败: {str(e)}"
            self.logger.error(error_msg)
            return False, None, error_msg

    def download_all(self) -> Dict:
        """
        下载所有配置的文件

        Returns:
            下载结果统计字典
        """
        start_time = datetime.now()
        self.logger.info("="*60)
        self.logger.info(f"开始下载任务 - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*60)

        # 获取页面内容
        html_content = self.fetch_page_content()
        if not html_content:
            self.logger.error("无法获取 CME 页面内容，任务终止")
            return {
                "success": False,
                "total": 0,
                "succeeded": 0,
                "failed": 0,
                "results": []
            }

        # 解析下载链接
        download_links = self.parse_download_links(html_content)

        # 下载所有文件
        results = []
        succeeded = 0
        failed = 0

        for file_config in DOWNLOAD_FILES:
            file_id = file_config["id"]
            url = download_links.get(file_id)

            result = {
                "file_id": file_id,
                "file_name": file_config["name"],
                "url": url,
                "success": False,
                "filepath": None,
                "error": None,
                "timestamp": datetime.now().isoformat()
            }

            if url:
                success, filepath, error = self.download_file(url, file_config)
                result["success"] = success
                result["filepath"] = filepath
                result["error"] = error

                if success:
                    succeeded += 1
                else:
                    failed += 1
            else:
                result["error"] = "未找到下载链接"
                failed += 1
                self.logger.error(f"跳过文件: {file_config['name']} - 未找到下载链接")

            results.append(result)
            self.download_results.append(result)  # 保存到实例变量，便于数据库操作

        # 统计结果
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        summary = {
            "success": failed == 0,
            "total": len(DOWNLOAD_FILES),
            "succeeded": succeeded,
            "failed": failed,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "results": results
        }

        self.logger.info("="*60)
        self.logger.info(f"下载任务完成 - 用时 {duration:.2f} 秒")
        self.logger.info(f"总计: {summary['total']} | 成功: {succeeded} | 失败: {failed}")
        self.logger.info("="*60)

        return summary

    def save_to_database(self, summary: Dict):
        """
        保存下载记录到数据库（预留接口）

        Args:
            summary: 下载结果统计字典

        注意：
        此方法为数据库集成预留接口
        实际使用时需要实现具体的数据库操作逻辑
        """
        # TODO: 实现数据库保存逻辑
        # 示例代码框架：
        """
        try:
            # 连接数据库
            db_conn = connect_to_database()

            # 插入下载日志
            log_id = db_conn.insert_download_log({
                'download_date': self.download_date,
                'total_files': summary['total'],
                'succeeded': summary['succeeded'],
                'failed': summary['failed'],
                'duration': summary['duration_seconds'],
                'start_time': summary['start_time'],
                'end_time': summary['end_time']
            })

            # 插入文件元数据
            for result in summary['results']:
                db_conn.insert_file_metadata({
                    'log_id': log_id,
                    'file_id': result['file_id'],
                    'file_name': result['file_name'],
                    'url': result['url'],
                    'filepath': result['filepath'],
                    'success': result['success'],
                    'error': result['error'],
                    'timestamp': result['timestamp']
                })

            db_conn.commit()
            self.logger.info("下载记录已保存到数据库")

        except Exception as e:
            self.logger.error(f"保存到数据库失败: {e}")
        """
        pass

    def get_download_results(self) -> List[Dict]:
        """
        获取下载结果列表
        便于外部调用和数据库集成

        Returns:
            下载结果列表
        """
        return self.download_results


if __name__ == "__main__":
    # 测试代码
    import sys
    sys.path.append(str(Path(__file__).parent))

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # 创建下载器并执行
    downloader = CMEDownloader()
    summary = downloader.download_all()

    # 显示结果
    print("\n下载结果:")
    for result in summary['results']:
        status = "✓" if result['success'] else "✗"
        print(f"{status} {result['file_name']}: {result.get('filepath', result.get('error'))}")
