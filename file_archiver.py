# file_archiver.py

import csv
import logging
import os  # <--- 1. 确保导入 os 模块
from datetime import datetime

logger = logging.getLogger(__name__)


class DailyFileArchiver:
    def __init__(self, base_filename: str, headers: list):
        self.base_filename = base_filename
        self.headers = headers
        self.csv_file = None
        self.csv_writer = None

    def _get_today_filename(self) -> str:
        today = datetime.now().strftime("%Y%m%d")
        return f"{self.base_filename}_{today}.csv"

    def _ensure_today_file_open(self):
        """
        确保当前日期的文件已正确打开。
        核心修改：增加了对文件是否存在的检查，以处理文件被外部删除的情况。
        """
        today_filename = self._get_today_filename()

        # --- 核心修改开始 ---
        # 检查条件：
        # 1. 文件未打开 (self.csv_file is None)
        # 2. 日期变更 (文件名不匹配)
        # 3. 文件被外部删除 (文件句柄存在，但文件系统中已无此文件)
        should_reopen = False
        if self.csv_file is None:
            should_reopen = True
        elif self.csv_file.name != today_filename:
            should_reopen = True
        elif not os.path.exists(self.csv_file.name):  # <--- 关键检查
            logger.warning(f"检测到文件 '{self.csv_file.name}' 已被外部删除，将重新创建。")
            should_reopen = True
        # --- 核心修改结束 ---

        if should_reopen:
            # 关闭旧文件句柄
            if self.csv_file:
                logger.info(f"关闭文件: {self.csv_file.name}")
                self.csv_file.close()
                self.csv_file = None
                self.csv_writer = None

            # 以追加模式 'a' 打开新文件。如果文件不存在，'a' 会创建它。
            try:
                self.csv_file = open(today_filename, 'a', newline='', encoding='utf-8-sig')
                self.csv_writer = csv.writer(self.csv_file)
                logger.info(f"成功打开/创建文件: {today_filename}")

                # 如果文件是新创建的（文件指针在开头），则写入表头
                if self.csv_file.tell() == 0:
                    self.csv_writer.writerow(self.headers)
                    logger.info(f"为新文件写入表头: {today_filename}")

            except Exception as e:
                logger.error(f"创建或打开CSV文件 '{today_filename}' 失败: {e}")
                self.csv_file = None
                self.csv_writer = None

    def write_rows(self, rows: list):
        """写入多行数据到当前的日文件中。"""
        if not rows:
            return

        # 在写入前，始终检查并确保文件是正确的（包括处理被删除的情况）
        self._ensure_today_file_open()

        if self.csv_writer:
            try:
                self.csv_writer.writerows(rows)
                self.csv_file.flush()
                logger.info(f"成功将 {len(rows)} 条数据备份到CSV文件")
            except Exception as e:
                logger.error(f"写入CSV文件时发生错误: {e}")
        else:
            logger.warning("CSV写入器未就绪，跳过本次文件备份")

    def close(self):
        """关闭文件句柄，释放资源。"""
        if self.csv_file:
            logger.info(f"正在关闭文件: {self.csv_file.name}")
            self.csv_file.close()
            self.csv_file = None
            self.csv_writer = None
            logger.info("CSV文件已彻底关闭")
