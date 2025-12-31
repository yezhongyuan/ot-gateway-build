import asyncio
import logging
import sys
import time
import signal
import os
from logging.handlers import RotatingFileHandler
from typing import List, Any

# 第三方库: pip install asyncua aiomysql loguru
import aiomysql
from asyncua import Client, ua
from asyncua.common.node import Node
from loguru import logger

# ================= 配置区域 (建议生产环境放入 .env 文件) =================

# OPC UA 服务端地址
OPC_URL = "opc.tcp://localhost:4840"

# 需要订阅采集的点位列表 (NodeID)
TARGET_NODES = [
    "ns=2;i=2",  # 温度
]

# 数据库连接配置
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',  # 请修改密码
    'db': 'test01',
    'autocommit': True,
    'connect_timeout': 10
}

# 性能调优参数
BATCH_SIZE = 500  # 批量写入阈值：攒够500条写一次数据库
FLUSH_INTERVAL = 5.0  # 时间阈值：每3秒强制写入一次（防止数据少时不写入）
QUEUE_MAX_SIZE = 50000  # 内存队列最大长度，防止内存溢出
LOG_DIR = "logs"  # 日志存放目录


# ================= 1. 生产级日志系统设置 =================

# def setup_logging():
#     """
#     配置日志系统：同时输出到控制台和文件
#     使用 RotatingFileHandler 实现日志轮转，防止磁盘写满
#     """
#     if not os.path.exists(LOG_DIR):
#         os.makedirs(LOG_DIR)
#
#     # 日志格式: 时间 - 模块名 - 级别 - 内容
#     log_format = logging.Formatter("%(asctime)s - %(name)s - [%(levelname)s] - %(message)s")
#
#     logger = logging.getLogger("IOT_Core")
#     logger.setLevel(logging.INFO)
#
#     # 1. 控制台处理器
#     stream_handler = logging.StreamHandler(sys.stdout)
#     stream_handler.setFormatter(log_format)
#     logger.addHandler(stream_handler)
#
#     # 2. 文件处理器 (日志轮转)
#     # maxBytes=10MB: 单个日志最大10MB
#     # backupCount=5: 保留最近5个日志文件
#     file_handler = RotatingFileHandler(
#         filename=os.path.join(LOG_DIR, "service.log"),
#         maxBytes=10 * 1024 * 1024,
#         backupCount=5,
#         encoding='utf-8'
#     )
#     file_handler.setFormatter(log_format)
#     logger.addHandler(file_handler)
#
#     return logger
#
#
# logger = setup_logging()
# ================= 1. 日志初始化与配置 =================

def setup_iot_logging():
    LOG_DIR = "logs"
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logger.remove()

    # A. 控制台
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        enqueue=True,
        colorize=True
    )

    # B. 主服务日志 (按天滚动 + 压缩)
    logger.add(
        os.path.join(LOG_DIR, "iot_service_{time:YYYY-MM-DD}.log"),
        rotation="00:00",
        retention="30 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        enqueue=True,
        encoding="utf-8"
    )

    # C. 错误日志独立备份 (修正点：retention=10)
    logger.add(
        os.path.join(LOG_DIR, "error_critical.log"),
        level="ERROR",
        rotation="10 MB",
        retention=10,  # 保留最近10个文件
        enqueue=True
    )


setup_iot_logging()


# ================= 2. 数据库写入服务 (消费者) =================

class DatabaseService:
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.pool = None
        self._running = True

    async def _init_pool(self):
        """建立或重建数据库连接池 (保持不变，很稳)"""
        while self._running:
            try:
                if self.pool:
                    self.pool.close()
                    await self.pool.wait_closed()
                self.pool = await aiomysql.create_pool(**DB_CONFIG, minsize=1, maxsize=10)
                logger.info(">>> 数据库连接池初始化成功")
                return True
            except Exception as e:
                logger.error(f"数据库连接失败: {e}，5秒后重试...")
                await asyncio.sleep(5)
        return False

    async def flush_data(self, buffer: List[tuple]) -> bool:
        """纯粹执行写入，不成功则返回 False"""
        if not buffer or not self.pool:
            return False

        sql = "INSERT INTO iot_sensor_data (node_id, value, quality, source_time) VALUES (%s, %s, %s, %s)"
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(sql, buffer)
                    logger.info(f"成功存入 {len(buffer)} 条数据")
                    return True
        except Exception as e:
            logger.error(f"数据库写入报错: {e}")
            return False

    async def run(self):
        """消费者主循环"""
        await self._init_pool()
        buffer = []
        last_flush_time = time.time()
        try:
            while self._running:
                try:
                    # 1. 尝试从队列取数据
                    try:
                        item = await asyncio.wait_for(self.queue.get(), timeout=0.5)
                        buffer.append(item)
                    except asyncio.TimeoutError:
                        pass

                        # 2. 检查触发条件
                    current_time = time.time()
                    if buffer and (len(buffer) >= BATCH_SIZE or (current_time - last_flush_time >= FLUSH_INTERVAL)):
                        # 尝试写入
                        if await self.flush_data(buffer):
                            # 成功才清空
                            for _ in range(len(buffer)): self.queue.task_done()
                            buffer = []
                            last_flush_time = current_time
                        else:
                            # 失败则不清除 buffer，触发连接池重建，进入下一次循环重试
                            logger.warning("数据暂存缓冲区，正在尝试恢复连接池...")
                            await self._init_pool()
                            await asyncio.sleep(2)

                except asyncio.CancelledError:
                    # 特别处理任务取消信号，确保它能抛出以便进入 finally
                    raise
                except Exception as e:
                    logger.error(f"DatabaseService 意外错误: {e}")
                    await asyncio.sleep(1)

        finally:
            # === 只要 run 结束，无论是正常停止还是报错，都会执行这里 ===
            logger.info("正在执行 DatabaseService 收尾清理...")

            # 1. 尝试刷写缓冲区剩余数据
            if buffer:
                logger.warning(f"服务停止，正在刷写剩余 {len(buffer)} 条数据...")
                # 注意：如果此时数据库本来就是断开的，这里可能会再次失败
                # 工业级应用可以考虑在这里将 buffer 写入本地文本文件
                await self.flush_data(buffer)

            # 2. 安全关闭连接池
            if self.pool:
                logger.info("正在关闭数据库连接池...")
                self.pool.close()
                await self.pool.wait_closed()
                logger.info("数据库连接池已彻底关闭")

    def stop(self):
        self._running = False


# ================= 3. OPC UA 订阅处理 (生产者) =================

class SubscriptionHandler:
    """
    OPC UA 订阅回调类
    注意: 这里的代码运行在 asyncua 的回调线程中，必须非常快，不能阻塞
    """

    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self.queue = queue
        self.loop = loop

    def datachange_notification(self, node: Node, val: Any, data):
        logger.info(f"收到原始信号: {node.nodeid.to_string()} = {val}")
        """
        当 PLC 点位数据变化时，自动触发此函数
        """
        try:
            node_id = node.nodeid.to_string()

            # 获取数据源时间戳 (SourceTimestamp)，这是数据产生的真实时间
            source_ts = data.monitored_item.Value.SourceTimestamp
            # 获取质量代码
            quality = str(data.monitored_item.Value.StatusCode)

            # 如果是 datetime 对象，确保它是 UTC 或者本地时间，这里直接存入
            # 这里的 val 需要转为字符串，保证兼容性
            payload = (node_id, str(val), quality, source_ts)

            # put_nowait 是非阻塞的
            # 如果队列满了 (超过 QUEUE_MAX_SIZE)，会抛出 QueueFull 异常
            # self.queue.put_nowait(payload)

            # 【豆包补丁】：线程安全地将数据投递回 asyncio 事件循环
            def put_into_queue():
                try:
                    self.queue.put_nowait(payload)
                except asyncio.QueueFull:
                    logger.warning("警告: 内存队列已满！数据库写入速度跟不上采集速度，正在丢弃数据！")

            self.loop.call_soon_threadsafe(put_into_queue)

        except Exception as e:
            logger.error(f"回调处理异常: {e}")

    def status_change_notification(self, status):
        """
        处理订阅状态变更（如连接中断、Session失效等）
        不再指定参数类型，防止版本不兼容
        """
        logger.warning(f"OPC UA 订阅状态变更: {status}")

    def event_notification(self, event):
        """
        处理事件通知（如报警）
        不再指定 ua.EventNotificationElement，防止 AttributeError
        """
        logger.info(f"收到 OPC UA 事件通知: {event}")


# ================= 4. OPC UA 客户端主服务 =================

class OpcUaService:
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self.queue = queue
        self.loop = loop
        self.client = None
        self._running = True

    async def run(self):
        """客户端主循环 (包含断线重连)"""
        logger.info(f"正在启动 OPC UA 采集服务，目标: {OPC_URL}")

        while self._running:
            self.client = None  # 确保每次循环开始前引用是干净的
            try:
                self.client = Client(url=OPC_URL)
                # 设置连接超时，防止网络死掉时程序永久卡死在 connect()
                self.client.connect_timeout = 10
                # 生产环境安全设置 (如需账号密码请取消注释)
                # self.client.set_user("admin")
                # self.client.set_password("123456")

                async with self.client:
                    logger.info("已连接至 OPC UA Server")

                    # 1. 注册 Namespace (可选，部分 PLC 需要)
                    # ns = await self.client.get_namespace_index(uri)

                    # 2. 建立订阅
                    handler = SubscriptionHandler(self.queue, self.loop)
                    # 500ms 扫描一次变化，如果这里设太快，PLC负载会变高
                    sub = await self.client.create_subscription(500, handler)

                    # 3. 获取点位节点对象
                    nodes = []
                    for node_str in TARGET_NODES:
                        try:
                            n = self.client.get_node(node_str)
                            nodes.append(n)
                        except Exception as e:
                            logger.error(f"无效的点位 ID: {node_str} - {e}")

                    if not nodes:
                        logger.error("没有有效的点位，等待重试...")
                        await asyncio.sleep(5)
                        continue

                    # 4. 订阅数据变化
                    await sub.subscribe_data_change(nodes)
                    logger.info(f"成功订阅 {len(nodes)} 个点位，进入监听模式...")

                    # --- 核心心跳监控 ---
                    while self._running:
                        try:
                            # 1. 尝试读取服务器当前时间或状态，这是最实时的链路检测
                            # await self.client.nodes.server_state.read_value()

                            # 使用标准强制节点 i=2259 (Server_ServerStatus_CurrentTime)
                            # 这在 Siemens, Beckhoff, Omron 等所有标准 PLC 上都存在
                            server_time_node = self.client.get_node(ua.NodeId(ua.ObjectIds.Server_ServerStatus_CurrentTime))
                            await server_time_node.read_value()

                        except Exception as e:
                            # 如果这里读失败了，说明 TCP 连接已经不可用了
                            logger.error(f"心跳检测失败，连接可能已断开: {e}")
                            break  # 跳出内循环，进入 finally 进行清理并重连

                        await asyncio.sleep(5)  # 每 5 秒心跳一次

            except (OSError, asyncio.TimeoutError, ua.UaError) as e:
                logger.error(f"连接断开或网络错误: {e}")
            except Exception as e:
                logger.critical(f"严重未知错误: {e}", exc_info=True)
            finally:
                if self.client:
                    try:
                        logger.info("正在强制断开并清理残留连接资源...")
                        await self.client.disconnect()
                    except:
                        logger.info("断网情况下 disconnect 报错是正常的...")
                        pass  # 断网情况下 disconnect 报错是正常的

                if self._running:
                    logger.warning("5 秒后尝试重新连接...")
                    await asyncio.sleep(5)

        logger.info("OPC UA 服务循环已结束")

    def stop(self):
        self._running = False


# ================= 4. 队列监控任务 =================
async def monitor_queue_task(queue: asyncio.Queue, max_size: int):
    """
    专门的监控任务，不影响业务逻辑
    每 10 秒打印一次队列堆积情况
    """
    while True:
        try:
            q_size = queue.qsize()
            if q_size > 0:
                usage_pct = (q_size / max_size) * 100
                # 如果负载超过 80%，用 warning 级别提醒
                level = "WARNING" if usage_pct > 80 else "INFO"
                logger.log(level, f"📊 队列健康监控: 当前堆积 {q_size} 条 | 负载率 {usage_pct:.2f}%")

            await asyncio.sleep(10)  # 每10秒检查一次
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"监控任务异常: {e}")
            await asyncio.sleep(10)


# ================= 5. 程序入口与生命周期管理 =================

async def main():
    loop = asyncio.get_running_loop()
    # 1. 创建共享队列 (限制大小为5万条，防止内存炸裂)
    # 假设一条数据占用 1KB，5万条约占用 50MB 内存，非常安全
    data_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

    # 2. 实例化服务
    db_service = DatabaseService(data_queue)
    opc_service = OpcUaService(data_queue, loop)

    # 3. 注册信号处理 (用于优雅退出)
    #loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        logger.warning("接收到停止信号 (SIGINT/SIGTERM)，正在准备安全退出...")
        opc_service.stop()
        # 注意：这里不立即停止 DB 服务，要让它把队列里的写完
        stop_event.set()

    # 注册 Ctrl+C
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows 下可能不支持 add_signal_handler，这只是一个警告
            pass

    # 4. 启动任务
    # 使用 create_task 将它们放入后台运行
    task_monitor = asyncio.create_task(monitor_queue_task(data_queue, QUEUE_MAX_SIZE))
    task_db = asyncio.create_task(db_service.run())
    task_opc = asyncio.create_task(opc_service.run())

    # 5. 等待停止信号
    # 在 Windows 下如果不支持信号，这里可能需要改成 loop.run_forever() 的变体
    try:
        # 主线程在这里挂起，直到收到停止信号
        while not stop_event.is_set():
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        # Windows 兼容处理
        signal_handler()

    # 6. 退出流程
    logger.info("正在等待任务结束...")

    # 首先停止数据库消费者的运行标志，让它处理完剩余数据后退出循环
    db_service.stop()

    # 等待数据库任务彻底完成 (包括最后一次 Flush)
    await task_db

    task_monitor.cancel()
    # 取消 OPC 任务 (因为 OPC 任务通常在 sleep，可以直接 cancel)
    task_opc.cancel()

    logger.info("服务已安全关闭 (All Services Stopped).")


if __name__ == "__main__":
    # Windows 平台下的 asyncio 策略修复
    if sys.platform.lower() == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # 已经处理过了，这里忽略
