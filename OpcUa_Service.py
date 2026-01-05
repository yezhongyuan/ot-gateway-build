import asyncio
import datetime
import logging
import sys
import time
import signal
import os
from logging.handlers import RotatingFileHandler
from typing import List, Any
import uuid
import json

# 第三方库: pip install asyncua aiomysql loguru
import aiomysql
from asyncua import Client, ua
from asyncua.common.node import Node
from loguru import logger

# 导入我们新创建的归档器类
from file_archiver import DailyFileArchiver

# ================= 配置区域 (建议生产环境放入 .env 文件) =================

# OPC UA 服务端地址
OPC_URL = "opc.tcp://172.21.254.50:49320"
# OPC_URL = "opc.tcp://localhost:4840"

# 数据库连接配置
# DB_CONFIG = {
#     'host': '127.0.0.1',
#     'port': 3306,
#     'user': 'root',
#     'password': '123456',  # 请修改密码
#     'db': 'test01',
#     'autocommit': True,
#     'connect_timeout': 10
# }

DB_CONFIG = {
    'host': '172.21.30.150',
    'port': 3306,
    'user': 'root',
    'password': '1qaz@WSX',  # 请修改密码
    'db': 'kpsnc_upom_mes',
    'autocommit': True,
    'connect_timeout': 10
}

# 性能调优参数
BATCH_SIZE = 500  # 批量写入阈值：攒够500条写一次数据库
FLUSH_INTERVAL = 5.0  # 时间阈值：每3秒强制写入一次（防止数据少时不写入）
QUEUE_MAX_SIZE = 50000  # 内存队列最大长度，防止内存溢出
LOG_DIR = "logs"  # 日志存放目录


# ================= 1. 日志初始化与配置 =================

def setup_iot_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logger.remove()

    # A. 控制台
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
               "| <level>{level: <8}</level> "
               "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
               "| <level>{message}</level>",
        level="DEBUG",
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


def load_target_nodes_config(config_path: str = "nodes_config.json") -> dict:
    if getattr(sys, 'frozen', False):
        config_path = os.path.join(os.path.dirname(sys.executable), "nodes_config.json")
    """从JSON文件加载目标节点配置"""
    if not os.path.exists(config_path):
        # 使用 logger.error 替换 logging.error
        logger.error(f"目标节点配置文件 '{config_path}' 未找到！程序将无法采集任何数据。")
        return {}

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        # 使用 logger.error 替换 logging.error
        logger.error(f"解析配置文件 '{config_path}' 时出错 (JSON格式错误): {e}")
        return {}
    except Exception as e:
        # 使用 logger.error 替换 logging.error
        logger.error(f"读取配置文件 '{config_path}' 时发生未知错误: {e}")
        return {}


def log_config_summary(nodes_config: dict):
    """
    打印加载的设备和点位配置摘要。
    """
    if not nodes_config:
        logger.warning("没有加载到任何设备配置，将不打印配置摘要。")
        return

    logger.info("=" * 60)
    logger.info("         设备与点位配置加载摘要")
    logger.info("=" * 60)

    for device_name, device_info in nodes_config.items():
        # 打印设备名称
        logger.info(f"  设备名称: {device_name}")

        # 打印触发点位
        trigger_node = device_info.get("trigger")
        if trigger_node:
            logger.info(f"    └─ 触发点位: {trigger_node}")
        else:
            logger.warning(f"    └─ 警告: 设备 '{device_name}' 未配置触发点位！")

        # 打印关联的采集点位
        items = device_info.get("items", [])
        if items:
            logger.info(f"    └─ 采集点位 ({len(items)}个):")
            for i, item_node in enumerate(items, 1):
                logger.info(f"        [{i}] {item_node}")
        else:
            logger.warning(f"    └─ 警告: 设备 '{device_name}' 未配置任何采集点位！")

        logger.info("-" * 40)  # 打印分隔线，使输出更清晰

    logger.info("=" * 60)
    logger.info(f"配置摘要打印完毕，共加载 {len(nodes_config)} 个设备。")
    logger.info("=" * 60)


# --- 在程序启动时加载配置 ---
TARGET_NODES = load_target_nodes_config()
TRIGGER_TO_DEVICE = {v["trigger"]: k for k, v in TARGET_NODES.items()}

# --- 在加载配置后打印读取信息 ---
log_config_summary(TARGET_NODES)


# ================= 2. 数据库写入服务 (消费者) =================

class DatabaseService:
    def __init__(self, queue: asyncio.Queue, archiver: DailyFileArchiver):
        self.queue = queue
        self.pool = None
        self._running = True
        self.archiver = archiver  # 保存归档器实例

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

        # === 新增：打印每条记录的简要信息 ===
        for i, record in enumerate(buffer):
            rfid, node_id, value, quality, source_time, trace_id = record
            logger.debug(
                f"准备写入 [{i + 1}/{len(buffer)}] | "
                f"设备: {node_id} | "
                f"RFID: {rfid} | "
                f"值: {value} | "
                f"时间: {source_time} | "
                f"Trace: {trace_id}"
            )

        sql = "INSERT INTO iot_sensor_data (rfid, node_id, value, quality, source_time, trace_id) VALUES (%s, %s, %s, %s, %s, %s)"
        buffer_to_write = buffer.copy()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(sql, buffer)
                    logger.info(f"成功存入 {len(buffer)} 条数据")

            # 2. 数据库写入成功后，立即在后台发起一个CSV写入任务 (非阻塞)
            # 这样文件写入不会影响 flush_data 的返回时间和返回值
            asyncio.create_task(self._write_to_csv(buffer_to_write))
            return True
        except Exception as e:
            logger.error(f"数据库写入报错: {e}")
            # 即使数据库写入失败，也可以选择备份数据到CSV，方便排查问题
            logger.warning(f"数据库写入失败，尝试将 {len(buffer_to_write)} 条数据备份到CSV...")
            asyncio.create_task(self._write_to_csv(buffer_to_write))
            return False

    async def _write_to_csv(self, buffer: List[tuple]):
        """一个专门用于异步写入CSV的辅助方法"""
        try:
            self.archiver.write_rows(buffer)
        except Exception as e:
            # 记录文件写入的错误，但不影响主流程
            logger.error(f"后台CSV写入任务发生错误: {e}")

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

    def __init__(self, queue: asyncio.Queue, client, loop: asyncio.AbstractEventLoop):
        self.queue = queue
        self.loop = loop
        self.client = client
        self.last_values = {}  # 记录上次信号状态

    def datachange_notification(self, node: Node, val: Any, data):

        # === 新增：为本次触发生成唯一ID ===
        trace_id = str(uuid.uuid4())  # 取前8位足够区分，如 "a1b2c3d4"

        # === 新增：打印原始 Variant 信息 ===
        variant = data.monitored_item.Value
        logger.debug(f"[{trace_id}] 原始信号 Variant: Value={variant.Value!r}, "f"StatusCode={variant.StatusCode}")

        """
        当 PLC 点位数据变化时，自动触发此函数
        """
        try:

            node_id = node.nodeid.to_string()
            prev_val = self.last_values.get(node_id)
            self.last_values[node_id] = val

            device_name = TRIGGER_TO_DEVICE.get(node_id)
            if not device_name:
                return

            triggered = False

            logger.info(f"[{trace_id}] 🔔 信号变化 | 设备: {device_name} | 点位: {node.nodeid.to_string()} | 新值: {val!r} | 旧值: {prev_val!r}")

            if isinstance(val, bool):
                # === 布尔模式：上升沿触发 ===
                if val is True and (prev_val is False or prev_val is None):
                    triggered = True

            elif isinstance(val, str) or val is None:
                # === 字符串模式：单件码变更触发 ===
                current_clean = (val or "").strip()  # None → "", 然后 strip
                prev_clean = prev_val.strip() if isinstance(prev_val, str) else ""

                # === 新增：始终打印原始值和清洗后值（用于排查）===
                logger.debug(
                    f"[{trace_id}] 字符串点位变更 | 节点: {node_id} | "
                    f"原始新值: {val!r} → 清洗后: {current_clean!r} | "
                    f"原始旧值: {prev_val!r} → 清洗后: {prev_clean!r}"
                )
                # 触发条件：当前非空 且 与上次不同
                if current_clean != "" and current_clean != prev_clean:
                    triggered = True

            else:
                # 可选：忽略其他类型，或按需扩展（如 int 状态码）
                logger.error(f"忽略非 bool/str 类型信号: {type(val).__name__} = {val!r}")
                return

            if triggered:
                logger.info(f"[{trace_id}] 🔔 触发队列事件 | 设备: {device_name} | 点位: {node.nodeid.to_string()} | " f"新值: {val!r} | 旧值: {prev_val!r}")
                # 跨线程派发异步任务
                asyncio.run_coroutine_threadsafe(self.read_associated_data(device_name, val, trace_id), self.loop)

        except Exception as e:
            logger.error(f"回调处理异常: {e}")

    async def read_associated_data(self, device_name, rfid, trace_id):

        try:
            config = TARGET_NODES.get(device_name)
            if not config: return

            nodes = [self.client.get_node(nid) for nid in config["items"]]

            # 批量读取 + 超时保护
            values = await asyncio.wait_for(self.client.read_values(nodes), timeout=3.0)

            # 3. 【核心修改】：提取点位名字并构建字典
            # 比如从 "ns=2;s=DeviceA.Temp" 中提取出 "Temp"
            data_dict = {}
            for i, node_id in enumerate(config["items"]):
                # 技巧：取点位字符串最后一部分作为 Key
                key = node_id.split('.')[-1]
                val = values[i]
                # 转换数值类型：如果是 float 则保留两位小数
                data_dict[key] = round(val, 2) if isinstance(val, (float, int)) else val

            # 4. 转换成标准的 JSON 字符串
            try:
                json_payload = json.dumps(data_dict, ensure_ascii=False)
            except Exception as e:
                logger.error(f"[{trace_id}] JSON 序列化失败: {e}, 原始数据: {data_dict}")
                return

            # 5. 入队 (格式: 设备名, JSON内容, 质量, 时间)
            payload = (
                rfid,
                device_name,
                json_payload,
                "Good",
                datetime.datetime.now(),
                trace_id
            )

            try:
                self.queue.put_nowait(payload)
                logger.info(f"[{trace_id}] ✅ {device_name} 完工信号：{rfid} 数据已入队: {json_payload}")
            except asyncio.QueueFull:
                logger.warning("[{trace_id}] 警告: 内存队列已满！数据库写入速度跟不上采集速度，正在丢弃数据！")
        except asyncio.TimeoutError:
            logger.error(f"[{trace_id}] ❌ {device_name} 读取超时")
        except Exception as e:
            logger.error(f"[{trace_id}] 读取任务异常: {e}")

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
                self.client.session_timeout = 60000  # 显式设为 60 秒
                # 生产环境安全设置 (如需账号密码请取消注释)
                # self.client.set_user("admin")
                # self.client.set_password("123456")

                async with self.client:
                    logger.info("已连接至 OPC UA Server")

                    # 1. 注册 Namespace (可选，部分 PLC 需要)
                    # ns = await self.client.get_namespace_index(uri)

                    # 1. 提取所有的 trigger 节点进行订阅
                    trigger_nodes = [self.client.get_node(v["trigger"]) for v in TARGET_NODES.values()]

                    # 2. 创建订阅
                    handler = SubscriptionHandler(self.queue, self.client, self.loop)
                    sub = await self.client.create_subscription(500, handler)

                    # 3. 订阅所有触发信号
                    await sub.subscribe_data_change(trigger_nodes)
                    logger.info(f"📡 已订阅 {len(trigger_nodes)} 个设备的触发信号")

                    # --- 核心心跳监控 ---
                    while self._running:
                        try:
                            # 1. 尝试读取服务器当前时间或状态，这是最实时的链路检测
                            # await self.client.nodes.server_state.read_value()

                            # 使用标准强制节点 i=2259 (Server_ServerStatus_CurrentTime)
                            # 这在 Siemens, Beckhoff, Omron 等所有标准 PLC 上都存在
                            server_time_node = self.client.get_node(
                                ua.NodeId(ua.ObjectIds.Server_ServerStatus_CurrentTime))
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

    # --- 关键修改：在这里创建 DailyFileArchiver 实例 ---
    csv_headers = ['rfid', 'node_id', 'value', 'quality', 'source_time', 'trace_id']
    file_archiver = DailyFileArchiver(base_filename="iot_backup", headers=csv_headers)

    # 2. 实例化服务
    db_service = DatabaseService(data_queue, file_archiver)
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

    # 4. 启动任务，使用 create_task 将它们放入后台运行
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
