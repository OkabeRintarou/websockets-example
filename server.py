"""
WebSocket 服务器 v3
- 自动负载均衡
- 错误感知和健康检查
- 错误衰减机制
- 时间窗口统计
- 客户端 IP/Port 信息存储 ⭐ 新增
"""
import asyncio
import logging
import uuid
import time
from typing import Dict, Callable, Optional, List
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from websockets.server import serve, WebSocketServerProtocol
import websockets

import hashlib
import secrets
import json

from message_protocol import (
    Message, MessageType, create_notification, 
    create_response, create_error, create_heartbeat,
    create_request
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class ClientStatus(str, Enum):
    """客户端状态"""
    HEALTHY = "healthy"      # 健康
    DEGRADED = "degraded"    # 降级（有错误但可用）
    UNHEALTHY = "unhealthy"  # 不健康（错误过多）
    OFFLINE = "offline"      # 离线


@dataclass
class RequestRecord:
    """请求记录"""
    timestamp: float
    success: bool
    response_time: float = 0.0


@dataclass
class ClientMetrics:
    """客户端指标（v3：增加连接信息存储）"""
    client_id: str
    
    # 客户端连接信息 ⭐ 新增
    remote_ip: str = ""
    remote_port: int = 0
    local_port: int = 0
    connect_time: float = 0.0
    
    # 使用 deque 存储最近的请求记录（时间窗口）
    recent_requests: deque = field(default_factory=lambda: deque(maxlen=100))
    
    last_error_time: float = 0
    last_success_time: float = 0
    status: ClientStatus = ClientStatus.HEALTHY
    weight: float = 1.0
    
    # 配置参数
    time_window: int = 300  # 时间窗口：5分钟
    error_decay_rate: float = 0.1  # 错误衰减率：每分钟衰减10%
    
    @property
    def connection_duration(self) -> float:
        """连接持续时间（秒）"""
        if self.connect_time > 0:
            return time.time() - self.connect_time
        return 0.0
    
    @property
    def recent_error_rate(self) -> float:
        """计算时间窗口内的错误率"""
        if not self.recent_requests:
            return 0.0
        
        current_time = time.time()
        cutoff_time = current_time - self.time_window
        
        # 只统计时间窗口内的请求
        valid_requests = [
            req for req in self.recent_requests 
            if req.timestamp > cutoff_time
        ]
        
        if not valid_requests:
            return 0.0
        
        error_count = sum(1 for req in valid_requests if not req.success)
        return error_count / len(valid_requests)
    
    @property
    def total_requests(self) -> int:
        """总请求数"""
        return len(self.recent_requests)
    
    @property
    def success_count(self) -> int:
        """成功数"""
        return sum(1 for req in self.recent_requests if req.success)
    
    @property
    def error_count(self) -> int:
        """错误数"""
        return sum(1 for req in self.recent_requests if not req.success)
    
    @property
    def avg_response_time(self) -> float:
        """平均响应时间（最近10次）"""
        recent_10 = list(self.recent_requests)[-10:]
        if not recent_10:
            return 0.0
        
        response_times = [req.response_time for req in recent_10 if req.success]
        if not response_times:
            return 0.0
        
        return sum(response_times) / len(response_times)
    
    def update_status(self):
        """
        更新客户端状态（基于时间窗口内的错误率）
        
        改进：
        1. 只看最近的错误率，不受历史影响
        2. 自动清理过期数据
        3. 动态调整阈值
        """
        error_rate = self.recent_error_rate
        
        # 如果最近有成功请求，且错误率不高，提升健康度
        current_time = time.time()
        time_since_last_success = current_time - self.last_success_time if self.last_success_time > 0 else float('inf')
        
        # 如果最近1分钟内有成功请求
        if time_since_last_success < 60:
            # 降低错误率的影响
            if error_rate >= 0.5:
                self.status = ClientStatus.DEGRADED  # 放宽：原本是 UNHEALTHY
                self.weight = 0.6
            elif error_rate >= 0.3:
                self.status = ClientStatus.HEALTHY  # 放宽：原本是 DEGRADED
                self.weight = 0.8
            else:
                self.status = ClientStatus.HEALTHY
                self.weight = 1.0
        else:
            # 标准评估
            if error_rate >= 0.6:  # 提高阈值：从50%到60%
                self.status = ClientStatus.UNHEALTHY
                self.weight = 0.2  # 提高权重：从0.1到0.2
            elif error_rate >= 0.3:  # 提高阈值：从20%到30%
                self.status = ClientStatus.DEGRADED
                self.weight = 0.6  # 提高权重：从0.5到0.6
            else:
                self.status = ClientStatus.HEALTHY
                self.weight = 1.0
        
        logger.debug(
            f"客户端 {self.client_id} 状态更新: "
            f"错误率={error_rate*100:.1f}%, 状态={self.status.value}, 权重={self.weight}"
        )
    
    def record_success(self, response_time: float):
        """记录成功请求"""
        self.recent_requests.append(RequestRecord(
            timestamp=time.time(),
            success=True,
            response_time=response_time
        ))
        self.last_success_time = time.time()
        self.update_status()
    
    def record_error(self):
        """记录错误请求"""
        self.recent_requests.append(RequestRecord(
            timestamp=time.time(),
            success=False,
            response_time=0.0
        ))
        self.last_error_time = time.time()
        self.update_status()
    
    def cleanup_old_records(self):
        """清理过期的请求记录（在时间窗口之外）"""
        current_time = time.time()
        cutoff_time = current_time - self.time_window
        
        # 找到第一个未过期的记录
        while self.recent_requests and self.recent_requests[0].timestamp < cutoff_time:
            self.recent_requests.popleft()
        
        # 重新评估状态
        self.update_status()
    
    def reset_metrics(self):
        """重置指标"""
        self.recent_requests.clear()
        self.last_error_time = 0
        self.last_success_time = 0
        self.update_status()
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            "remote_ip": self.remote_ip,
            "remote_port": self.remote_port,
            "local_port": self.local_port,
            "connection_duration": f"{self.connection_duration:.0f}s",
            "total_requests": self.total_requests,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "recent_error_rate": f"{self.recent_error_rate*100:.1f}%",
            "avg_response_time": f"{self.avg_response_time*1000:.1f}ms",
            "status": self.status.value,
            "weight": self.weight,
            "time_window": f"{self.time_window}s"
        }


class LoadBalancer:
    """负载均衡器"""
    
    def __init__(self, strategy: str = "least_loaded"):
        self.strategy = strategy
        self.round_robin_index = 0
    
    def select_client(self, metrics: Dict[str, ClientMetrics]) -> Optional[str]:
        """
        选择一个客户端
        
        改进：
        1. 优先选择 HEALTHY 客户端
        2. 如果没有 HEALTHY，才选择 DEGRADED
        3. 只有在极端情况下才选择 UNHEALTHY
        """
        # 按健康度分组
        healthy_clients = {
            cid: m for cid, m in metrics.items()
            if m.status == ClientStatus.HEALTHY
        }
        
        degraded_clients = {
            cid: m for cid, m in metrics.items()
            if m.status == ClientStatus.DEGRADED
        }
        
        unhealthy_clients = {
            cid: m for cid, m in metrics.items()
            if m.status == ClientStatus.UNHEALTHY
        }
        
        # 优先级选择
        if healthy_clients:
            selected_pool = healthy_clients
            logger.debug(f"从 {len(healthy_clients)} 个健康客户端中选择")
        elif degraded_clients:
            selected_pool = degraded_clients
            logger.info(f"⚠️  无健康客户端，从 {len(degraded_clients)} 个降级客户端中选择")
        elif unhealthy_clients:
            selected_pool = unhealthy_clients
            logger.warning(f"⚠️  无健康/降级客户端，从 {len(unhealthy_clients)} 个不健康客户端中选择")
        else:
            logger.error("❌ 没有任何可用客户端")
            return None
        
        # 根据策略选择
        if self.strategy == "round_robin":
            return self._round_robin(list(selected_pool.keys()))
        elif self.strategy == "least_loaded":
            return self._least_loaded(selected_pool)
        elif self.strategy == "random":
            import random
            return random.choice(list(selected_pool.keys()))
        elif self.strategy == "weighted_random":
            return self._weighted_random(selected_pool)
        else:
            return self._least_loaded(selected_pool)
    
    def _round_robin(self, client_ids: List[str]) -> str:
        """轮询策略"""
        client_id = client_ids[self.round_robin_index % len(client_ids)]
        self.round_robin_index += 1
        return client_id
    
    def _least_loaded(self, metrics: Dict[str, ClientMetrics]) -> str:
        """最少负载策略（考虑权重、响应时间和请求计数）"""
        # 综合考虑权重、响应时间和请求计数
        def score(m: ClientMetrics) -> float:
            # 请求计数惩罚：防止单个客户端被过度使用
            request_count = len(m.recent_requests)
            request_penalty = request_count * 0.1
            # 权重越高越好，响应时间越低越好
            time_factor = 1.0 / (1.0 + m.avg_response_time) if m.avg_response_time > 0 else 1.0
            # 综合得分：权重 * 时间因子 / (1 + 请求惩罚)
            return m.weight * time_factor / (1.0 + request_penalty)
        
        return max(metrics.items(), key=lambda x: score(x[1]))[0]

    def _weighted_random(self, metrics: Dict[str, ClientMetrics]) -> str:
        """加权随机策略"""
        import random
        
        clients = list(metrics.items())
        weights = [m.weight for _, m in clients]
        
        # 确保权重都大于0
        if sum(weights) == 0:
            weights = [1.0] * len(clients)
        
        selected = random.choices(clients, weights=weights, k=1)[0]
        return selected[0]


class WebSocketServer:
    """WebSocket 服务器"""
    
    def __init__(
        self, 
        host: str = "0.0.0.0", 
        port: int = 8765,
        requester_port: int = 8766,  # Requester 端口
        lb_strategy: str = "least_loaded",
        cleanup_interval: int = 60  # 清理间隔（秒）
    ):
        self.host = host
        self.port = port
        self.requester_port = requester_port
        self.clients: Dict[str, WebSocketServerProtocol] = {}
        self.requester_connections: Dict[str, WebSocketServerProtocol] = {}  # Requester 客户端连接
        self.requester_pending_requests: Dict[str, asyncio.Future] = {}  # Requester 待处理请求
        self.metrics: Dict[str, ClientMetrics] = {}
        self.load_balancer = LoadBalancer(strategy=lb_strategy)
        self.cleanup_interval = cleanup_interval
        
        # 请求追踪
        self.pending_requests: Dict[str, float] = {}
        
        # 消息处理器映射
        self.handlers: Dict[MessageType, Callable] = {
            MessageType.REQUEST: self._handle_request,
            MessageType.HEARTBEAT: self._handle_heartbeat,
            MessageType.RESPONSE: self._handle_response,
        }
        
        # 请求处理器映射
        self.request_handlers: Dict[str, Callable] = {
            "get_time": self._action_get_time,
            "get_client_info": self._action_get_client_info,
            "calculate": self._action_calculate,
            "get_metrics": self._action_get_metrics,
        }
    
    # ============ 定期清理任务 ============
    
    async def cleanup_loop(self):
        """定期清理过期数据"""
        while True:
            await asyncio.sleep(self.cleanup_interval)
            
            logger.debug("执行定期清理...")
            for client_id, metrics in self.metrics.items():
                if client_id in self.clients:  # 只清理在线客户端
                    metrics.cleanup_old_records()
            
            logger.debug(f"清理完成，当前在线: {len(self.clients)}, 健康: {self._count_healthy_clients()}")
    
    # ============ 客户端连接管理 ============
    
    async def register_client(self, ws: WebSocketServerProtocol) -> str:
        """注册新客户端（v3：存储连接信息）"""
        client_id = str(uuid.uuid4())[:8]
        self.clients[client_id] = ws
        
        # 获取客户端连接信息 ⭐ 新增
        remote_address = ws.remote_address  # (ip, port)
        local_address = ws.local_address    # (ip, port)
        remote_ip = remote_address[0] if remote_address else "unknown"
        remote_port = remote_address[1] if remote_address else 0
        local_port = local_address[1] if local_address else 0
        
        # 创建客户端指标，包含连接信息
        self.metrics[client_id] = ClientMetrics(
            client_id=client_id,
            remote_ip=remote_ip,
            remote_port=remote_port,
            local_port=local_port,
            connect_time=time.time()
        )
        
        logger.info(
            f"✓ 客户端 {client_id} 已连接 "
            f"[{remote_ip}:{remote_port} -> :{local_port}] "
            f"[在线: {len(self.clients)}, 健康: {self._count_healthy_clients()}]"
        )
        
        welcome_msg = create_notification(
            "连接成功",
            f"你的客户端ID: {client_id}\n来自: {remote_ip}:{remote_port}",
            "info"
        )
        await ws.send(welcome_msg.to_json())
        return client_id
    
    async def unregister_client(self, client_id: str):
        """注销客户端"""
        if client_id in self.clients:
            del self.clients[client_id]
            
            if client_id in self.metrics:
                m = self.metrics[client_id]
                self.metrics[client_id].status = ClientStatus.OFFLINE
                logger.info(
                    f"✗ 客户端 {client_id} 已断开 "
                    f"[{m.remote_ip}:{m.remote_port}] "
                    f"[连接时长: {m.connection_duration:.0f}s] "
                    f"[在线: {len(self.clients)}, 健康: {self._count_healthy_clients()}]"
                )
    
    def _count_healthy_clients(self) -> int:
        """统计健康客户端数量"""
        return sum(
            1 for cid, m in self.metrics.items()
            if cid in self.clients and m.status == ClientStatus.HEALTHY
        )
    
    # ============ 消息处理核心 ============
    
    async def handle_message(self, client_id: str, raw_message: str):
        """处理接收到的消息"""
        try:
            msg = Message.from_json(raw_message)
            logger.debug(f"收到来自 {client_id} 的消息: {msg.type}")
            
            handler = self.handlers.get(msg.type)
            if handler:
                await handler(client_id, msg)
            else:
                logger.warning(f"未知消息类型: {msg.type}")
        
        except ValueError as e:
            logger.error(f"消息解析失败: {e}")
            if client_id in self.metrics:
                self.metrics[client_id].record_error()
    
    # ============ 具体消息处理器 ============
    
    async def _handle_heartbeat(self, client_id: str, msg: Message):
        """处理心跳消息"""
        logger.debug(f"收到 {client_id} 的心跳")
        await self.send_to_client(client_id, create_heartbeat())
    
    async def _handle_response(self, client_id: str, msg: Message):
        """处理客户端响应（Worker 的响应）"""
        request_id = msg.data.get("request_id")
        result = msg.data.get("result")
        success = msg.data.get("success", True)
        
        # 检查是否是 Requester 的代理请求
        if request_id in self.requester_pending_requests:
            future = self.requester_pending_requests[request_id]
            if not future.done():
                # 将响应传递给等待的 Requester
                future.set_result({
                    "success": success,
                    "data": result,
                    "error": None if success else str(result)
                })
                logger.debug(f"📨 转发 Worker {client_id} 响应给 Requester: {request_id}")
        
        # 计算响应时间
        if request_id in self.pending_requests:
            start_time = self.pending_requests.pop(request_id)
            response_time = time.time() - start_time
            
            if success:
                self.metrics[client_id].record_success(response_time)
                logger.info(
                    f"✓ {client_id} 响应成功 "
                    f"[{response_time*1000:.1f}ms] "
                    f"结果: {result}"
                )
            else:
                self.metrics[client_id].record_error()
                logger.warning(
                    f"✗ {client_id} 响应失败 "
                    f"错误: {result}"
                )
        else:
            logger.debug(f"收到 {client_id} 的响应，但找不到对应的请求ID")
    
    async def _handle_request(self, client_id: str, msg: Message):
        """处理请求消息"""
        action = msg.data.get("action")
        params = msg.data.get("params", {})
        
        logger.info(f"客户端 {client_id} 请求: {action}")
        
        action_handler = self.request_handlers.get(action)
        if action_handler:
            result = await action_handler(params)
            response = create_response(msg.msg_id, result, success=True)
        else:
            response = create_response(
                msg.msg_id,
                f"不支持的 action: {action}",
                success=False
            )
        
        await self.send_to_client(client_id, response)
    
    # ============ Action 处理器 ============
    
    async def _action_get_time(self, params: dict) -> str:
        """获取服务器时间"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    async def _action_get_client_info(self, params: dict) -> dict:
        """获取在线客户端信息"""
        clients_info = []
        for cid in self.clients:
            m = self.metrics[cid]
            clients_info.append({
                "id": cid,
                "ip": m.remote_ip,
                "port": m.remote_port,
                "status": m.status.value,
                "duration": f"{m.connection_duration:.0f}s"
            })
        
        return {
            "total_clients": len(self.clients),
            "healthy_clients": self._count_healthy_clients(),
            "clients": clients_info
        }
    
    async def _action_calculate(self, params: dict) -> dict:
        """执行计算"""
        try:
            a = params.get("a", 0)
            b = params.get("b", 0)
            op = params.get("op", "+")
            
            if op == "+":
                result = a + b
            elif op == "-":
                result = a - b
            elif op == "*":
                result = a * b
            elif op == "/":
                result = a / b if b != 0 else "除数不能为0"
            else:
                return {"error": f"不支持的运算符: {op}"}
            
            return {"result": f"{a} {op} {b} = {result}"}
        except Exception as e:
            return {"error": str(e)}
    
    async def _action_get_metrics(self, params: dict) -> dict:
        """获取客户端指标"""
        metrics_data = {}
        for client_id, metrics in self.metrics.items():
            if client_id in self.clients:
                metrics_data[client_id] = metrics.get_stats()
        return metrics_data
    
    # ============ 负载均衡的请求发送 ============
    
    async def send_request_auto(
        self, 
        action: str, 
        params: dict = None,
        retries: int = 3
    ) -> Optional[dict]:
        """自动选择客户端发送请求（负载均衡）"""
        for attempt in range(retries):
            # 选择一个客户端
            client_id = self.load_balancer.select_client(self.metrics)
            
            if not client_id:
                logger.error("❌ 没有可用的客户端")
                return None
            
            metrics = self.metrics[client_id]
            logger.info(
                f"[尝试 {attempt + 1}/{retries}] "
                f"选择客户端 {client_id} [{metrics.remote_ip}:{metrics.remote_port}] "
                f"[状态: {metrics.status.value}, "
                f"权重: {metrics.weight:.2f}, "
                f"错误率: {metrics.recent_error_rate*100:.1f}%]"
            )
            
            # 发送请求
            msg = create_request(action, params)
            self.pending_requests[msg.msg_id] = time.time()
            
            try:
                await self.send_to_client(client_id, msg)
                return {"client_id": client_id, "msg_id": msg.msg_id, "status": "sent"}
            
            except Exception as e:
                logger.error(f"发送请求到 {client_id} 失败: {e}")
                self.metrics[client_id].record_error()
                continue
        
        logger.error(f"❌ 发送请求失败，已重试 {retries} 次")
        return None
    
    # ============ 主动发送消息 ============
    
    async def send_to_client(self, client_id: str, msg: Message):
        """向指定客户端发送消息"""
        if client_id not in self.clients:
            logger.warning(f"客户端 {client_id} 不存在或已离线")
            return
        
        try:
            ws = self.clients[client_id]
            await ws.send(msg.to_json())
            logger.debug(f"发送消息到 {client_id}: {msg.type}")
        except Exception as e:
            logger.error(f"发送消息到 {client_id} 失败: {e}")
            self.metrics[client_id].record_error()
    
    async def broadcast(self, msg: Message, exclude: str = None):
        """广播消息给所有客户端"""
        for client_id in list(self.clients.keys()):
            if client_id != exclude:
                await self.send_to_client(client_id, msg)
    
    # ============ 客户端连接处理 ============
    
    async def handle_client(self, ws: WebSocketServerProtocol):
        """处理单个客户端连接"""
        client_id = await self.register_client(ws)
        
        try:
            async for raw_message in ws:
                await self.handle_message(client_id, raw_message)
        
        except websockets.exceptions.ConnectionClosedOK:
            logger.info(f"客户端 {client_id} 正常断开")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"客户端 {client_id} 异常断开: {e}")
        except Exception as e:
            logger.error(f"处理客户端 {client_id} 时发生错误: {e}")
        finally:
            await self.unregister_client(client_id)
    
    # ============ Requester 功能（请求代理）============
    
    async def handle_requester(self, ws: WebSocketServerProtocol):
        """
        处理 Requester 连接（请求客户端）
        
        Requester 发送请求 -> Server 转发给 Worker -> 返回响应给 Requester
        """
        requester_id = str(uuid.uuid4())[:8]
        remote_addr = ws.remote_address
        logger.info(f"🔵 Requester 连接: {requester_id} from {remote_addr}")
        
        self.requester_connections[requester_id] = ws
        
        try:
            async for raw_message in ws:
                try:
                    # 解析 Requester 的请求
                    message = json.loads(raw_message)
                    request_id = message.get("request_id", str(uuid.uuid4()))
                    command = message.get("command")
                    data = message.get("data", {})
                    
                    logger.info(f"📨 Requester {requester_id} 请求: {command} [ID: {request_id}]")
                    
                    # 转发给 Worker（使用负载均衡）
                    response = await self.proxy_to_worker(
                        command=command,
                        data=data,
                        request_id=request_id
                    )
                    
                    # 返回响应给 Requester
                    await ws.send(json.dumps({
                        "request_id": request_id,
                        "success": response.get("success", False),
                        "data": response.get("data"),
                        "error": response.get("error"),
                        "processed_by": response.get("worker_id")
                    }))
                    
                    logger.info(f"✅ Requester {requester_id} 请求完成: {request_id}")
                    
                except json.JSONDecodeError:
                    error_response = {
                        "success": False,
                        "error": "Invalid JSON format"
                    }
                    await ws.send(json.dumps(error_response))
                    logger.warning(f"⚠️  Requester {requester_id} 发送了无效的 JSON")
                    
                except Exception as e:
                    error_response = {
                        "success": False,
                        "error": str(e)
                    }
                    await ws.send(json.dumps(error_response))
                    logger.error(f"❌ 处理 Requester {requester_id} 请求失败: {e}")
        
        except websockets.exceptions.ConnectionClosedOK:
            logger.info(f"🔵 Requester {requester_id} 正常断开")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"🔵 Requester {requester_id} 异常断开: {e}")
        except Exception as e:
            logger.error(f"❌ Requester {requester_id} 错误: {e}")
        finally:
            if requester_id in self.requester_connections:
                del self.requester_connections[requester_id]
            logger.info(f"🔵 Requester {requester_id} 已清理")
    
    async def proxy_to_worker(
        self, 
        command: str, 
        data: dict,
        request_id: str,
        timeout: float = 30.0
    ) -> dict:
        """
        将 Requester 的请求代理转发给 Worker
        
        Args:
            command: 命令名称
            data: 请求数据
            request_id: 请求ID
            timeout: 超时时间
            
        Returns:
            dict: 响应数据
        """
        # 选择一个 Worker
        worker_id = self.load_balancer.select_client(self.metrics)
        
        if not worker_id:
            return {
                "success": False,
                "error": "No available workers",
                "data": None
            }
        
        ws = self.clients.get(worker_id)
        if not ws:
            return {
                "success": False,
                "error": f"Worker {worker_id} not found",
                "data": None
            }
        
        try:
            # 构建请求消息（使用现有的消息格式）
            # create_request 使用 action 而不是 command
            request_msg = Message(
                msg_type=MessageType.REQUEST,
                data={"action": command, "params": data},
                msg_id=request_id  # 使用 Requester 的 request_id 作为消息 ID
            )
            
            # 记录请求
            self.pending_requests[request_id] = time.time()
            
            # 发送给 Worker
            await ws.send(request_msg.to_json())
            logger.debug(f"📤 转发请求 {request_id} 到 Worker {worker_id}: {command}")
            
            # 等待响应（通过 handle_response 处理）
            # 创建一个 Future 来等待响应
            future = asyncio.Future()
            self.requester_pending_requests[request_id] = future
            
            try:
                response = await asyncio.wait_for(future, timeout=timeout)
                return {
                    "success": response.get("success", True),
                    "data": response.get("data"),
                    "error": response.get("error"),
                    "worker_id": worker_id
                }
            except asyncio.TimeoutError:
                logger.warning(f"⏱️  Worker {worker_id} 响应超时: {request_id}")
                return {
                    "success": False,
                    "error": "Worker response timeout",
                    "data": None,
                    "worker_id": worker_id
                }
            finally:
                # 清理
                if request_id in self.requester_pending_requests:
                    del self.requester_pending_requests[request_id]
                if request_id in self.pending_requests:
                    del self.pending_requests[request_id]
                    
        except Exception as e:
            logger.error(f"❌ 转发请求失败 {request_id}: {e}")
            return {
                "success": False,
                "error": f"Proxy error: {str(e)}",
                "data": None,
                "worker_id": worker_id
            }
    
    # ============ 控制台 ============
    
    async def console(self):
        """服务器控制台"""
        logger.info("控制台已启动，输入 'help' 查看命令")
        
        while True:
            try:
                cmd = await asyncio.to_thread(input, "\n[命令] > ")
                
                if not cmd.strip():
                    continue
                
                parts = cmd.split(maxsplit=1)
                command = parts[0].lower()
                
                if command == "help":
                    print("\n可用命令:")
                    print("  list - 列出在线客户端及连接信息")
                    print("  metrics - 显示客户端详细指标")
                    print("  send <client_id> <action> [params] - 发送到指定客户端")
                    print("  auto <action> [params] - 自动选择客户端发送（推荐）⭐")
                    print("  broadcast <message> - 广播消息")
                    print("  reset <client_id> - 重置客户端指标")
                    print("  cleanup - 手动执行清理")
                    print("  quit - 退出服务器")
                
                elif command == "list":
                    print(f"\n在线客户端 ({len(self.clients)}):")
                    for cid in self.clients:
                        m = self.metrics[cid]
                        print(
                            f"  - {cid} "
                            f"[{m.remote_ip}:{m.remote_port} -> :{m.local_port}] "
                            f"[状态: {m.status.value}, "
                            f"权重: {m.weight:.2f}, "
                            f"错误率: {m.recent_error_rate*100:.1f}%, "
                            f"请求数: {m.total_requests}, "
                            f"在线: {m.connection_duration:.0f}s]"
                        )
                
                elif command == "metrics":
                    print("\n客户端详细指标:")
                    for cid in self.clients:
                        m = self.metrics[cid]
                        stats = m.get_stats()
                        print(f"\n  客户端 {cid}:")
                        for key, value in stats.items():
                            print(f"    {key}: {value}")
                
                elif command == "send" and len(parts) > 1:
                    args = parts[1].split(maxsplit=2)
                    if len(args) >= 2:
                        client_id = args[0]
                        action = args[1]
                        params = eval(args[2]) if len(args) > 2 else {}
                        
                        msg = create_request(action, params)
                        self.pending_requests[msg.msg_id] = time.time()
                        await self.send_to_client(client_id, msg)
                        logger.info(f"已发送请求到 {client_id}: {action}")
                
                elif command == "auto" and len(parts) > 1:
                    args = parts[1].split(maxsplit=1)
                    action = args[0]
                    params = eval(args[1]) if len(args) > 1 else {}
                    
                    result = await self.send_request_auto(action, params)
                    if result:
                        print(f"✓ 请求已发送: {result}")
                    else:
                        print("✗ 请求发送失败")
                
                elif command == "broadcast" and len(parts) > 1:
                    content = parts[1]
                    msg = create_notification("服务器广播", content)
                    await self.broadcast(msg)
                    logger.info(f"已广播消息: {content}")
                
                elif command == "reset" and len(parts) > 1:
                    client_id = parts[1].strip()
                    if client_id in self.metrics:
                        self.metrics[client_id].reset_metrics()
                        print(f"✓ 已重置客户端 {client_id} 的指标")
                    else:
                        print(f"✗ 客户端 {client_id} 不存在")
                
                elif command == "cleanup":
                    print("执行手动清理...")
                    for client_id, metrics in self.metrics.items():
                        if client_id in self.clients:
                            metrics.cleanup_old_records()
                    print("✓ 清理完成")
                
                elif command == "quit":
                    logger.info("服务器关闭中...")
                    break
                
                else:
                    print("未知命令，输入 'help' 查看帮助")
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"控制台错误: {e}")
    
    # ============ 启动服务器 ============
    
    async def start(self):
        """启动服务器（双端口：Worker + Requester）"""
        logger.info(
            f"服务器 v3 启动中 "
            f"[负载均衡: {self.load_balancer.strategy}, "
            f"清理间隔: {self.cleanup_interval}s]"
        )
        
        # 启动清理任务
        cleanup_task = asyncio.create_task(self.cleanup_loop())
        
        try:
            # 同时启动两个 WebSocket 服务器
            async with serve(self.handle_client, self.host, self.port) as worker_server,                        serve(self.handle_requester, self.host, self.requester_port) as requester_server:
                logger.info(f"✓ Worker 服务器已启动，监听 {self.host}:{self.port}")
                logger.info(f"✓ Requester API 已启动，监听 {self.host}:{self.requester_port}")
                await self.console()
        finally:
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass


# ============ 入口 ============

async def main():
    server = WebSocketServer(
        host="0.0.0.0", 
        port=8765,              # Worker 端口
        requester_port=8766,    # Requester API 端口
        lb_strategy="least_loaded",
        cleanup_interval=60     # 每60秒清理一次过期数据
    )
    await server.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n服务器已关闭")
