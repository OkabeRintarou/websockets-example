"""
改进版 WebSocket 客户端
- 使用 JSON 消息协议
- 消息处理器模式（易扩展）
- 支持自动重连
"""
import asyncio
import logging
from typing import Dict, Callable
from datetime import datetime
import websockets

from message_protocol import (
    Message, MessageType, create_request, 
    create_response, create_heartbeat
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class WebSocketClient:
    """WebSocket 客户端类"""
    
    def __init__(self, server_url: str = "ws://localhost:8765"):
        self.server_url = server_url
        self.ws = None
        self.client_id = None
        self.running = False
        
        # 消息处理器映射（核心扩展点）
        self.handlers: Dict[MessageType, Callable] = {
            MessageType.NOTIFICATION: self._handle_notification,
            MessageType.REQUEST: self._handle_request,
            MessageType.HEARTBEAT: self._handle_heartbeat,
            MessageType.ERROR: self._handle_error,
        }
        
        # 请求处理器映射（处理服务器发来的请求）
        self.request_handlers: Dict[str, Callable] = {
            "get_time": self._action_get_time,
            "get_system_info": self._action_get_system_info,
            "execute_command": self._action_execute_command,
            # 可轻松扩展新的 action
        }
    
    # ============ 消息处理核心 ============
    
    async def handle_message(self, raw_message: str):
        """处理接收到的消息（根据消息类型分发）"""
        try:
            msg = Message.from_json(raw_message)
            logger.debug(f"收到消息: {msg.type}")
            
            # 根据消息类型分发到对应处理器
            handler = self.handlers.get(msg.type)
            if handler:
                await handler(msg)
            else:
                logger.warning(f"未知消息类型: {msg.type}")
        
        except ValueError as e:
            logger.error(f"消息解析失败: {e}")
    
    # ============ 具体消息处理器 ============
    
    async def _handle_notification(self, msg: Message):
        """处理通知消息"""
        title = msg.data.get("title", "通知")
        content = msg.data.get("content", "")
        level = msg.data.get("level", "info")
        
        logger.info(f"[{level.upper()}] {title}: {content}")
    
    async def _handle_heartbeat(self, msg: Message):
        """处理心跳消息"""
        logger.debug("收到服务器心跳")
    
    async def _handle_error(self, msg: Message):
        """处理错误消息"""
        error = msg.data.get("error", "未知错误")
        code = msg.data.get("code", "")
        logger.error(f"服务器错误 [{code}]: {error}")
    
    async def _handle_request(self, msg: Message):
        """处理服务器请求"""
        action = msg.data.get("action")
        params = msg.data.get("params", {})
        
        logger.info(f"收到服务器请求: {action}")
        
        # 查找对应的 action 处理器
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
        
        await self.send(response)
        logger.info(f"已响应请求: {action}")
    
    # ============ Action 处理器（可扩展） ============
    
    async def _action_get_time(self, params: dict) -> str:
        """获取客户端时间"""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    async def _action_get_system_info(self, params: dict) -> dict:
        """获取系统信息"""
        import platform
        import sys
        
        return {
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": sys.version,
            "machine": platform.machine(),
        }
    
    async def _action_execute_command(self, params: dict) -> dict:
        """执行命令（示例：可根据需要实现）"""
        command = params.get("command", "")
        logger.info(f"收到命令执行请求: {command}")
        
        # 注意：实际应用中需要权限控制和安全检查
        # 这里仅作示例
        return {
            "status": "received",
            "command": command,
            "note": "命令已接收，但未实际执行（安全考虑）"
        }
    
    # ============ 发送消息 ============
    
    async def send(self, msg: Message):
        """发送消息到服务器"""
        if self.ws:
            try:
                await self.ws.send(msg.to_json())
                logger.debug(f"发送消息: {msg.type}")
            except Exception as e:
                logger.error(f"发送消息失败: {e}")
    
    async def send_request(self, action: str, params: dict = None):
        """发送请求到服务器"""
        msg = create_request(action, params)
        await self.send(msg)
        logger.info(f"发送请求: {action}")
    
    # ============ 心跳机制 ============
    
    async def heartbeat_loop(self, interval: int = 30):
        """心跳循环"""
        while self.running:
            await asyncio.sleep(interval)
            if self.ws:
                await self.send(create_heartbeat())
                logger.debug("发送心跳")
    
    # ============ 连接管理 ============
    
    async def connect(self):
        """连接到服务器"""
        logger.info(f"连接到服务器: {self.server_url}")
        
        try:
            self.ws = await websockets.connect(
                self.server_url,
                ping_interval=60,
                ping_timeout=120
            )
            self.running = True
            logger.info("✓ 已连接到服务器")
            
            # 启动心跳任务
            heartbeat_task = asyncio.create_task(self.heartbeat_loop())
            
            try:
                # 持续接收消息
                async for raw_message in self.ws:
                    await self.handle_message(raw_message)
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
        
        except websockets.exceptions.ConnectionRefusedError:
            logger.error("连接失败: 服务器未启动或地址错误")
        except websockets.exceptions.ConnectionClosedOK:
            logger.info("连接正常关闭")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"连接异常关闭: {e}")
        except Exception as e:
            logger.error(f"连接错误: {e}")
        finally:
            self.running = False
            self.ws = None
    
    async def connect_with_retry(self, max_retries: int = -1, retry_interval: int = 10):
        """带重连的连接"""
        retry_count = 0
        
        while max_retries < 0 or retry_count < max_retries:
            try:
                await self.connect()
            except KeyboardInterrupt:
                logger.info("用户中断连接")
                break
            
            if max_retries >= 0:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"达到最大重连次数 {max_retries}")
                    break
            
            logger.info(f"将在 {retry_interval} 秒后重连...")
            await asyncio.sleep(retry_interval)
    
    # ============ 客户端控制台（可选） ============
    
    async def console(self):
        """客户端控制台，用于手动发送请求"""
        logger.info("客户端控制台已启动，输入 'help' 查看命令")
        
        while self.running:
            try:
                cmd = await asyncio.to_thread(
                    input,
                    "\n[客户端] > "
                )
                
                if not cmd.strip():
                    continue
                
                parts = cmd.split(maxsplit=1)
                command = parts[0].lower()
                
                if command == "help":
                    print("\n可用命令:")
                    print("  request <action> [params] - 发送请求到服务器")
                    print("  quit - 断开连接")
                    print("\n示例:")
                    print("  request get_time")
                    print("  request calculate {'a': 10, 'b': 20, 'op': '+'}")
                
                elif command == "request" and len(parts) > 1:
                    args = parts[1].split(maxsplit=1)
                    action = args[0]
                    params = eval(args[1]) if len(args) > 1 else {}
                    await self.send_request(action, params)
                
                elif command == "quit":
                    logger.info("断开连接...")
                    self.running = False
                    if self.ws:
                        await self.ws.close()
                    break
                
                else:
                    print("未知命令，输入 'help' 查看帮助")
            
            except Exception as e:
                logger.error(f"控制台错误: {e}")
    
    # ============ 启动客户端 ============
    
    async def start(self, enable_console: bool = False):
        """启动客户端"""
        if enable_console:
            # 同时启动连接和控制台
            await asyncio.gather(
                self.connect_with_retry(),
                self.console()
            )
        else:
            # 仅启动连接（持续运行）
            await self.connect_with_retry()


# ============ 入口 ============

async def main():
    import sys
    import os
    
    # 优先级: 命令行参数 > 环境变量 > 默认值
    if len(sys.argv) >= 3:
        # 从命令行参数读取: python client.py <host> <port>
        host = sys.argv[1]
        port = sys.argv[2]
        server_url = f"ws://{host}:{port}"
        logger.info(f"使用命令行参数: {server_url}")
    elif "WEBSOCKET_SERVER_HOST" in os.environ:
        # 从环境变量读取
        host = os.environ.get("WEBSOCKET_SERVER_HOST", "localhost")
        port = os.environ.get("WEBSOCKET_SERVER_PORT", "8765")
        server_url = f"ws://{host}:{port}"
        logger.info(f"使用环境变量: {server_url}")
    else:
        # 使用默认值
        server_url = "ws://localhost:8765"
        logger.info(f"使用默认值: {server_url}")
    
    client = WebSocketClient(server_url=server_url)
    
    # 方式1: 仅保持连接，处理服务器请求
    await client.start(enable_console=False)
    
    # 方式2: 启用控制台，可手动发送请求
    # await client.start(enable_console=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n客户端已关闭")
