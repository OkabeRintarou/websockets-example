"""
WebSocket 消息协议定义
支持自定义消息类型的扩展
"""
from typing import Dict, Any, Optional
from enum import Enum
import json
from datetime import datetime


class MessageType(str, Enum):
    """消息类型枚举"""
    # 系统消息
    CONNECT = "connect"           # 连接建立
    DISCONNECT = "disconnect"     # 断开连接
    HEARTBEAT = "heartbeat"       # 心跳
    ERROR = "error"               # 错误消息
    
    # 业务消息
    REQUEST = "request"           # 服务器请求
    RESPONSE = "response"         # 客户端响应
    NOTIFICATION = "notification" # 通知消息
    
    # 自定义消息（可扩展）
    CUSTOM = "custom"             # 自定义消息


class Message:
    """标准消息格式"""
    
    def __init__(
        self,
        msg_type: MessageType,
        data: Dict[str, Any],
        msg_id: Optional[str] = None,
        timestamp: Optional[float] = None
    ):
        self.type = msg_type
        self.data = data
        self.msg_id = msg_id or self._generate_id()
        self.timestamp = timestamp or datetime.now().timestamp()
    
    @staticmethod
    def _generate_id() -> str:
        """生成唯一消息ID"""
        return f"msg_{int(datetime.now().timestamp() * 1000)}"
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps({
            "type": self.type.value,
            "data": self.data,
            "msg_id": self.msg_id,
            "timestamp": self.timestamp
        }, ensure_ascii=False)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """从JSON字符串解析"""
        try:
            obj = json.loads(json_str)
            return cls(
                msg_type=MessageType(obj["type"]),
                data=obj["data"],
                msg_id=obj.get("msg_id"),
                timestamp=obj.get("timestamp")
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise ValueError(f"Invalid message format: {e}")
    
    def __repr__(self):
        return f"Message(type={self.type}, msg_id={self.msg_id}, data={self.data})"


# ============ 便捷的消息构造函数 ============

def create_request(action: str, params: Dict[str, Any] = None) -> Message:
    """创建请求消息"""
    return Message(
        msg_type=MessageType.REQUEST,
        data={"action": action, "params": params or {}}
    )


def create_response(request_id: str, result: Any, success: bool = True) -> Message:
    """创建响应消息"""
    return Message(
        msg_type=MessageType.RESPONSE,
        data={
            "request_id": request_id,
            "result": result,
            "success": success
        }
    )


def create_notification(title: str, content: str, level: str = "info") -> Message:
    """创建通知消息"""
    return Message(
        msg_type=MessageType.NOTIFICATION,
        data={
            "title": title,
            "content": content,
            "level": level  # info, warning, error
        }
    )


def create_heartbeat() -> Message:
    """创建心跳消息"""
    return Message(
        msg_type=MessageType.HEARTBEAT,
        data={}
    )


def create_error(error_msg: str, error_code: str = None) -> Message:
    """创建错误消息"""
    return Message(
        msg_type=MessageType.ERROR,
        data={
            "error": error_msg,
            "code": error_code
        }
    )
