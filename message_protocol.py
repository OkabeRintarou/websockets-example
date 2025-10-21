"""
WebSocket Message Protocol
Supports extensible message types
"""
from typing import Dict, Any, Optional
from enum import Enum
import json
from datetime import datetime


class MessageType(str, Enum):
    """Message Type Enumeration"""
    # System messages
    CONNECT = "connect"           # Connection established
    DISCONNECT = "disconnect"     # Connection closed
    HEARTBEAT = "heartbeat"       # Heartbeat
    ERROR = "error"               # Error message
    
    # Business messages
    REQUEST = "request"           # Server request
    RESPONSE = "response"         # Client response
    NOTIFICATION = "notification" # Notification message
    
    # Custom messages (extensible)
    CUSTOM = "custom"             # Custom message


class Message:
    """Standard Message Format"""
    
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
        """Generate unique message ID"""
        return f"msg_{int(datetime.now().timestamp() * 1000)}"
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps({
            "type": self.type.value,
            "data": self.data,
            "msg_id": self.msg_id,
            "timestamp": self.timestamp
        }, ensure_ascii=False, separators=(',', ':'))
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Parse from JSON string"""
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


# ============ Message Creation Functions ============

def create_request(action: str, params: Dict[str, Any] = None) -> Message:
    """Create request message"""
    return Message(
        msg_type=MessageType.REQUEST,
        data={"action": action, "params": params or {}}
    )


def create_response(request_id: str, result: Any, success: bool = True) -> Message:
    """Create response message"""
    return Message(
        msg_type=MessageType.RESPONSE,
        data={
            "request_id": request_id,
            "result": result,
            "success": success
        }
    )


def create_notification(title: str, content: str, level: str = "info") -> Message:
    """Create notification message"""
    return Message(
        msg_type=MessageType.NOTIFICATION,
        data={
            "title": title,
            "content": content,
            "level": level  # info, warning, error
        }
    )


def create_heartbeat() -> Message:
    """Create heartbeat message"""
    return Message(
        msg_type=MessageType.HEARTBEAT,
        data={}
    )


def create_error(error_msg: str, error_code: str = None) -> Message:
    """Create error message"""
    return Message(
        msg_type=MessageType.ERROR,
        data={
            "error": error_msg,
            "code": error_code
        }
    )