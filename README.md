# 改进版 WebSocket 示例

基于 `websockets` 库的可扩展 WebSocket 服务器和客户端实现。

## 主要特性

### 1. **JSON 消息协议** (`message_protocol.py`)
- 类型化的消息格式
- 易于序列化/反序列化
- 支持消息ID和时间戳
- 内置多种消息类型

### 2. **消息处理器模式**
- Handler 映射机制
- 易于扩展新的消息类型
- 清晰的代码结构

### 3. **Action 处理器**
- 请求-响应模式
- 可插拔的 action 处理器
- 支持自定义业务逻辑

### 4. **完善的功能**
- 自动重连
- 心跳机制
- 日志系统
- 错误处理
- 控制台交互

## 文件说明

```
message_protocol.py   # 消息协议定义
server.py            # WebSocket 服务器
client.py            # WebSocket 客户端
example_custom_handler.py  # 扩展示例
```

## 快速开始

### 1. 安装依赖

```bash
pip install websockets
```

### 2. 启动服务器

```bash
python server.py
```

服务器启动后，可以使用控制台命令：
- `help` - 查看帮助
- `list` - 列出在线客户端
- `send <client_id> <action> [params]` - 发送请求
- `broadcast <message>` - 广播消息
- `quit` - 退出

### 3. 启动客户端

```bash
python client.py
```

## 消息类型

### 系统消息
- `CONNECT` - 连接建立
- `DISCONNECT` - 断开连接
- `HEARTBEAT` - 心跳
- `ERROR` - 错误消息

### 业务消息
- `REQUEST` - 请求消息
- `RESPONSE` - 响应消息
- `NOTIFICATION` - 通知消息

### 自定义消息
- `CUSTOM` - 自定义消息（可扩展）

## 消息格式

```json
{
  "type": "request",
  "msg_id": "msg_1234567890",
  "timestamp": 1234567890.123,
  "data": {
    "action": "get_time",
    "params": {}
  }
}
```

## 扩展方法

### 1. 添加新的 Action 处理器

**服务器端：**

```python
class MyServer(WebSocketServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 注册新的 action
        self.request_handlers["my_action"] = self._handle_my_action
    
    async def _handle_my_action(self, params: dict) -> dict:
        # 你的处理逻辑
        return {"result": "success"}
```

**客户端端：**

```python
class MyClient(WebSocketClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 注册新的 action
        self.request_handlers["my_action"] = self._handle_my_action
    
    async def _handle_my_action(self, params: dict) -> dict:
        # 你的处理逻辑
        return {"result": "success"}
```

### 2. 添加新的消息类型处理器

```python
# 在 message_protocol.py 中添加新的消息类型
class MessageType(str, Enum):
    # ... 现有类型 ...
    MY_TYPE = "my_type"  # 新增

# 在服务器/客户端类中注册处理器
self.handlers[MessageType.MY_TYPE] = self._handle_my_type

async def _handle_my_type(self, msg: Message):
    # 处理逻辑
    pass
```

## 内置 Action

### 服务器端

- `get_time` - 获取服务器时间
- `get_client_info` - 获取在线客户端信息
- `calculate` - 执行计算
  ```python
  params = {"a": 10, "b": 20, "op": "+"}
  ```

### 客户端

- `get_time` - 获取客户端时间
- `get_system_info` - 获取系统信息
- `execute_command` - 执行命令（示例，未实际执行）

## 使用示例

### 服务器向客户端发送请求

在服务器控制台：
```
send abc123 get_time
send abc123 calculate {"a": 10, "b": 20, "op": "+"}
```

### 客户端向服务器发送请求

启动客户端时启用控制台：
```python
# 修改 client.py 的 main()
await client.start(enable_console=True)
```

在客户端控制台：
```
request get_time
request calculate {"a": 10, "b": 20, "op": "+"}
```

## 对比旧版本

### 旧版本问题
- 字符串解析消息（易出错）
- if-else 处理消息（难扩展）
- print 输出（缺少日志级别）
- 缺少类型提示

### 新版本优势
- JSON 格式（类型安全）
- Handler 模式（易扩展）
- logging 系统（专业日志）
- 完整类型提示

## 推荐其他库

如果需要更高级的功能，可以考虑：

1. **Socket.IO** (`python-socketio`)
   - 更完善的事件系统
   - 自动重连
   - 房间和命名空间

2. **FastAPI + WebSockets**
   - 与 HTTP API 集成
   - 自动文档
   - 依赖注入

3. **Django Channels**
   - 与 Django 集成
   - 支持多协议

但对于大多数场景，`websockets` + 好的架构已经足够！

## 许可

MIT License

---

## Worker & Requester 架构（v3.1 新增）

### 架构概述

服务器现在支持两种类型的客户端：

#### 1. **Worker Client（工作客户端）**
- **端口**: 8765（默认）
- **作用**: 连接到服务器，等待接收任务并执行
- **通信**: 双向 WebSocket，接收服务器请求并返回响应
- **负载均衡**: 服务器自动分配任务到不同的 Worker
- **文件**: `client.py`

#### 2. **Requester Client（请求客户端）**
- **端口**: 8766（默认）
- **作用**: 向服务器发送请求，服务器转发给 Worker 处理
- **通信**: 双向 WebSocket，发送请求并接收响应
- **API 接口**: JSON 格式的简单 API
- **文件**: `requester_client.py`

### 工作流程

```
Requester (Port 8766)  →  Server  →  Worker (Port 8765)
    ↓                                      ↓
1. 发送 JSON 请求              2. 转发到最优 Worker
    ↓                                      ↓
4. 接收响应              ←     3. Worker 处理并响应
```

### 快速开始

#### 1. 启动服务器
```bash
python server.py
```
服务器会在两个端口上监听：
- **8765**: Worker 连接端口
- **8766**: Requester API 端口

#### 2. 启动 Worker 客户端
```bash
# 启动多个 Worker（不同终端）
python client.py localhost 8765  # Worker 1
python client.py localhost 8765  # Worker 2
python client.py localhost 8765  # Worker 3
```

#### 3. 使用 Requester 发送请求
```bash
python requester_client.py localhost 8766
```

### Requester API 格式

#### 请求格式
```json
{
  "request_id": "uuid-string",
  "command": "command_name",
  "data": {
    "key": "value"
  }
}
```

#### 响应格式
```json
{
  "request_id": "uuid-string",
  "success": true,
  "data": "result_data",
  "error": null,
  "processed_by": "worker_id"
}
```

### 支持的命令

#### get_time - 获取当前时间
```json
// 请求
{"command": "get_time", "data": {}}

// 响应
{"success": true, "data": "2025-10-20 15:30:45", "processed_by": "abc123"}
```

#### echo - 回显消息
```json
// 请求
{"command": "echo", "data": {"message": "Hello World"}}

// 响应
{"success": true, "data": "Echo: Hello World", "processed_by": "abc123"}
```

### Python 使用示例

```python
import asyncio
import websockets
import json
import uuid

async def send_request():
    uri = "ws://localhost:8766"
    async with websockets.connect(uri) as ws:
        # 构建请求
        request = {
            "request_id": str(uuid.uuid4()),
            "command": "get_time",
            "data": {}
        }
        
        # 发送请求
        await ws.send(json.dumps(request))
        
        # 接收响应
        response = await ws.recv()
        result = json.loads(response)
        
        print(f"Success: {result['success']}")
        print(f"Data: {result['data']}")
        print(f"Processed by: {result['processed_by']}")

asyncio.run(send_request())
```

### 负载均衡

服务器会自动将 Requester 的请求分配到不同的 Worker：

- **策略**: `least_loaded`（最少负载）
- **考虑因素**: Worker 权重、响应时间、请求数量、健康状态

### 错误处理

#### Worker 不可用
```json
{"success": false, "error": "No available workers", "data": null}
```

#### 超时（默认 30 秒）
```json
{"success": false, "error": "Worker response timeout", "data": null}
```

### 配置

在 `server.py` 的 `main()` 函数中修改：

```python
server = WebSocketServer(
    host="0.0.0.0",
    port=8765,              # Worker 端口
    requester_port=8766,    # Requester API 端口
    lb_strategy="least_loaded",
    cleanup_interval=60
)
```

### 可用负载均衡策略

- `least_loaded` - 最少负载（推荐）
- `round_robin` - 轮询
- `random` - 随机
- `weighted_random` - 加权随机

### 优势

1. **解耦**: Requester 和 Worker 完全独立
2. **负载均衡**: 自动分配请求到最优 Worker
3. **容错**: Worker 故障时自动切换
4. **简单**: Requester 使用简单的 JSON API
5. **扩展**: 可以轻松添加更多 Worker
