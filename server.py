import asyncio
import websockets
import uuid
from websockets import WebSocketServerProtocol

# 存储在线客户端：{client_id: websocket实例}
connected_clients = {}

# 注册客户端（连接建立时）
async def register_client(ws: WebSocketServerProtocol) -> str:
    client_id = str(uuid.uuid4())[:8]  # 生成短ID便于识别
    connected_clients[client_id] = ws
    print(f"[服务器] 客户端 {client_id} 上线，当前在线：{len(connected_clients)} 个")
    # 向客户端发送其ID（用于客户端标识自己）
    await ws.send(f"[系统通知] 你的客户端ID：{client_id}，已连接服务器")
    return client_id

# 注销客户端（连接断开时）
async def unregister_client(client_id: str):
    if client_id in connected_clients:
        del connected_clients[client_id]
        print(f"[服务器] 客户端 {client_id} 下线，当前在线：{len(connected_clients)} 个")

# 向指定客户端发送请求
async def send_request_to_client(client_id: str, request_content: str):
    if client_id not in connected_clients:
        print(f"[服务器] 客户端 {client_id} 不存在或已下线")
        return
    ws = connected_clients[client_id]
    # 发送“请求”（格式：请求标识+内容，便于客户端解析）
    await ws.send(f"[服务器请求] {request_content}")
    print(f"[服务器] 已向 {client_id} 发送请求：{request_content}")

# 处理单个客户端的交互逻辑
async def handle_client(ws: WebSocketServerProtocol):
    client_id = await register_client(ws)
    try:
        # 持续监听客户端的响应（或客户端主动发送的消息）
        async for message in ws:
            # 解析客户端消息（区分“响应”和“主动消息”）
            if message.startswith("[客户端响应]"):
                request_id = message.split("|")[1] if "|" in message else "未知"
                response_content = message.split("|")[2] if "|" in message else message
                print(f"[服务器] 收到 {client_id} 的响应（请求ID：{request_id}）：{response_content}")
            else:
                print(f"[服务器] 收到 {client_id} 的主动消息：{message}")

    except websockets.exceptions.ConnectionClosedOK:
        print(f"[服务器] 客户端 {client_id} 正常断开连接")
    except websockets.exceptions.ConnectionClosedError:
        print(f"[服务器] 客户端 {client_id} 异常断开连接（如网络中断）")
    finally:
        await unregister_client(client_id)

# 服务器控制台：手动输入指令，向指定客户端发送请求
async def server_console():
    while True:
        # 等待用户输入（格式：客户端ID|请求内容）
        cmd = await asyncio.to_thread(input, "\n请输入指令（格式：客户端ID|请求内容，如 123456|获取当前时间）：")
        if "|" not in cmd:
            print("格式错误！请用“|”分隔客户端ID和请求内容")
            continue
        client_id, request_content = cmd.split("|", 1)
        # 异步向客户端发送请求（不阻塞控制台）
        asyncio.create_task(send_request_to_client(client_id.strip(), request_content.strip()))

# 启动服务器
async def main():
    # 启动 WebSocket 服务器（监听 8765 端口）
    async with websockets.serve(handle_client, "0.0.0.0", 8765):
        print("服务器已启动，监听端口 8765（按 Ctrl+C 关闭）")
        # 启动服务器控制台（用于手动发送请求）
        await server_console()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n服务器已手动关闭")
