"""
Requester Client - 向服务器发送请求并接收响应

这个客户端连接到服务器的 Requester API 端口（默认 8766），
发送请求后，服务器会将请求转发给 Worker 客户端处理，
然后将响应返回给这个 Requester。
"""
import asyncio
import websockets
import json
import uuid
import sys


class RequesterClient:
    def __init__(self, server_url: str = "ws://localhost:8766"):
        self.server_url = server_url
        self.ws = None
    
    async def connect(self):
        """连接到服务器的 Requester API"""
        print(f"🔌 连接到 Requester API: {self.server_url}")
        self.ws = await websockets.connect(self.server_url)
        print("✅ 已连接")
    
    async def send_request(self, command: str, data: dict = None) -> dict:
        """
        发送请求并等待响应
        
        Args:
            command: 命令名称（如 'get_time', 'echo' 等）
            data: 请求数据
            
        Returns:
            dict: 响应数据
        """
        if not self.ws:
            raise Exception("未连接到服务器")
        
        request_id = str(uuid.uuid4())
        request = {
            "request_id": request_id,
            "command": command,
            "data": data or {}
        }
        
        print(f"📤 发送请求 [{request_id[:8]}]: {command}")
        await self.ws.send(json.dumps(request))
        
        # 等待响应
        response_raw = await self.ws.recv()
        response = json.loads(response_raw)
        
        if response.get("success"):
            print(f"✅ 请求成功 [{request_id[:8]}]")
            print(f"   处理者: {response.get('processed_by')}")
            print(f"   结果: {response.get('data')}")
        else:
            print(f"❌ 请求失败 [{request_id[:8]}]")
            print(f"   错误: {response.get('error')}")
        
        return response
    
    async def close(self):
        """关闭连接"""
        if self.ws:
            await self.ws.close()
            print("🔌 连接已关闭")


async def main():
    # 从命令行参数获取服务器地址
    if len(sys.argv) >= 3:
        host = sys.argv[1]
        port = sys.argv[2]
        server_url = f"ws://{host}:{port}"
    else:
        server_url = "ws://localhost:8766"
    
    client = RequesterClient(server_url)
    
    try:
        await client.connect()
        
        # 示例：发送多个请求
        print("\n" + "="*50)
        print("示例 1: 获取时间")
        print("="*50)
        await client.send_request("get_time")
        
        await asyncio.sleep(1)
        
        print("\n" + "="*50)
        print("示例 2: Echo 测试")
        print("="*50)
        await client.send_request("echo", {"message": "Hello from Requester!"})
        
        await asyncio.sleep(1)
        
        print("\n" + "="*50)
        print("示例 3: 计算测试")
        print("="*50)
        await client.send_request("calculate", {"a": 10, "b": 20, "op": "add"})
        
        await asyncio.sleep(1)
        
        # 交互式模式
        print("\n" + "="*50)
        print("进入交互模式（输入 'quit' 退出）")
        print("="*50)
        print("可用命令：")
        print("  get_time - 获取当前时间")
        print("  echo <message> - 回显消息")
        print("  quit - 退出")
        print()
        
        while True:
            try:
                cmd = await asyncio.to_thread(input, "命令 > ")
                cmd = cmd.strip()
                
                if not cmd:
                    continue
                
                if cmd.lower() == 'quit':
                    break
                
                parts = cmd.split(maxsplit=1)
                command = parts[0]
                
                if command == "echo" and len(parts) > 1:
                    data = {"message": parts[1]}
                elif command == "get_time":
                    data = {}
                else:
                    data = {}
                
                await client.send_request(command, data)
                print()
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"错误: {e}")
    
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 再见！")
