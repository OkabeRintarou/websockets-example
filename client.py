import asyncio
import websockets
from datetime import datetime
import time

# 生成请求ID（用于匹配请求和响应，避免混淆）
def generate_request_id():
    return f"req_{int(time.time() * 1000)}"  # 时间戳作为唯一ID

# 处理服务器的请求（核心：客户端的“服务端角色”逻辑）
def handle_server_request(request_content: str) -> str:
    """根据服务器请求内容，返回对应响应"""
    if "获取当前时间" in request_content:
        return f"当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    elif "获取客户端信息" in request_content:
        return f"客户端信息：Python 3.9+，WebSocket 客户端，持续在线"
    elif "执行计算" in request_content:
        # 示例：解析请求中的数字并计算（如“执行计算 10+20”）
        try:
            nums = request_content.split(" ")[-1]
            a, b = nums.split("+")
            return f"计算结果：{a} + {b} = {int(a) + int(b)}"
        except Exception as e:
            return f"计算失败：{str(e)}"
    else:
        return f"未知请求，内容：{request_content}"

# 客户端心跳机制（定期发送空消息，维持连接）
async def send_heartbeat(ws, interval=30):
    """每 interval 秒发送一次心跳"""
    while True:
        await asyncio.sleep(interval)
        try:
            await ws.send("[心跳]")  # 发送心跳标识
            # print(f"[客户端] 发送心跳（{datetime.now().strftime('%H:%M:%S')}）")
        except Exception as e:
            print(f"[客户端] 心跳发送失败：{str(e)}")
            break

# 客户端主逻辑（持续连接+监听请求）
async def client_main(server_url: str = "ws://localhost:8765"):
    print(f"[客户端] 尝试连接服务器：{server_url}")
    try:
        # 建立 WebSocket 连接（持续保持）
        async with websockets.connect(server_url, ping_interval=60, ping_timeout=120) as ws:
            print("[客户端] 已成功连接服务器，等待接收请求...")
            
            # 启动心跳任务（后台运行，不阻塞主逻辑）
            heartbeat_task = asyncio.create_task(send_heartbeat(ws))
            
            # 持续监听服务器发送的消息（核心循环：不退出）
            async for message in ws:
                # 忽略心跳消息，只处理业务请求
                if message == "[心跳]":
                    continue
                
                # 解析服务器消息：区分“系统通知”和“业务请求”
                if message.startswith("[系统通知]"):
                    print(f"[客户端] {message}")
                elif message.startswith("[服务器请求]"):
                    # 提取请求内容，处理并回复
                    request_content = message.replace("[服务器请求]", "").strip()
                    print(f"\n[客户端] 收到服务器请求：{request_content}")
                    
                    # 处理请求（调用本地逻辑）
                    response_content = handle_server_request(request_content)
                    request_id = generate_request_id()
                    
                    # 向服务器发送响应（格式：[客户端响应]|请求ID|响应内容）
                    await ws.send(f"[客户端响应]|{request_id}|{response_content}")
                    print(f"[客户端] 发送响应（请求ID：{request_id}）：{response_content}")
                else:
                    print(f"[客户端] 收到未知消息：{message}")
            
            # 心跳任务收尾（若主循环退出，取消心跳）
            heartbeat_task.cancel()
            await heartbeat_task
        
    except websockets.exceptions.ConnectionRefusedError:
        print("[客户端] 连接失败：服务器未启动或地址错误")
    except websockets.exceptions.ConnectionClosedOK:
        print("[客户端] 正常断开与服务器的连接")
    except websockets.exceptions.ConnectionClosedError:
        print("[客户端] 与服务器连接异常断开（如网络中断），10秒后重试...")
        # 连接断开后重试（可选逻辑）
        await asyncio.sleep(10)
        await client_main(server_url)  # 递归重试
    except KeyboardInterrupt:
        print("[客户端] 手动关闭连接")
    except Exception as e:
        print(f"[客户端] 发生未知错误：{str(e)}")

if __name__ == "__main__":
    # 启动客户端（连接本地服务器，可替换为公网服务器地址）
    asyncio.run(client_main(server_url="ws://localhost:8765"))
