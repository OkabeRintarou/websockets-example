"""
Requester Client - Send requests to server and receive responses

This client connects to the server's Requester API port (default 8766),
sends requests, and the server forwards the requests to Worker clients for processing,
then returns the response to this Requester.
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
        """Connect to the server's Requester API"""
        print(f"🔌 Connecting to Requester API: {self.server_url}")
        self.ws = await websockets.connect(self.server_url)
        print("✅ Connected")
    
    async def send_request(self, command: str, data: dict = None) -> dict:
        """
        Send request and wait for response
        
        Args:
            command: Command name (e.g. 'get_time', 'echo', etc.)
            data: Request data
            
        Returns:
            dict: Response data
        """
        if not self.ws:
            raise Exception("Not connected to server")
        
        request_id = str(uuid.uuid4())
        request = {
            "request_id": request_id,
            "command": command,
            "data": data or {}
        }
        
        print(f"📤 Sending request [{request_id[:8]}]: {command}")
        await self.ws.send(json.dumps(request))
        
        # Wait for response
        response_raw = await self.ws.recv()
        response = json.loads(response_raw)
        
        if response.get("success"):
            print(f"✅ Request succeeded [{request_id[:8]}]")
            print(f"   Processed by: {response.get('processed_by')}")
            print(f"   Result: {response.get('data')}")
        else:
            print(f"❌ Request failed [{request_id[:8]}]")
            print(f"   Error: {response.get('error')}")
        
        return response
    
    async def close(self):
        """Close connection"""
        if self.ws:
            await self.ws.close()
            print("🔌 Connection closed")


async def main():
    # Get server address from command line arguments
    if len(sys.argv) >= 3:
        host = sys.argv[1]
        port = sys.argv[2]
        server_url = f"ws://{host}:{port}"
    else:
        server_url = "ws://localhost:8766"
    
    client = RequesterClient(server_url)
    
    try:
        await client.connect()
        
        # Example: Send multiple requests
        print("\n" + "="*50)
        print("Example 1: Get time")
        print("="*50)
        await client.send_request("get_time")
        
        await asyncio.sleep(1)
        
        print("\n" + "="*50)
        print("Example 3: Market data test")
        print("="*50)
        # Example market data request
        market_data = {
            "symbol_type": "stock",
            "code": "000001", 
            "period": "daily",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "adjust": "qfq"
        }
        await client.send_request("get_market", market_data)
        
        await asyncio.sleep(1)
    
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")