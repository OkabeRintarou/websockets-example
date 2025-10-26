"""
WebSocket Server
- Automatic load balancing
- Error detection and health checking
- Error attenuation mechanism
- Time window statistics
- Client IP/Port information storage
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class ClientStatus(str, Enum):
    """Client Status"""
    HEALTHY = "healthy"      # Healthy
    DEGRADED = "degraded"    # Degraded (has errors but available)
    UNHEALTHY = "unhealthy"  # Unhealthy (too many errors)
    OFFLINE = "offline"      # Offline


@dataclass
class RequestRecord:
    """Request Record"""
    timestamp: float
    success: bool
    response_time: float = 0.0


@dataclass
class ClientMetrics:
    """Client Metrics"""
    client_id: str
    
    # Client connection information
    remote_ip: str = ""
    remote_port: int = 0
    local_port: int = 0
    connect_time: float = 0.0
    
    # Use deque to store recent request records (time window)
    recent_requests: deque = field(default_factory=lambda: deque(maxlen=100))
    
    last_error_time: float = 0
    last_success_time: float = 0
    status: ClientStatus = ClientStatus.HEALTHY
    weight: float = 1.0
    
    # Configuration parameters
    time_window: int = 300  # Time window: 5 minutes
    error_decay_rate: float = 0.1  # Error decay rate: 10% per minute
    
    @property
    def connection_duration(self) -> float:
        """Connection duration (seconds)"""
        if self.connect_time > 0:
            return time.time() - self.connect_time
        return 0.0
    
    @property
    def recent_error_rate(self) -> float:
        """Calculate error rate within time window"""
        if not self.recent_requests:
            return 0.0
        
        current_time = time.time()
        cutoff_time = current_time - self.time_window
        
        # Only count requests within time window
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
        """Total requests"""
        return len(self.recent_requests)
    
    @property
    def success_count(self) -> int:
        """Success count"""
        return sum(1 for req in self.recent_requests if req.success)
    
    @property
    def error_count(self) -> int:
        """Error count"""
        return sum(1 for req in self.recent_requests if not req.success)
    
    @property
    def avg_response_time(self) -> float:
        """Average response time"""
        recent_10 = list(self.recent_requests)[-10:]
        if not recent_10:
            return 0.0
        
        response_times = [req.response_time for req in recent_10 if req.success]
        if not response_times:
            return 0.0
        
        return sum(response_times) / len(response_times)
    
    def update_status(self):
        """
        Update client status (based on error rate within time window)
        
        Improvements:
        1. Only look at recent error rate, not affected by history
        2. Automatically clean up expired data
        3. Dynamically adjust thresholds
        """
        error_rate = self.recent_error_rate
        
        # If there has been a recent successful request and error rate is not high, improve health
        current_time = time.time()
        time_since_last_success = current_time - self.last_success_time if self.last_success_time > 0 else float('inf')
        
        # If there was a successful request within the last 1 minute
        if time_since_last_success < 60:
            # Reduce the impact of error rate
            if error_rate >= 0.5:
                self.status = ClientStatus.DEGRADED  # Relaxed: originally UNHEALTHY
                self.weight = 0.6
            elif error_rate >= 0.3:
                self.status = ClientStatus.HEALTHY  # Relaxed: originally DEGRADED
                self.weight = 0.8
            else:
                self.status = ClientStatus.HEALTHY
                self.weight = 1.0
        else:
            # Standard evaluation
            if error_rate >= 0.6:  # Increased threshold: from 50% to 60%
                self.status = ClientStatus.UNHEALTHY
                self.weight = 0.2  # Increased weight: from 0.1 to 0.2
            elif error_rate >= 0.3:  # Increased threshold: from 20% to 30%
                self.status = ClientStatus.DEGRADED
                self.weight = 0.6  # Increased weight: from 0.5 to 0.6
            else:
                self.status = ClientStatus.HEALTHY
                self.weight = 1.0
        
        logger.debug(
            f"Client {self.client_id} status updated: "
            f"Error rate={error_rate*100:.1f}%, Status={self.status.value}, Weight={self.weight}"
        )
    
    def record_success(self, response_time: float):
        """Record successful request"""
        self.recent_requests.append(RequestRecord(
            timestamp=time.time(),
            success=True,
            response_time=response_time
        ))
        self.last_success_time = time.time()
        self.update_status()
    
    def record_error(self):
        """Record failed request"""
        self.recent_requests.append(RequestRecord(
            timestamp=time.time(),
            success=False,
            response_time=0.0
        ))
        self.last_error_time = time.time()
        self.update_status()
    
    def cleanup_old_records(self):
        """Clean up expired request records (outside time window)"""
        current_time = time.time()
        cutoff_time = current_time - self.time_window
        
        # Find the first unexpired record
        while self.recent_requests and self.recent_requests[0].timestamp < cutoff_time:
            self.recent_requests.popleft()
        
        # Re-evaluate status
        self.update_status()
    
    def reset_metrics(self):
        """Reset metrics"""
        self.recent_requests.clear()
        self.last_error_time = 0
        self.last_success_time = 0
        self.update_status()
    
    def get_stats(self) -> dict:
        """Get statistics"""
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
    """Load Balancer"""
    
    def __init__(self, strategy: str = "least_loaded"):
        self.strategy = strategy
        self.round_robin_index = 0
    
    def select_client(self, metrics: Dict[str, ClientMetrics]) -> Optional[str]:
        """
        Select a client
        
        Improvements:
        1. Prioritize HEALTHY clients
        2. If no HEALTHY clients, then select DEGRADED
        3. Only select UNHEALTHY in extreme cases
        """
        # Group by health status
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
        
        # Priority selection
        if healthy_clients:
            selected_pool = healthy_clients
            logger.debug(f"Selecting from {len(healthy_clients)} healthy clients")
        elif degraded_clients:
            selected_pool = degraded_clients
            logger.info(f"⚠️  No healthy clients, selecting from {len(degraded_clients)} degraded clients")
        elif unhealthy_clients:
            selected_pool = unhealthy_clients
            logger.warning(f"⚠️  No healthy/degraded clients, selecting from {len(unhealthy_clients)} unhealthy clients")
        else:
            logger.error("❌ No available clients")
            return None
        
        # Select based on strategy
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
        """Round-robin strategy"""
        client_id = client_ids[self.round_robin_index % len(client_ids)]
        self.round_robin_index += 1
        return client_id
    
    def _least_loaded(self, metrics: Dict[str, ClientMetrics]) -> str:
        """Least loaded strategy (considering weight, response time, and request count)"""
        # Consider weight, response time, and request count comprehensively
        def score(m: ClientMetrics) -> float:
            # Request count penalty: prevent a single client from being overused
            request_count = len(m.recent_requests)
            request_penalty = request_count * 0.1
            # Higher weight is better, lower response time is better
            time_factor = 1.0 / (1.0 + m.avg_response_time) if m.avg_response_time > 0 else 1.0
            # Composite score: weight * time factor / (1 + request penalty)
            return m.weight * time_factor / (1.0 + request_penalty)
        
        return max(metrics.items(), key=lambda x: score(x[1]))[0]

    def _weighted_random(self, metrics: Dict[str, ClientMetrics]) -> str:
        """Weighted random strategy"""
        import random
        
        clients = list(metrics.items())
        weights = [m.weight for _, m in clients]
        
        # Ensure all weights are greater than 0
        if sum(weights) == 0:
            weights = [1.0] * len(clients)
        
        selected = random.choices(clients, weights=weights, k=1)[0]
        return selected[0]


class WebSocketServer:
    """WebSocket Server"""
    
    def __init__(
        self, 
        host: str = "0.0.0.0", 
        port: int = 8765,
        requester_port: int = 8766,  # Requester port
        lb_strategy: str = "least_loaded",
        cleanup_interval: int = 60,  # Cleanup interval (seconds)
        handshake_key: str = "deadbeafbeafdead"  # Handshake key
    ):
        self.host = host
        self.port = port
        self.requester_port = requester_port
        self.clients: Dict[str, WebSocketServerProtocol] = {}
        self.requester_connections: Dict[str, WebSocketServerProtocol] = {}  # Requester client connections
        self.requester_pending_requests: Dict[str, asyncio.Future] = {}  # Requester pending requests
        self.metrics: Dict[str, ClientMetrics] = {}
        self.load_balancer = LoadBalancer(strategy=lb_strategy)
        self.cleanup_interval = cleanup_interval
        self.handshake_key = handshake_key  # Store the handshake key
        
        # Request tracking
        self.pending_requests: Dict[str, float] = {}
        
        # Message handler mapping
        self.handlers: Dict[MessageType, Callable] = {
            MessageType.REQUEST: self._handle_request,
            MessageType.HEARTBEAT: self._handle_heartbeat,
            MessageType.RESPONSE: self._handle_response,
        }
        
        # Request handler mapping
        self.request_handlers: Dict[str, Callable] = {
            "get_time": self._action_get_time,
            "get_client_info": self._action_get_client_info,
            "calculate": self._action_calculate,
            "get_metrics": self._action_get_metrics,
        }
    
    # ============ Periodic Cleanup Tasks ============
    
    async def cleanup_loop(self):
        """Periodically clean up expired data"""
        while True:
            await asyncio.sleep(self.cleanup_interval)
            
            logger.debug("Performing periodic cleanup...")
            for client_id, metrics in self.metrics.items():
                if client_id in self.clients:  # Only clean up online clients
                    metrics.cleanup_old_records()
            
            logger.debug(f"Cleanup completed, currently online: {len(self.clients)}, healthy: {self._count_healthy_clients()}")
    
    # ============ Client Connection Management ============
    
    async def register_client(self, client_id: str, ws: WebSocketServerProtocol):
        """Register new client"""
        self.clients[client_id] = ws
        
        # Get client connection information
        remote_address = ws.remote_address  # (ip, port)
        local_address = ws.local_address    # (ip, port)
        remote_ip = remote_address[0] if remote_address else "unknown"
        remote_port = remote_address[1] if remote_address else 0
        local_port = local_address[1] if local_address else 0
        
        # Create client metrics with connection information
        self.metrics[client_id] = ClientMetrics(
            client_id=client_id,
            remote_ip=remote_ip,
            remote_port=remote_port,
            local_port=local_port,
            connect_time=time.time()
        )
        
        logger.info(
            f"✓ Client {client_id} connected [{remote_ip}:{remote_port} -> :{local_port}] [Online: {len(self.clients)}, Healthy: {self._count_healthy_clients()}]"
        )
        
        welcome_msg = create_notification(
            "Connection Successful",
            f"Your client ID: {client_id}\nFrom: {remote_ip}:{remote_port}",
            "info"
        )
        await ws.send(welcome_msg.to_json())
    
    async def unregister_client(self, client_id: str):
        """Unregister client"""
        if client_id in self.clients:
            del self.clients[client_id]
            
            if client_id in self.metrics:
                m = self.metrics[client_id]
                self.metrics[client_id].status = ClientStatus.OFFLINE
                logger.info(
                    f"✗ Client {client_id} disconnected "
                    f"[{m.remote_ip}:{m.remote_port}] "
                    f"[Connection duration: {m.connection_duration:.0f}s] "
                    f"[Online: {len(self.clients)}, Healthy: {self._count_healthy_clients()}]"
                )
    
    def _count_healthy_clients(self) -> int:
        """Count healthy clients"""
        return sum(
            1 for cid, m in self.metrics.items()
            if cid in self.clients and m.status == ClientStatus.HEALTHY
        )
    
    # ============ Message Processing Core ============
    
    async def handle_message(self, client_id: str, raw_message: str):
        """Handle received message"""
        # Print the size of the received message
        logger.info(f"Received message size from {client_id}: {len(raw_message)} bytes")
        
        try:
            msg = Message.from_json(raw_message)
            logger.debug(f"Received message from {client_id}: {msg.type}")
            
            handler = self.handlers.get(msg.type)
            if handler:
                await handler(client_id, msg)
            else:
                logger.warning(f"Unknown message type: {msg.type}")
        
        except ValueError as e:
            logger.error(f"Message parsing failed: {e}")
            if client_id in self.metrics:
                self.metrics[client_id].record_error()
    
    # ============ Specific Message Handlers ============
    
    async def _handle_heartbeat(self, client_id: str, msg: Message):
        """Handle heartbeat message"""
        logger.debug(f"Received heartbeat from {client_id}")
        await self.send_to_client(client_id, create_heartbeat())
    
    async def _handle_response(self, client_id: str, msg: Message):
        """Handle client response (Worker response)"""
        request_id = msg.data.get("request_id")
        result = msg.data.get("result")
        success = msg.data.get("success", True)
        
        # Check if this is a Requester proxy request
        if request_id in self.requester_pending_requests:
            future = self.requester_pending_requests[request_id]
            if not future.done():
                # Pass response to waiting Requester
                future.set_result({
                    "success": success,
                    "data": result,
                    "error": None if success else str(result)
                })
                logger.debug(f"Forwarding Worker {client_id} response to Requester: {request_id}")
        
        # Calculate response time
        if request_id in self.pending_requests:
            start_time = self.pending_requests.pop(request_id)
            response_time = time.time() - start_time
            
            if success:
                self.metrics[client_id].record_success(response_time)
                logger.info(
                    f"✓ {client_id} responded successfully "
                    f"[{response_time*1000:.1f}ms]"
                )
                # Print detailed timing information for specific actions like get_market
                if isinstance(result, dict) and result.get('action') == 'get_market':
                    logger.info(f"📈 Market data request latency: {response_time*1000:.1f}ms")
            else:
                self.metrics[client_id].record_error()
                logger.warning(
                    f"✗ {client_id} response failed "
                    f"Error: {result}"
                )
        else:
            logger.debug(f"Received response from {client_id} but cannot find corresponding request ID")
    
    async def _handle_request(self, client_id: str, msg: Message):
        """Handle request message"""
        action = msg.data.get("action")
        params = msg.data.get("params", {})
        
        logger.info(f"Client {client_id} requested: {action}")
        
        action_handler = self.request_handlers.get(action)
        if action_handler:
            result = await action_handler(params)
            response = create_response(msg.msg_id, result, success=True)
        else:
            response = create_response(
                msg.msg_id,
                f"Unsupported action: {action}",
                success=False
            )
        
        await self.send_to_client(client_id, response)
    
    # ============ Action Handlers ============
    
    async def _action_get_time(self, params: dict) -> str:
        """Get server time"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    async def _action_get_client_info(self, params: dict) -> dict:
        """Get online client information"""
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
        """Perform calculation"""
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
                result = a / b if b != 0 else "Divisor cannot be zero"
            else:
                return {"error": f"Unsupported operator: {op}"}
            
            return {"result": f"{a} {op} {b} = {result}"}
        except Exception as e:
            return {"error": str(e)}
    
    async def _action_get_metrics(self, params: dict) -> dict:
        """Get client metrics"""
        metrics_data = {}
        for client_id, metrics in self.metrics.items():
            if client_id in self.clients:
                metrics_data[client_id] = metrics.get_stats()
        return metrics_data
    
    # ============ Load-balanced Request Sending ============
    
    async def send_request_auto(
        self, 
        action: str, 
        params: dict = None,
        retries: int = 3
    ) -> Optional[dict]:
        """Automatically select client to send request (load balancing)"""
        for attempt in range(retries):
            # Select a client
            client_id = self.load_balancer.select_client(self.metrics)
            
            if not client_id:
                logger.error("❌ No available clients")
                return None
            
            metrics = self.metrics[client_id]
            logger.info(
                f"[Attempt {attempt + 1}/{retries}] "
                f"Selecting client {client_id} [{metrics.remote_ip}:{metrics.remote_port}] "
                f"[Status: {metrics.status.value}, "
                f"Weight: {metrics.weight:.2f}, "
                f"Error rate: {metrics.recent_error_rate*100:.1f}%]"
            )
            
            # Send request
            msg = create_request(action, params)
            self.pending_requests[msg.msg_id] = time.time()
            
            try:
                await self.send_to_client(client_id, msg)
                return {"client_id": client_id, "msg_id": msg.msg_id, "status": "sent"}
            
            except Exception as e:
                logger.error(f"Failed to send request to {client_id}: {e}")
                self.metrics[client_id].record_error()
                continue
        
        logger.error(f"❌ Failed to send request, retried {retries} times")
        return None
    
    # ============ Active Message Sending ============
    
    async def send_to_client(self, client_id: str, msg: Message):
        """Send message to specified client"""
        if client_id not in self.clients:
            logger.warning(f"Client {client_id} does not exist or is offline")
            return
        
        try:
            ws = self.clients[client_id]
            await ws.send(msg.to_json())
            logger.debug(f"Sent message to {client_id}: {msg.type}")
        except Exception as e:
            logger.error(f"Failed to send message to {client_id}: {e}")
            self.metrics[client_id].record_error()
    
    async def broadcast(self, msg: Message, exclude: str = None):
        """Broadcast message to all clients"""
        for client_id in list(self.clients.keys()):
            if client_id != exclude:
                await self.send_to_client(client_id, msg)
    
    # ============ Client Connection Handling ============
    
    async def handle_client(self, ws: WebSocketServerProtocol):
        """Handle individual client connection"""
        # Generate client ID but don't register yet
        client_id = str(uuid.uuid4())[:8]
        
        # Get client connection information early
        remote_address = ws.remote_address  # (ip, port)
        remote_ip = remote_address[0] if remote_address else "unknown"
        remote_port = remote_address[1] if remote_address else 0
        client_address = f"{remote_ip}:{remote_port}"
        
        try:
            # Wait for handshake message before registering client (with 5s timeout)
            try:
                raw_message = await asyncio.wait_for(ws.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"Client {client_id} [{client_address}] handshake timeout (5s)")
                await ws.close()
                return
            
            try:
                msg = Message.from_json(raw_message)
                # Check if it's a handshake message with the correct key
                if msg.type == MessageType.HANDSHAKE:
                    handshake_key = msg.data.get("handshake_key", "")
                    if handshake_key == self.handshake_key:
                        logger.info(f"Client {client_id} [{client_address}] handshake successful")
                        
                        # Now register the client
                        await self.register_client(client_id, ws)
                        
                        # Send handshake confirmation
                        confirmation_msg = create_notification(
                            "Handshake Successful",
                            "Connection established and client registered",
                            "info"
                        )
                        await ws.send(confirmation_msg.to_json())
                        
                        # Continue processing messages
                        async for raw_message in ws:
                            await self.handle_message(client_id, raw_message)
                    else:
                        logger.warning(f"Client {client_id} [{client_address}] sent invalid handshake key")
                        error_msg = create_error(
                            "Invalid handshake",
                            "HANDSHAKE_ERROR"
                        )
                        await ws.send(error_msg.to_json())
                        await ws.close()
                else:
                    logger.warning(f"Client {client_id} [{client_address}] sent non-handshake message as first message")
                    error_msg = create_error(
                        "Invalid handshake",
                        "HANDSHAKE_ERROR"
                    )
                    await ws.send(error_msg.to_json())
                    await ws.close()
                    
            except ValueError as e:
                logger.error(f"Invalid message format from client {client_id} [{client_address}]: {e}")
                error_msg = create_error(
                    "Invalid message format",
                    "INVALID_FORMAT"
                )
                await ws.send(error_msg.to_json())
                await ws.close()
        
        except websockets.exceptions.ConnectionClosedOK:
            logger.info(f"Client {client_id} [{client_address}] disconnected normally")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"Client {client_id} [{client_address}] disconnected abnormally: {e}")
        except Exception as e:
            logger.error(f"Error handling client {client_id} [{client_address}]: {e}")
        finally:
            # Only unregister if client was registered
            if client_id in self.clients:
                await self.unregister_client(client_id)
    
    # ============ Requester Functionality (Request Proxy) ============
    
    async def handle_requester(self, ws: WebSocketServerProtocol):
        """
        Handle Requester connection (request client)
        
        Requester sends request -> Server forwards to Worker -> Return response to Requester
        """
        requester_id = str(uuid.uuid4())[:8]
        remote_addr = ws.remote_address
        logger.info(f"🔵 Requester connected: {requester_id} from {remote_addr}")
        
        self.requester_connections[requester_id] = ws
        
        try:
            async for raw_message in ws:
                try:
                    # Parse Requester's request
                    message = json.loads(raw_message)
                    request_id = message.get("request_id", str(uuid.uuid4()))
                    command = message.get("command")
                    data = message.get("data", {})
                    
                    logger.info(f"📨 Requester {requester_id} request: {command} [ID: {request_id}]")
                    
                    # Special handling for get_markets: distribute across workers
                    if command == "get_markets":
                        # Handle both direct list and nested dict formats
                        if isinstance(data, dict) and "markets" in data:
                            markets = data.get("markets", [])
                            logger.info("🔄 Using nested 'markets' format")
                        elif isinstance(data, list):
                            markets = data
                            logger.info("✅ Using direct list format")
                        else:
                            markets = []
                            logger.warning("⚠️  Invalid markets format")
                        
                        if isinstance(markets, list) and len(markets) > 1:
                            # Use distributed processing for multiple markets
                            response = await self.proxy_get_markets_distributed(
                                markets=markets,
                                request_id=request_id
                            )
                        else:
                            # Single market or invalid, use normal proxy
                            response = await self.proxy_to_worker(
                                command=command,
                                data=markets,  # Use direct list instead of wrapping in dict
                                request_id=request_id
                            )
                    else:
                        # Normal proxy for other commands
                        response = await self.proxy_to_worker(
                            command=command,
                            data=data,
                            request_id=request_id
                        )
                    
                    # Return response to Requester
                    await ws.send(json.dumps({
                        "request_id": request_id,
                        "success": response.get("success", False),
                        "data": response.get("data"),
                        "error": response.get("error"),
                        "processed_by": response.get("worker_id")
                    }))
                    
                    logger.info(f"✅ Requester {requester_id} request completed: {request_id}")
                    
                except json.JSONDecodeError:
                    error_response = {
                        "success": False,
                        "error": "Invalid JSON format"
                    }
                    await ws.send(json.dumps(error_response))
                    logger.warning(f"⚠️  Requester {requester_id} sent invalid JSON")
                    
                except Exception as e:
                    error_response = {
                        "success": False,
                        "error": str(e)
                    }
                    await ws.send(json.dumps(error_response))
                    logger.error(f"❌ Failed to handle Requester {requester_id} request: {e}")
        
        except websockets.exceptions.ConnectionClosedOK:
            logger.info(f"🔵 Requester {requester_id} disconnected normally")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"🔵 Requester {requester_id} disconnected abnormally: {e}")
        except Exception as e:
            logger.error(f"❌ Requester {requester_id} error: {e}")
        finally:
            if requester_id in self.requester_connections:
                del self.requester_connections[requester_id]
            logger.info(f"🔵 Requester {requester_id} cleaned up")
    
    async def proxy_get_markets_distributed(
        self,
        markets: list,
        request_id: str,
        timeout: float = 10.0
    ) -> dict:
        """
        Distribute get_markets request across multiple workers

        Strategy:
        1. Get available workers
        2. Distribute markets evenly across workers
        3. Group same code to same worker (for potential caching)
           - Same code with different periods go to the same worker
           - e.g., 588000 daily and 588000 5-min both go to worker1
        4. Send requests in parallel
        5. Merge results

        Args:
            markets: List of market params [{"code": "588000", "period": "daily", ...}, ...]
            request_id: Original request ID
            timeout: Timeout for each worker request
            
        Returns:
            dict: Merged response with all results
        """
        # Get available workers
        available_workers = [
            cid for cid, metric in self.metrics.items()
            if metric.status == ClientStatus.HEALTHY and cid in self.clients
        ]
        
        if not available_workers:
            # Fallback to single worker
            logger.warning("No healthy workers available, using fallback")
            return await self.proxy_to_worker("get_markets", markets, request_id, timeout)
        
        num_workers = len(available_workers)
        logger.info(f"📊 Distributing {len(markets)} markets across {num_workers} workers")
        
        # Group markets by symbol for better caching
        # If same symbol appears multiple times, send to same worker
        symbol_to_worker = {}
        worker_assignments = {worker_id: [] for worker_id in available_workers}
        
        # Distribute markets
        for i, market in enumerate(markets):
            # Use code as the grouping key (same code goes to same worker regardless of period)
            code = market.get("code", f"unknown_{i}")
            
            # If we've seen this code before, use the same worker
            if code in symbol_to_worker:
                worker_id = symbol_to_worker[code]
            else:
                # Round-robin assignment for new codes
                worker_id = available_workers[len(symbol_to_worker) % num_workers]
                symbol_to_worker[code] = worker_id
            
            worker_assignments[worker_id].append(market)
        
        # Log distribution
        for worker_id, assigned_markets in worker_assignments.items():
            if assigned_markets:
                codes = [m.get("code", "?") for m in assigned_markets]
                logger.info(f"  Worker {worker_id}: {len(assigned_markets)} markets, codes: {codes}")
        
        # Send requests to workers in parallel
        tasks = []
        worker_ids = []
        
        for worker_id, assigned_markets in worker_assignments.items():
            if not assigned_markets:
                continue
            
            # Create unique sub-request ID
            sub_request_id = f"{request_id}_sub_{worker_id}"
            
            logger.info(f"📤 Sending {len(assigned_markets)} markets to worker {worker_id}")
            
            # Create task for this specific worker (not using load balancer)
            task = self._send_to_specific_worker(
                worker_id=worker_id,
                command="get_markets",
                data=assigned_markets,  # Direct list instead of {"markets": assigned_markets}
                request_id=sub_request_id,
                timeout=timeout
            )
            tasks.append(task)
            worker_ids.append(worker_id)
        
        # Wait for all workers to respond
        start_time = time.time()
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time
        
        # Merge results
        all_results = []
        total_success = 0
        total_failed = 0
        errors = []
        
        for worker_id, response in zip(worker_ids, responses):
            if isinstance(response, Exception):
                logger.error(f"Worker {worker_id} failed: {response}")
                errors.append(f"Worker {worker_id}: {str(response)}")
                # Count as failed
                assigned_count = len(worker_assignments[worker_id])
                total_failed += assigned_count
            elif not response.get("success"):
                logger.error(f"Worker {worker_id} returned error: {response.get('error')}")
                errors.append(f"Worker {worker_id}: {response.get('error')}")
                assigned_count = len(worker_assignments[worker_id])
                total_failed += assigned_count
            else:
                # Extract results from worker response
                worker_data = response.get("data", {})
                
                if isinstance(worker_data, dict):
                    results = worker_data.get("results", [])
                    summary = worker_data.get("summary", {})
                    
                    all_results.extend(results)
                    total_success += summary.get("success", 0)
                    total_failed += summary.get("failed", 0)
                    
                    logger.debug(f"Worker {worker_id}: {summary.get('success')}/{summary.get('total')} successful")
        
        logger.info(
            f"✅ Distributed query completed: {total_success}/{len(markets)} successful, "
            f"elapsed {elapsed:.2f}s, "
            f"used {len(worker_ids)} workers"
        )
        
        return {
            "success": True,
            "data": {
                "success": True,
                "results": all_results,
                "summary": {
                    "total": len(markets),
                    "success": total_success,
                    "failed": total_failed
                },
                "distributed": {
                    "workers_used": len(worker_ids),
                    "elapsed_time": elapsed,
                    "errors": errors if errors else None
                }
            },
            "worker_id": f"distributed_{len(worker_ids)}_workers"
        }
    
    async def _send_to_specific_worker(
        self,
        worker_id: str,
        command: str,
        data: dict,
        request_id: str,
        timeout: float = 10.0
    ) -> dict:
        """
        Send request to a specific worker (bypass load balancer)
        
        Args:
            worker_id: Target worker ID
            command: Command name
            data: Request data
            request_id: Request ID
            timeout: Timeout duration
            
        Returns:
            dict: Response data
        """
        start_time = time.time()
        
        # Get the specific worker connection
        ws = self.clients.get(worker_id)
        if not ws:
            logger.error(f"❌ Worker {worker_id} not found")
            return {
                "success": False,
                "error": f"Worker {worker_id} not found",
                "data": None
            }
        
        try:
            # Build request message
            request_msg = Message(
                msg_type=MessageType.REQUEST,
                data={"action": command, "params": data},
                msg_id=request_id
            )
            
            # Record request
            future = asyncio.Future()
            self.requester_pending_requests[request_id] = future
            
            # Send request
            await ws.send(request_msg.to_json())
            logger.debug(f"📤 Sent to worker {worker_id}: {command} [ID: {request_id}]")
            
            # Wait for response
            try:
                response_data = await asyncio.wait_for(future, timeout=timeout)
                elapsed = time.time() - start_time
                logger.debug(f"✅ Worker {worker_id} responded in {elapsed:.3f}s")
                return response_data
            except asyncio.TimeoutError:
                logger.error(f"⏱️ Timeout waiting for worker {worker_id}")
                return {
                    "success": False,
                    "error": f"Timeout waiting for worker {worker_id}",
                    "data": None
                }
        except Exception as e:
            logger.error(f"❌ Error sending to worker {worker_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": None
            }
        finally:
            # Cleanup
            if request_id in self.requester_pending_requests:
                del self.requester_pending_requests[request_id]
    
    async def proxy_to_worker(
        self, 
        command: str, 
        data: dict,
        request_id: str,
        timeout: float = 10.0
    ) -> dict:
        """
        Proxy Requester's request to Worker
        
        Args:
            command: Command name
            data: Request data
            request_id: Request ID
            timeout: Timeout duration
            
        Returns:
            dict: Response data
        """
        # Record start time for latency measurement
        start_time = time.time()

        # Select a Worker
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
            # Build request message (using existing message format)
            # create_request uses action instead of command
            request_msg = Message(
                msg_type=MessageType.REQUEST,
                data={"action": command, "params": data},
                msg_id=request_id  # Use Requester's request_id as message ID
            )
            
            # Record request
            self.pending_requests[request_id] = start_time
            
            # Send to Worker
            await ws.send(request_msg.to_json())
            logger.debug(f"📤 Forward request {request_id} to Worker {worker_id}: {command}")

            # Wait for response (handled by handle_response)
            # Create a Future to wait for response
            future = asyncio.Future()
            self.requester_pending_requests[request_id] = future
            
            try:
                response = await asyncio.wait_for(future, timeout=timeout)
                # Calculate and log the latency
                latency = (time.time() - start_time) * 1000  # Convert to milliseconds
                logger.info(f"⏱️  Request {request_id} completed in {latency:.1f}ms (processed by {worker_id})")

                return {
                    "success": response.get("success", True),
                    "data": response.get("data"),
                    "error": response.get("error"),
                    "worker_id": worker_id
                }
            except asyncio.TimeoutError:
                logger.warning(f"⏱️  Worker {worker_id} response timeout: {request_id}")
                return {
                    "success": False,
                    "error": "Worker response timeout",
                    "data": None,
                    "worker_id": worker_id
                }
            finally:
                # Cleanup
                if request_id in self.requester_pending_requests:
                    del self.requester_pending_requests[request_id]
                if request_id in self.pending_requests:
                    del self.pending_requests[request_id]
                    
        except Exception as e:
            logger.error(f"❌ Forward request failed {request_id}: {e}")
            return {
                "success": False,
                "error": f"Proxy error: {str(e)}",
                "data": None,
                "worker_id": worker_id
            }
    
    # ============ Console ============
    
    async def console(self):
        """Server console"""
        logger.info("Console started, type 'help' for commands")
        
        while True:
            try:
                cmd = await asyncio.to_thread(input, "\n[Command] > ")
                
                if not cmd.strip():
                    continue
                
                parts = cmd.split(maxsplit=1)
                command = parts[0].lower()
                
                if command == "help":
                    print("\nAvailable commands:")
                    print("  list - List online clients and connection information")
                    print("  metrics - Display detailed client metrics")
                    print("  send <client_id> <action> [params] - Send to specified client")
                    print("  auto <action> [params] - Automatically select client to send (recommended) ⭐")
                    print("  broadcast <message> - Broadcast message")
                    print("  reset <client_id> - Reset client metrics")
                    print("  cleanup - Manually perform cleanup")
                    print("  quit - Exit server")
                
                elif command == "list":
                    print(f"\nOnline clients ({len(self.clients)}):")
                    for cid in self.clients:
                        m = self.metrics[cid]
                        print(
                            f"  - {cid} "
                            f"[{m.remote_ip}:{m.remote_port} -> :{m.local_port}] "
                            f"[Status: {m.status.value}, "
                            f"Weight: {m.weight:.2f}, "
                            f"Error rate: {m.recent_error_rate*100:.1f}%, "
                            f"Requests: {m.total_requests}, "
                            f"Online: {m.connection_duration:.0f}s]"
                        )
                
                elif command == "metrics":
                    print("\nDetailed client metrics:")
                    for cid in self.clients:
                        m = self.metrics[cid]
                        stats = m.get_stats()
                        print(f"\n  Client {cid}:")
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
                        logger.info(f"Sent request to {client_id}: {action}")
                
                elif command == "auto" and len(parts) > 1:
                    args = parts[1].split(maxsplit=1)
                    action = args[0]
                    params = eval(args[1]) if len(args) > 1 else {}
                    
                    result = await self.send_request_auto(action, params)
                    if result:
                        print(f"✓ Request sent: {result}")
                    else:
                        print("✗ Failed to send request")
                
                elif command == "broadcast" and len(parts) > 1:
                    content = parts[1]
                    msg = create_notification("Server Broadcast", content)
                    await self.broadcast(msg)
                    logger.info(f"Broadcast message: {content}")
                
                elif command == "reset" and len(parts) > 1:
                    client_id = parts[1].strip()
                    if client_id in self.metrics:
                        self.metrics[client_id].reset_metrics()
                        print(f"✓ Reset metrics for client {client_id}")
                    else:
                        print(f"✗ Client {client_id} does not exist")
                
                elif command == "cleanup":
                    print("Performing manual cleanup...")
                    for client_id, metrics in self.metrics.items():
                        if client_id in self.clients:
                            metrics.cleanup_old_records()
                    print("✓ Cleanup completed")
                
                elif command == "quit":
                    logger.info("Shutting down server...")
                    break
                
                else:
                    print("Unknown command, type 'help' for help")
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Console error: {e}")
    
    # ============ Start Server ============
    
    async def start(self, enable_console=False):
        """Start server (dual port: Worker + Requester)"""
        logger.info(
            f"Starting server "
            f"[Load balancing: {self.load_balancer.strategy}, "
            f"Cleanup interval: {self.cleanup_interval}s]"
        )
        
        # Start cleanup task
        cleanup_task = asyncio.create_task(self.cleanup_loop())
        
        try:
            # Start both WebSocket servers simultaneously with increased max message size
            async with serve(self.handle_client, self.host, self.port, max_size=5 * 1024 * 1024) as worker_server, \
                       serve(self.handle_requester, self.host, self.requester_port, max_size=5 * 1024 * 1024) as requester_server:
                logger.info(f"✓ Worker server started, listening on {self.host}:{self.port}")
                logger.info(f"✓ Requester API started, listening on {self.host}:{self.requester_port}")
                
                # Only start console if explicitly enabled
                if enable_console:
                    await self.console()
                else:
                    # Wait indefinitely without blocking the event loop
                    while True:
                        await asyncio.sleep(3600)  # Sleep for an hour, but allow interruption
        finally:
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass


# ============ Entry Point ============

async def main():
    import sys
    import os
    import argparse
    
    # Create argument parser
    parser = argparse.ArgumentParser(description='WebSocket Server')
    parser.add_argument('--worker-port', type=int, default=8765, 
                        help='Worker port (default: 8765)')
    parser.add_argument('--requester-port', type=int, default=8766, 
                        help='Requester API port (default: 8766)')
    parser.add_argument('--enable-console', action='store_true',
                        help='Enable server console (default: disabled)')
    parser.add_argument('--handshake-key', type=str, default='deadbeafbeafdead',
                        help='Handshake key for client authentication (default: deadbeafbeafdead)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Use command line arguments, or fall back to environment variables, or defaults
    worker_port = args.worker_port if args.worker_port else \
                  int(os.environ.get("WEBSOCKET_WORKER_PORT", "8765"))
    requester_port = args.requester_port if args.requester_port else \
                     int(os.environ.get("WEBSOCKET_REQUESTER_PORT", "8766"))
    enable_console = args.enable_console
    handshake_key = args.handshake_key if args.handshake_key else \
                    os.environ.get("WEBSOCKET_HANDSHAKE_KEY", "deadbeafbeafdead")
    
    logger.info(f"Using ports: worker_port={worker_port}, requester_port={requester_port}")
    logger.info(f"Using handshake key: {handshake_key}")
    
    server = WebSocketServer(
        host="0.0.0.0", 
        port=worker_port,              # Worker port
        requester_port=requester_port,    # Requester API port
        lb_strategy="least_loaded",
        cleanup_interval=60,     # Clean up expired data every 60 seconds
        handshake_key=handshake_key  # Handshake key
    )
    
    # Increase max message size for server as well
    await server.start(enable_console)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nServer shut down")
