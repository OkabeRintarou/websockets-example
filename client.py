"""
Improved WebSocket Client
- Using JSON message protocol
- Message handler pattern (easily extensible)
- Supports automatic reconnection
"""
import asyncio
import logging
from typing import Dict, Callable
from datetime import datetime
import websockets
import time
import concurrent.futures
import multiprocessing
import pandas as pd
import kline

from message_protocol import (
    Message, MessageType, create_request, 
    create_response, create_heartbeat, create_handshake
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class WebSocketClient:
    """WebSocket Client Class"""
    
    def __init__(self, server_url: str = "ws://localhost:8765", handshake_key: str = "deadbeafbeafdead"):
        self.server_url = server_url
        self.ws = None
        self.client_id = None
        self.running = False
        self.handshake_key = handshake_key  # Store the handshake key
        
        # Message handler mapping (core extensibility point)
        self.handlers: Dict[MessageType, Callable] = {
            MessageType.NOTIFICATION: self._handle_notification,
            MessageType.REQUEST: self._handle_request,
            MessageType.HEARTBEAT: self._handle_heartbeat,
            MessageType.ERROR: self._handle_error,
        }
        
        # Request handler mapping (handles requests from server)
        self.request_handlers: Dict[str, Callable] = {
            "get_time": self._action_get_time,
            "get_system_info": self._action_get_system_info,
            "get_market" : self._action_get_market,
            "get_markets" : self._action_get_markets,
            # Easily extend new actions
        }
    
    # ============ Message Processing Core ============
    
    async def handle_message(self, raw_message: str):
        """Process received messages (dispatch based on message type)"""
        try:
            msg = Message.from_json(raw_message)
            logger.debug(f"Message received: {msg.type}")
            
            # Dispatch to corresponding handler based on message type
            handler = self.handlers.get(msg.type)
            if handler:
                await handler(msg)
            else:
                logger.warning(f"Unknown message type: {msg.type}")
        
        except ValueError as e:
            logger.error(f"Message parsing failed: {e}")
    
    # ============ Specific Message Handlers ============
    
    async def _handle_notification(self, msg: Message):
        """Handle notification messages"""
        title = msg.data.get("title", "Notification")
        content = msg.data.get("content", "")
        level = msg.data.get("level", "info")
        
        logger.info(f"[{level.upper()}] {title}: {content}")
    
    async def _handle_heartbeat(self, msg: Message):
        """Handle heartbeat messages"""
        logger.debug("Server heartbeat received")
    
    async def _handle_error(self, msg: Message):
        """Handle error messages"""
        error = msg.data.get("error", "Unknown error")
        code = msg.data.get("code", "")
        logger.error(f"Server error [{code}]: {error}")
    
    async def _handle_request(self, msg: Message):
        """Handle server requests"""
        action = msg.data.get("action")
        params = msg.data.get("params", {})
        
        logger.info(f"Server request received: {action}")
        
        # Find corresponding action handler
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
        
        await self.send(response)
        logger.info(f"Request responded: {action}")
    
    # ============ Action Handlers (Extensible) ============
    
    async def _action_get_time(self, params: dict) -> str:
        """Get client time"""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    async def _action_get_market(self, params: dict) -> dict:
        """Get market data via HTTP request"""
        
        try:
            # Use run_in_executor with None to automatically use the current event loop
            # Pass the params dict to the request function
            start_time = time.time()
            response_data = await asyncio.get_event_loop().run_in_executor(
                None, 
                self._perform_market_request, 
                params
            )
            elapsed_time = time.time() - start_time
            
            # Log the execution time
            symbol = params.get('code', 'unknown')
            period = params.get('period', 'unknown')
            logger.info(f"Market request for {symbol} (period: {period}) completed in {elapsed_time:.2f} seconds")
            
            # If response is successful and contains data, try to convert to DataFrame and log time
            if response_data.get("success") and "data" in response_data:
                df_conversion_start = time.time()
                try:
                    # Try to convert to DataFrame
                    data_list = response_data.get("data", [])
                    if isinstance(data_list, list) and len(data_list) > 0:
                        df = pd.DataFrame(data_list)
                        df_conversion_time = time.time() - df_conversion_start
                        logger.info(f"JSON to DataFrame conversion for {symbol} completed in {df_conversion_time:.4f} seconds, DataFrame shape: {df.shape}")
                except Exception as e:
                    df_conversion_time = time.time() - df_conversion_start
                    logger.warning(f"Failed to convert JSON to DataFrame for {symbol} in {df_conversion_time:.4f} seconds: {e}")
            
            return response_data
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _perform_market_request(self, params: dict) -> dict:
        """
        Perform the actual market data request (runs in thread pool)
        
        Args:
            params: All parameters passed to the action
                   Required: symbol_type, period, start_date, end_date
                   Optional: format_type, adjust
            
        Returns:
            dict: Response data
        """
        
        # Validate all parameters
        validation_result = self._validate_market_params(params)
        if not validation_result["success"]:
            return validation_result
        
        # Extract validated parameters
        validated_params = validation_result["data"]
        symbol_type = validated_params['symbol_type']
        code = validated_params['code']
        period = validated_params['period']
        start_datetime = validated_params['start_date']
        end_datetime = validated_params['end_date']
        format_type = validated_params['format_type']
        adjust = validated_params['adjust']
        
        try:
            # Measure the time taken to execute kline.get_quotation
            start_time = time.time()
            json_data = kline.get_quotation(symbol_type, code, period, start_datetime, end_datetime, adjust)
            elapsed_time = time.time() - start_time
            
            # Log the execution time with period and success/failure information
            success = json_data.get("success", False)
            logger.info(f"kline.get_quotation for {code} (period: {period}) {'succeeded' if success else 'failed'} in {elapsed_time:.2f} seconds")
            return json_data
        except Exception as e:
            logger.info(f"kline.get_quotation for {code} (period: {period}) failed with exception in {time.time() - start_time:.2f} seconds: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def _action_get_markets(self, params: dict) -> dict:
        """
        Batch get market data for multiple symbols
        
        Args:
            params: [
                {"symbol_type": "etf", "code": "588000", ...},
                ...
            ]
        
        Returns:
            dict with results, summary, etc.
        """
        import json
        logger.info(f"🔵 _action_get_markets called")
        logger.info(f"🔵 params type: {type(params)}")

        # Handle both direct list and nested dict formats
        if isinstance(params, dict) and "markets" in params:
            markets = params["markets"]
            logger.info("🔄 Using nested 'markets' format")
        elif isinstance(params, list):
            markets = params
            logger.info("✅ Using direct list format")
        else:
            logger.warning("⚠️  Invalid parameter format")
            return {
                "success": False,
                "error": "Parameters must be a list of market objects or a dict with 'markets' key containing a list",
                "results": []
            }
        
        if not markets:
            logger.warning("⚠️  No markets specified")
            return {
                "success": False,
                "error": "No markets specified",
                "results": []
            }
        
        if not isinstance(markets, list):
            logger.warning(f"⚠️  markets is not a list: {type(markets)}")
            return {
                "success": False,
                "error": "markets must be a list",
                "results": []
            }
        
        logger.info(f"📊 Batch querying {len(markets)} markets")
        
        # Create tasks
        tasks = []
        for market_params in markets:
            task = self._action_get_market(market_params)
            tasks.append(task)
        
        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        success_count = 0
        failed_count = 0
        
        # Track DataFrame conversion time for all markets
        total_df_conversion_time = 0
        df_conversion_count = 0
        
        for market_params, result in zip(markets, results):
            symbol = market_params.get("code", "UNKNOWN")
            period = market_params.get("period", "UNKNOWN")
            
            if isinstance(result, Exception):
                logger.error(f"  ❌ {symbol} (period: {period}): {result}")
                processed_results.append({
                    "symbol": symbol,
                    "success": False,
                    "error": str(result)
                })
                failed_count += 1
            else:
                if result.get("success", True):
                    logger.info(f"  ✅ {symbol} (period: {period}): Success")
                    
                    # If result is successful, track DataFrame conversion time
                    if "data" in result:
                        df_conversion_start = time.time()
                        try:
                            # Try to convert to DataFrame
                            data_list = result.get("data", [])
                            if isinstance(data_list, list) and len(data_list) > 0:
                                df = pd.DataFrame(data_list)
                                df_conversion_time = time.time() - df_conversion_start
                                total_df_conversion_time += df_conversion_time
                                df_conversion_count += 1
                                logger.debug(f"JSON to DataFrame conversion for {symbol} completed in {df_conversion_time:.4f} seconds, DataFrame shape: {df.shape}")
                        except Exception as e:
                            df_conversion_time = time.time() - df_conversion_start
                            logger.warning(f"Failed to convert JSON to DataFrame for {symbol} in {df_conversion_time:.4f} seconds: {e}")
                    
                    processed_results.append({
                        "symbol": symbol,
                        "success": True,
                        "data": result
                    })
                    success_count += 1
                else:
                    logger.warning(f"  ❌ {symbol} (period: {period}): {result.get('error')}")
                    processed_results.append({
                        "symbol": symbol,
                        "success": False,
                        "error": result.get("error", "Unknown error")
                    })
                    failed_count += 1
        
        # Log average DataFrame conversion time
        if df_conversion_count > 0:
            avg_df_conversion_time = total_df_conversion_time / df_conversion_count
            logger.info(f"📈 Average JSON to DataFrame conversion time: {avg_df_conversion_time:.4f} seconds across {df_conversion_count} successful conversions")
        
        logger.info(f"✅ Completed: {success_count}/{len(markets)} successful")
        
        return {
            "success": True,
            "results": processed_results,
            "summary": {
                "total": len(markets),
                "success": success_count,
                "failed": failed_count
            }
        }

    def _validate_market_params(self, params: dict) -> dict:
        """
        Validate market data request parameters.
        
        Args:
            params: Parameters to validate
            
        Returns:
            dict: Validation result with success flag and validated data or error message
        """
        from datetime import datetime
        
        # Define valid values
        VALID_SYMBOL_TYPES = ['stock', 'etf', 'futures', 'bond', 'fund', 'index']
        VALID_PERIODS = [
            '1', '5', '15', '30', '60', '120',  # 分钟级别
            'daily', 'weekly', 'monthly'  # 大周期级别
        ]
        VALID_ADJUST = ['none', 'qfq', 'hfq']
        
        # Extract required parameters
        symbol_type = params.get('symbol_type')
        code = params.get('code')
        period = params.get('period')
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        
        # Extract optional parameters with defaults
        format_type = params.get('format', 'json')
        adjust = params.get('adjust', 'qfq')
        
        # Validate required parameters
        if not symbol_type:
            return {"success": False, "error": "Missing required parameter: symbol_type"}
        
        # Validate symbol_type
        if symbol_type not in VALID_SYMBOL_TYPES:
            return {"success": False, "error": f"Invalid symbol_type: {symbol_type}. Valid values are: {VALID_SYMBOL_TYPES}"}
        
        if not code:
            return {"success": False, "error": "Missing required parameter: code"}

        if not period:
            return {"success": False, "error": "Missing required parameter: period"}
            
        # Validate period
        if period not in VALID_PERIODS:
            return {"success": False, "error": f"Invalid period: {period}. Valid values are: {VALID_PERIODS}"}
            
        if not start_date:
            return {"success": False, "error": "Missing required parameter: start_date"}
            
        if not end_date:
            return {"success": False, "error": "Missing required parameter: end_date"}
        
        # Validate adjust parameter
        if adjust not in VALID_ADJUST:
            return {"success": False, "error": f"Invalid adjust: {adjust}. Valid values are: {VALID_ADJUST}"}
        
        # Validate and parse date formats
        try:
            parsed_start_date = self._parse_datetime(start_date)
        except ValueError as e:
            return {"success": False, "error": f"Invalid start_date format: {str(e)}"}
            
        try:
            parsed_end_date = self._parse_datetime(end_date)
        except ValueError as e:
            return {"success": False, "error": f"Invalid end_date format: {str(e)}"}
        
        # All validations passed - return validated and parsed parameters
        validated_data = {
            'symbol_type': symbol_type,
            'code' : code,
            'period': period,
            'start_date': parsed_start_date,
            'end_date': parsed_end_date,
            'format_type': format_type,
            'adjust': adjust
        }
        
        return {"success": True, "data": validated_data}
    
    def _parse_datetime(self, date_string):
        """
        Parse datetime string to datetime object.
        Supports both date-only and datetime formats.
        
        Args:
            date_string (str): Date string in format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            datetime: Parsed datetime object or None if input is None
        """
        from datetime import datetime
        
        if not date_string:
            return None

        # Try parsing as datetime first (with time components)
        try:
            return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass

        # Try parsing as date only (without time components)
        try:
            return datetime.strptime(date_string, '%Y-%m-%d')
        except ValueError:
            pass

        # If neither format works, raise an error
        raise ValueError(f"Invalid date format: {date_string}. Expected 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'")
    
    async def _action_get_system_info(self, params: dict) -> dict:
        """Get system information"""
        import platform
        import sys
        import psutil
        
        # Get CPU information
        cpu_count_physical = psutil.cpu_count(logical=False)
        cpu_count_logical = psutil.cpu_count(logical=True)
        
        # Get memory information
        memory = psutil.virtual_memory()
        memory_total_gb = round(memory.total / (1024**3), 2)
        
        return {
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": sys.version,
            "machine": platform.machine(),
            "cpu_count_physical": cpu_count_physical,
            "cpu_count_logical": cpu_count_logical,
            "memory_total_gb": memory_total_gb
        }
    
    # ============ Send Messages ============
    
    async def send(self, msg: Message):
        """Send message to server"""
        if self.ws:
            try:
                await self.ws.send(msg.to_json())
                logger.debug(f"Message sent: {msg.type}")
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
    
    async def send_request(self, action: str, params: dict = None):
        """Send request to server"""
        msg = create_request(action, params)
        await self.send(msg)
        logger.info(f"Request sent: {action}")
    
    async def send_markets_request(self, markets_params: list):
        """Send request for multiple markets to server
        markets_params: list of dicts, each dict is the same as get_market params
        """
        msg = create_request("get_markets", markets_params)
        await self.send(msg)
        logger.info(f"Request sent: get_markets with {len(markets_params)} markets")
    
    # ============ Heartbeat Mechanism ============
    
    async def heartbeat_loop(self, interval: int = 30):
        """Heartbeat loop"""
        while self.running:
            await asyncio.sleep(interval)
            if self.ws:
                await self.send(create_heartbeat())
                logger.debug("Heartbeat sent")
    
    # ============ Connection Management ============
    
    async def connect(self):
        """Connect to server"""
        logger.info(f"Connecting to server: {self.server_url}")
        
        try:
            # Increase max message size to handle large market data responses
            self.ws = await websockets.connect(
                self.server_url,
                ping_interval=60,
                ping_timeout=120,
                max_size=5 * 1024 * 1024,  # 5MB max message size
                close_timeout=10  # Set close timeout
            )
            self.running = True
            logger.info("✓ Connected to server")
            
            # Send handshake message immediately after connection
            handshake_msg = create_handshake(self.handshake_key)
            await self.send(handshake_msg)
            logger.debug("Handshake message sent")
            
            # Start heartbeat task
            heartbeat_task = asyncio.create_task(self.heartbeat_loop())
            
            try:
                # Continuously receive messages
                async for raw_message in self.ws:
                    await self.handle_message(raw_message)
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}")
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
        
        except OSError as e:
            # Catch network connection errors, including DNS resolution errors
            logger.error(f"Network error: {e}")
        except websockets.exceptions.ConnectionClosedOK:
            logger.info("Connection closed normally")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"Connection closed abnormally: {e}")
        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            self.running = False
            if self.ws:
                try:
                    await self.ws.close()
                except:
                    pass
                self.ws = None
    
    async def connect_with_retry(self, max_retries: int = -1, retry_interval: int = 10):
        """Connection with reconnection"""
        retry_count = 0
        
        while max_retries < 0 or retry_count < max_retries:
            try:
                await self.connect()
            except KeyboardInterrupt:
                logger.info("User interrupted connection")
                break
            
            if max_retries >= 0:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Maximum reconnection attempts reached {max_retries}")
                    break
            
            logger.info(f"Reconnecting in {retry_interval} seconds...")
            await asyncio.sleep(retry_interval)
    
    # ============ Client Console (Optional) ============
    
    async def console(self):
        """Client console for manual request sending"""
        logger.info("Client console started, type 'help' for commands")
        
        while self.running:
            try:
                cmd = await asyncio.to_thread(
                    input,
                    "\n[Client] > "
                )
                
                if not cmd.strip():
                    continue
                
                parts = cmd.split(maxsplit=1)
                command = parts[0].lower()
                
                if command == "help":
                    print("\nAvailable commands:")
                    print("  request <action> [params] - Send request to server")
                    print("  quit - Disconnect")
                    print("\nExamples:")
                    print("  request get_time")
                    print("  request calculate {'a': 10, 'b': 20, 'op': '+'}")
                
                elif command == "request" and len(parts) > 1:
                    args = parts[1].split(maxsplit=1)
                    action = args[0]
                    params = eval(args[1]) if len(args) > 1 else {}
                    await self.send_request(action, params)
                
                elif command == "quit":
                    logger.info("Disconnecting...")
                    self.running = False
                    if self.ws:
                        await self.ws.close()
                    break
                
                else:
                    print("Unknown command, type 'help' for help")
            
            except Exception as e:
                logger.error(f"Console error: {e}")
    
    # ============ Start Client ============
    
    async def start(self, enable_console: bool = False, max_retries: int = 10):
        """Start Client"""
        if enable_console:
            # Start both connection and console simultaneously
            await asyncio.gather(
                self.connect_with_retry(max_retries=max_retries),
                self.console()
            )
        else:
            # Start connection only (continuous running)
            await self.connect_with_retry(max_retries=max_retries)


# ============ Entry Point ============

async def main():
    import sys
    import os
    import argparse
    
    # Create argument parser
    parser = argparse.ArgumentParser(description='WebSocket Client')
    parser.add_argument('host', nargs='?', default='localhost',
                        help='Server host (default: localhost)')
    parser.add_argument('port', nargs='?', default='8765',
                        help='Server port (default: 8765)')
    parser.add_argument('--handshake-key', type=str, default='deadbeafbeafdead',
                        help='Handshake key for server authentication (default: deadbeafbeafdead)')
    parser.add_argument('--max-retries', type=int, default=10,
                        help='Maximum number of reconnection attempts (default: 10, -1 for infinite)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Priority: Command line arguments > Environment variables > Default values
    if args.host and args.port:
        # Read from command line arguments: python client.py <host> <port>
        host = args.host
        port = args.port
        server_url = f"ws://{host}:{port}"
        logger.info(f"Using command line arguments: {server_url}")
    elif "WEBSOCKET_SERVER_HOST" in os.environ:
        # Read from environment variables
        host = os.environ.get("WEBSOCKET_SERVER_HOST", "localhost")
        port = os.environ.get("WEBSOCKET_SERVER_PORT", "8765")
        server_url = f"ws://{host}:{port}"
        logger.info(f"Using environment variables: {server_url}")
    else:
        # Use default values
        server_url = "ws://localhost:8765"
        logger.info(f"Using default values: {server_url}")
    
    handshake_key = args.handshake_key if args.handshake_key else \
                    os.environ.get("WEBSOCKET_HANDSHAKE_KEY", "deadbeafbeafdead")
    
    max_retries = args.max_retries if args.max_retries else \
                  int(os.environ.get("WEBSOCKET_MAX_RETRIES", "10"))
    
    logger.info(f"Using handshake key: {handshake_key}")
    logger.info(f"Using max retries: {max_retries}")
    
    # Create client
    client = WebSocketClient(server_url=server_url, handshake_key=handshake_key)
    
    # Increase thread pool size based on CPU cores for better concurrent performance
    cpu_count = multiprocessing.cpu_count()
    # Use 4 times the CPU count, but at least 10 and at most 50
    thread_count = max(10, min(50, cpu_count * 4))
    
    logger.info(f"System has {cpu_count} CPU cores, setting thread pool size to {thread_count}")
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_count)
    loop.set_default_executor(executor)
    
    # Method 1: Maintain connection only, handle server requests
    await client.start(enable_console=False, max_retries=max_retries)
    
    # Method 2: Enable console, manually send requests
    # await client.start(enable_console=True, max_retries=max_retries)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nClient closed")
