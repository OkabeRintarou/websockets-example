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
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import logging


@dataclass
class MarketParams:
    """
    Market data parameters for requesting market information
    
    Attributes:
        symbol_type: Type of symbol (stock, etf, futures, bond, fund, index)
        code: Symbol code
        period: Time period (1, 5, 15, 30, 60, 120, daily, weekly, monthly)
        start_date: Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        end_date: End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        format: Format type (default: json)
        adjust: Adjustment method (none, qfq, hfq)
    """
    symbol_type: str
    code: str
    period: str
    start_date: str
    end_date: str
    format: str = "json"
    adjust: str = "qfq"
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert MarketParams object to dictionary
        
        Returns:
            Dict[str, Any]: Dictionary representation of MarketParams
        """
        return {
            "symbol_type": self.symbol_type,
            "code": self.code,
            "period": self.period,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "format": self.format,
            "adjust": self.adjust
        }


class RequesterClient:
    def __init__(self, server_url: str = "ws://localhost:8766", logger: Optional[logging.Logger] = None):
        self.server_url = server_url
        self.ws = None
        self.logger = logger or logging.getLogger(__name__)
        self._loop = None
    
    async def connect(self):
        """Connect to the server's Requester API"""
        self.logger.info(f"Connecting to Requester API: {self.server_url}")
        self.ws = await websockets.connect(self.server_url, max_size=5 * 1024 * 1024)  # 5MB max message size
        self.logger.info("Connected")
    
    def connect_sync(self):
        """Synchronous version of connect"""
        self.logger.info("Creating new event loop for synchronous operation")
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self.logger.info("Running connect() in event loop")
            self._loop.run_until_complete(self.connect())
            self.logger.info("Connection established successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            raise
    
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
        
        self.logger.info(f"Sending request [{request_id[:8]}]: {command}")
        start_time = asyncio.get_event_loop().time()
        await self.ws.send(json.dumps(request))
        self.logger.debug(f"Request sent: {request}")
        
        # Wait for response
        self.logger.debug("Waiting for response...")
        response_raw = await self.ws.recv()
        end_time = asyncio.get_event_loop().time()
        elapsed_time = end_time - start_time
        self.logger.debug("Response received")
        response = json.loads(response_raw)
        
        if response.get("success"):
            self.logger.info(f"Request succeeded [{request_id[:8]}] in {elapsed_time:.2f} seconds")
            self.logger.info(f"Processed by: {response.get('processed_by')}")
            # Only print result if it's small enough to avoid flooding the console
            result_data = response.get('data')
            if result_data and isinstance(result_data, dict) and len(str(result_data)) < 500:
                self.logger.info(f"Result: {result_data}")
            elif result_data:
                self.logger.info(f"Result: <data truncated, {len(str(result_data))} characters>")
            else:
                self.logger.info(f"Result: {result_data}")
        else:
            self.logger.error(f"Request failed [{request_id[:8]}] in {elapsed_time:.2f} seconds")
            self.logger.error(f"Error: {response.get('error')}")
        
        return response
    
    def send_request_sync(self, command: str, data: dict = None) -> dict:
        """Synchronous version of send_request"""
        if self._loop is None:
            raise Exception("Not connected. Call connect_sync() first.")
        self.logger.info(f"Sending synchronous request: {command}")
        try:
            start_time = time.time()
            result = self._loop.run_until_complete(self.send_request(command, data))
            end_time = time.time()
            elapsed_time = end_time - start_time
            self.logger.info(f"Synchronous request completed in {elapsed_time:.2f} seconds")
            return result
        except Exception as e:
            self.logger.error(f"Failed to send request: {e}")
            raise
    
    async def get_markets(self, markets: List[MarketParams]) -> dict:
        """
        Get market data for multiple symbols
        
        Args:
            markets: List of MarketParams objects containing market parameters
            
        Returns:
            dict: Response data with market information
        """
        self.logger.info(f"Requesting data for {len(markets)} markets")
        # Convert MarketParams objects to dictionaries
        markets_dict = [market.to_dict() for market in markets]
        self.logger.debug(f"Market parameters: {markets_dict}")
        start_time = asyncio.get_event_loop().time()
        response = await self.send_request("get_markets", {"markets": markets_dict})
        end_time = asyncio.get_event_loop().time()
        elapsed_time = end_time - start_time
        self.logger.info(f"get_markets request completed in {elapsed_time:.2f} seconds")
        return response
    
    def get_markets_sync(self, markets: List[MarketParams]) -> dict:
        """Synchronous version of get_markets"""
        if self._loop is None:
            raise Exception("Not connected. Call connect_sync() first.")
        self.logger.info(f"Requesting data for {len(markets)} markets (synchronous)")
        try:
            start_time = time.time()
            result = self._loop.run_until_complete(self.get_markets(markets))
            end_time = time.time()
            elapsed_time = end_time - start_time
            self.logger.info(f"Synchronous get_markets request completed in {elapsed_time:.2f} seconds")
            return result
        except Exception as e:
            self.logger.error(f"Failed to get markets: {e}")
            raise
    
    async def get_markets_as_dataframe(self, markets: List[MarketParams]) -> List[Optional[pd.DataFrame]]:
        """
        Get market data for multiple symbols and convert to pandas DataFrame
        
        Args:
            markets: List of MarketParams objects containing market parameters
            
        Returns:
            List[Optional[pd.DataFrame]]: List of DataFrames corresponding to input markets list.
                                          If a market request failed or cannot be matched, the corresponding element will be None.
        """
        self.logger.info(f"Requesting market data as DataFrame for {len(markets)} markets")
        response = await self.get_markets(markets)
        
        if not response.get("success"):
            self.logger.error(f"Request failed: {response.get('error')}")
            raise Exception(f"Request failed: {response.get('error')}")
        
        data = response.get("data", {})
        result_dataframes = [None] * len(markets)  # Initialize with None for all markets
        
        # Handle distributed response format
        if "results" in data:
            # Distributed format - extract data from results
            results = data.get("results", [])
            self.logger.info(f"Received {len(results)} results from server")
            
            # If lengths don't match, we can't reliably map responses to requests
            if len(results) != len(markets):
                self.logger.warning(f"Response count ({len(results)}) does not match request count ({len(markets)}). Cannot map responses to requests.")
                return result_dataframes
            
            # Process each result
            for i, result in enumerate(results):
                if "data" in result:
                    market_data = result.get("data", {})
                    
                    # Check if the request was successful
                    if result.get("success") and isinstance(market_data, dict) and "data" in market_data:
                        # Check the format of the data
                        format_type = market_data.get("format", "json")  # Default to json if not specified
                        df_data = market_data.get("data", [])
                        
                        if format_type == "json":
                            if df_data:
                                df = pd.DataFrame(df_data)
                                # Add metadata columns to the DataFrame
                                if "code" in market_data:
                                    df["code"] = market_data["code"]
                                if "name" in market_data:
                                    df["name"] = market_data["name"]
                                result_dataframes[i] = df
                                self.logger.debug(f"Processed market {i} successfully, DataFrame shape: {df.shape}")
                        else:
                            # For non-JSON formats, log a warning
                            symbol = market_data.get("code") or markets[i].code
                            self.logger.warning(f"Unsupported data format '{format_type}' for symbol {symbol}. Only JSON format is currently supported.")
                    elif not result.get("success"):
                        # Request failed, leave None in the corresponding position
                        error_msg = result.get("error", "Unknown error")
                        symbol = markets[i].code if i < len(markets) else "unknown"
                        self.logger.error(f"Request for symbol {symbol} failed: {error_msg}")
        else:
            # Non-distributed format is not supported for this method
            # This method is specifically for get_markets which should return distributed format
            self.logger.warning("Received non-distributed response format")
            pass
        
        self.logger.info(f"Completed processing {len(result_dataframes)} DataFrames")
        return result_dataframes
    
    def get_markets_as_dataframe_sync(self, markets: List[MarketParams]) -> List[Optional[pd.DataFrame]]:
        """Synchronous version of get_markets_as_dataframe"""
        if self._loop is None:
            raise Exception("Not connected. Call connect_sync() first.")
        self.logger.info(f"Requesting market data as DataFrame for {len(markets)} markets (synchronous)")
        try:
            result = self._loop.run_until_complete(self.get_markets_as_dataframe(markets))
            self.logger.info("Markets data as DataFrame request completed successfully")
            return result
        except Exception as e:
            self.logger.error(f"Failed to get markets as DataFrame: {e}")
            raise
    
    async def get_market_as_dataframe(self, market: MarketParams) -> Optional[pd.DataFrame]:
        """
        Get market data for a single symbol and convert to pandas DataFrame
        
        Args:
            market: MarketParams object containing market parameters
            
        Returns:
            Optional[pd.DataFrame]: DataFrame with market data, or None if request failed
        """
        self.logger.info(f"Requesting market data as DataFrame for single market: {market.code}")
        # Convert MarketParams object to dictionary
        market_dict = market.to_dict()
        start_time = asyncio.get_event_loop().time()
        response = await self.send_request("get_market", market_dict)
        end_time = asyncio.get_event_loop().time()
        elapsed_time = end_time - start_time
        self.logger.info(f"get_market request for {market.code} completed in {elapsed_time:.2f} seconds")
        
        if not response.get("success"):
            self.logger.error(f"Request for symbol {market.code} failed: {response.get('error')}")
            return None
        
        data = response.get("data", {})
        
        # Check if we have market data
        if isinstance(data, dict) and "data" in data:
            # Check the format of the data
            format_type = data.get("format", "json")  # Default to json if not specified
            df_data = data.get("data", [])
            
            if format_type == "json":
                if df_data:
                    df = pd.DataFrame(df_data)
                    # Add metadata columns to the DataFrame
                    if "code" in data:
                        df["code"] = data["code"]
                    if "name" in data:
                        df["name"] = data["name"]
                    self.logger.info(f"Market data DataFrame created, shape: {df.shape}")
                    return df
            else:
                # For non-JSON formats, log a warning and return None for now
                self.logger.warning(f"Unsupported data format '{format_type}' for symbol {market.code}. Only JSON format is currently supported.")
                return None
        
        self.logger.warning(f"No data received for market {market.code}")
        return None
    
    def get_market_as_dataframe_sync(self, market: MarketParams) -> Optional[pd.DataFrame]:
        """Synchronous version of get_market_as_dataframe"""
        if self._loop is None:
            raise Exception("Not connected. Call connect_sync() first.")
        self.logger.info(f"Requesting market data as DataFrame for single market: {market.code} (synchronous)")
        try:
            start_time = time.time()
            result = self._loop.run_until_complete(self.get_market_as_dataframe(market))
            end_time = time.time()
            elapsed_time = end_time - start_time
            self.logger.info(f"Synchronous get_market request for {market.code} completed in {elapsed_time:.2f} seconds")
            return result
        except Exception as e:
            self.logger.error(f"Failed to get market as DataFrame: {e}")
            raise
    
    async def close(self):
        """Close connection"""
        self.logger.info("Closing connection")
        if self.ws:
            await self.ws.close()
            self.logger.info("Connection closed")
    
    def close_sync(self):
        """Synchronous version of close"""
        self.logger.info("Closing connection (synchronous)")
        if self._loop is None:
            return
        try:
            self._loop.run_until_complete(self.close())
        except Exception as e:
            self.logger.error(f"Failed to close connection: {e}")
        finally:
            self._loop.close()
            self.logger.info("Event loop closed")


async def main():
    # Configure logging for the main function
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    main_logger = logging.getLogger(__name__)
    
    # Get server address from command line arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", default="8766", help="Server port (default: 8766)")
    args = parser.parse_args()
    
    server_url = f"ws://{args.host}:{args.port}"
    
    client = RequesterClient(server_url, main_logger)
    
    try:
        await client.connect()
        
        # Example: Send multiple requests
        main_logger.info("="*50)
        main_logger.info("Example 1: Get time")
        main_logger.info("="*50)
        await client.send_request("get_time")
        
        await asyncio.sleep(1)
        
        main_logger.info("="*50)
        main_logger.info("Example 2: Market data test (50 markets with different ETFs and periods)")
        main_logger.info("="*50)
        # Example market data request with 50 markets using specified ETF codes and different periods
        etf_codes = ["588000", "159682", "513010", "588800"]
        periods = ["1", "5", "15", "30", "60", "120", "daily", "weekly", "monthly"]
        
        # Use current date as end_date and one year ago as start_date
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        # Format dates appropriately
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        # Create 50 market requests
        markets_data = []
        for i in range(50):
            etf_code = etf_codes[i % len(etf_codes)]
            period = periods[i % len(periods)]
            
            # For minute-level periods, we need to specify time with seconds precision
            if period in ["1", "5", "15", "30", "60", "120"]:
                start_date_formatted = start_date_str + " 00:00:00"
                end_date_formatted = end_date_str + " 23:59:59"
            else:
                start_date_formatted = start_date_str
                end_date_formatted = end_date_str
            
            market_param = MarketParams(
                symbol_type="etf",
                code=etf_code,
                period=period,
                start_date=start_date_formatted,
                end_date=end_date_formatted,
                adjust="qfq"
            )
            markets_data.append(market_param)
        
        await client.get_markets(markets_data)
        
        await asyncio.sleep(1)
        
        main_logger.info("="*50)
        main_logger.info("Example 3: Market data test (single market as DataFrame)")
        main_logger.info("="*50)
        # Example market data request using MarketParams object and convert to DataFrame
        # For minute-level periods, we need to specify time with seconds precision
        market_param = MarketParams(
            symbol_type="etf",
            code="588000",
            period="daily",
            start_date=start_date_str,
            end_date=end_date_str,
            adjust="qfq"
        )
        df = await client.get_market_as_dataframe(market_param)
        if df is not None:
            main_logger.info(f"DataFrame shape: {df.shape}")
            if not df.empty:
                main_logger.info("First 5 rows:")
                main_logger.info(df.head())
        else:
            main_logger.info("Failed to get market data")
    
    finally:
        await client.close()


def main_sync():
    """Synchronous version of main for demonstration"""
    # Configure logging for the main function
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S'
    )
    main_logger = logging.getLogger("requester_client.main")
    
    # Get server address from command line arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", default="8766", help="Server port (default: 8766)")
    args = parser.parse_args()
    
    server_url = f"ws://{args.host}:{args.port}"
    
    main_logger.info("Starting synchronous requester client")
    client = RequesterClient(server_url, main_logger)
    
    try:
        main_logger.info("Connecting to server...")
        client.connect_sync()
        main_logger.info("Connected to server successfully")
        
        # Example: Send multiple requests
        main_logger.info("="*50)
        main_logger.info("Example 1: Get time")
        main_logger.info("="*50)
        client.send_request_sync("get_time")
        
        import time
        time.sleep(1)
        
        main_logger.info("="*50)
        main_logger.info("Example 2: Market data test (50 markets with different ETFs and periods)")
        main_logger.info("="*50)
        # Example market data request with 50 markets using specified ETF codes and different periods
        etf_codes = ["588000", "159682", "513010", "588800"]
        periods = ["1", "5", "15", "30", "60", "120", "daily", "weekly", "monthly"]
        
        # Use current date as end_date and one year ago as start_date
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        # Format dates appropriately
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        # Create 50 market requests
        markets_data = []
        for i in range(50):
            etf_code = etf_codes[i % len(etf_codes)]
            period = periods[i % len(periods)]
            
            # For minute-level periods, we need to specify time with seconds precision
            if period in ["1", "5", "15", "30", "60", "120"]:
                start_date_formatted = start_date_str + " 00:00:00"
                end_date_formatted = end_date_str + " 23:59:59"
            else:
                start_date_formatted = start_date_str
                end_date_formatted = end_date_str
            
            market_param = MarketParams(
                symbol_type="etf",
                code=etf_code,
                period=period,
                start_date=start_date_formatted,
                end_date=end_date_formatted,
                adjust="qfq"
            )
            markets_data.append(market_param)
        
        client.get_markets_sync(markets_data)
        
        time.sleep(1)
        
        main_logger.info("="*50)
        main_logger.info("Example 3: Market data test (single market as DataFrame)")
        main_logger.info("="*50)
        # Example market data request using MarketParams object and convert to DataFrame
        # For minute-level periods, we need to specify time with seconds precision
        market_param = MarketParams(
            symbol_type="etf",
            code="588000",
            period="daily",
            start_date=start_date_str,
            end_date=end_date_str,
            adjust="qfq"
        )
        df = client.get_market_as_dataframe_sync(market_param)
        if df is not None:
            main_logger.info(f"DataFrame shape: {df.shape}")
            if not df.empty:
                main_logger.info("First 5 rows:")
                main_logger.info(df.head())
        else:
            main_logger.info("Failed to get market data")
    
    except Exception as e:
        main_logger.error(f"Error in main_sync: {e}")
        import traceback
        main_logger.error(traceback.format_exc())
    finally:
        main_logger.info("Closing connection...")
        client.close_sync()
        main_logger.info("Client closed")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sync", action="store_true", help="Run in synchronous mode")
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", default="8766", help="Server port (default: 8766)")
    args = parser.parse_args()
    
    try:
        if args.sync:
            main_sync()
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
