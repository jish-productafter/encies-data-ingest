"""
WebSocket client for managing real-time data connections to Polymarket.
"""
import asyncio
import json
import websockets
from typing import Optional, Callable, Dict, Any
from .model import Message, SubscriptionMessage, ConnectionStatus


DEFAULT_HOST = "wss://ws-live-data.polymarket.com"
DEFAULT_PING_INTERVAL = 5  # seconds


class RealTimeDataClient:
    """
    A client for managing real-time WebSocket connections, handling messages,
    subscriptions, and automatic reconnections.
    """
    
    def __init__(
        self,
        on_connect: Optional[Callable[["RealTimeDataClient"], None]] = None,
        on_message: Optional[Callable[["RealTimeDataClient", Message], None]] = None,
        on_status_change: Optional[Callable[[ConnectionStatus], None]] = None,
        host: str = DEFAULT_HOST,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        auto_reconnect: bool = True
    ):
        """
        Constructs a new RealTimeDataClient instance.
        
        Args:
            on_connect: Optional callback function that is called when the client successfully connects
            on_message: Optional callback function that is called when the client receives a message
            on_status_change: Optional callback function that is called when the client receives a connection status update
            host: Optional host address to connect to
            ping_interval: Optional interval in seconds for sending ping messages to keep the connection alive
            auto_reconnect: Optional flag to enable or disable automatic reconnection when the connection is lost
        """
        self.host = host
        self.ping_interval = ping_interval
        self.auto_reconnect = auto_reconnect
        self.on_custom_message = on_message
        self.on_connect = on_connect
        self.on_status_change = on_status_change
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def connect(self) -> "RealTimeDataClient":
        """
        Establishes a WebSocket connection to the server.
        
        Returns:
            Self for method chaining
        """
        self.notify_status_change(ConnectionStatus.CONNECTING)
        self._running = True
        
        try:
            self.ws = await websockets.connect(self.host)
            self.notify_status_change(ConnectionStatus.CONNECTED)
            
            # Start ping task
            self._ping_task = asyncio.create_task(self._ping_loop())
            
            # Start message handler
            asyncio.create_task(self._message_handler())
            
            if self.on_connect:
                # Call on_connect in a way that doesn't block
                if asyncio.iscoroutinefunction(self.on_connect):
                    await self.on_connect(self)
                else:
                    self.on_connect(self)
            
            return self
        except Exception as e:
            print(f"Connection error: {e}")
            self.notify_status_change(ConnectionStatus.DISCONNECTED)
            if self.auto_reconnect:
                await asyncio.sleep(1)
                return await self.connect()
            raise
    
    async def _ping_loop(self) -> None:
        """Continuously sends ping messages to keep the connection alive."""
        while self._running and self.ws:
            try:
                # Check if connection is closed by checking close_code
                if self.ws.close_code is not None:
                    break
                await self.ws.ping()
                await asyncio.sleep(self.ping_interval)
            except (websockets.exceptions.ConnectionClosed, Exception) as e:
                print(f"Ping error: {e}")
                break
    
    async def _message_handler(self) -> None:
        """Handles incoming WebSocket messages."""
        if not self.ws:
            return
        
        try:
            while self._running and self.ws:
                # Check if connection is closed
                if self.ws.close_code is not None:
                    break
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=1.0)
                    if isinstance(message, str) and len(message) > 0:
                        if self.on_custom_message and "payload" in message:
                            try:
                                parsed_message = json.loads(message)
                                # Call on_custom_message - handle both sync and async callbacks
                                if asyncio.iscoroutinefunction(self.on_custom_message):
                                    await self.on_custom_message(self, parsed_message)
                                else:
                                    self.on_custom_message(self, parsed_message)
                            except json.JSONDecodeError as e:
                                print(f"Error parsing message: {e}")
                        else:
                            print(f"onMessage error: {{'event': {message}}}")
                except asyncio.TimeoutError:
                    # Timeout is fine, just continue the loop
                    continue
        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
            self.notify_status_change(ConnectionStatus.DISCONNECTED)
            if self.auto_reconnect and self._running:
                await asyncio.sleep(1)
                await self.connect()
        except Exception as e:
            print(f"Error in message handler: {e}")
            self.notify_status_change(ConnectionStatus.DISCONNECTED)
            if self.auto_reconnect and self._running:
                await asyncio.sleep(1)
                await self.connect()
    
    async def disconnect(self) -> None:
        """Closes the WebSocket connection."""
        self.auto_reconnect = False
        self._running = False
        if self._ping_task:
            self._ping_task.cancel()
        if self.ws:
            try:
                # Check if connection is already closed
                if self.ws.close_code is None:
                    await self.ws.close()
            except Exception as e:
                print(f"Error closing connection: {e}")
        self.notify_status_change(ConnectionStatus.DISCONNECTED)
    
    async def subscribe(self, msg: SubscriptionMessage) -> None:
        """
        Subscribes to a data stream by sending a subscription message.
        
        Args:
            msg: Subscription request message
        """
        if not self.ws or self.ws.close_code is not None:
            print("Socket not open. Cannot subscribe.")
            return
        
        try:
            subscribe_msg = {"action": "subscribe", **msg}
            await self.ws.send(json.dumps(subscribe_msg))
        except Exception as e:
            print(f"Subscribe error: {e}")
            if self.ws:
                await self.ws.close()
    
    async def unsubscribe(self, msg: SubscriptionMessage) -> None:
        """
        Unsubscribes from a data stream by sending an unsubscription message.
        
        Args:
            msg: Unsubscription request message
        """
        if not self.ws or self.ws.close_code is not None:
            print("Socket not open. Cannot unsubscribe.")
            return
        
        try:
            print(f"Unsubscribing: {msg}")
            unsubscribe_msg = {"action": "unsubscribe", **msg}
            await self.ws.send(json.dumps(unsubscribe_msg))
        except Exception as e:
            print(f"Unsubscribe error: {e}")
            if self.ws:
                await self.ws.close()
    
    def notify_status_change(self, status: ConnectionStatus) -> ConnectionStatus:
        """
        Callback for connection status changes.
        
        Args:
            status: Status of the connection
            
        Returns:
            The status that was set
        """
        if self.on_status_change:
            self.on_status_change(status)
        return status

