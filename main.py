"""
Main entry point for Polymarket real-time data monitoring.
"""

import asyncio
import json
import signal
from datetime import datetime
from typing import Dict, List, Optional
from src.api import get_polymarket_events
from src.client import RealTimeDataClient
from src.model import (
    CryptoPricesChainlinkUpdateMessage,
    CryptoPricesSubscribeMessage,
    ClobMarketPriceChangeMessage,
    Message,
    TokenPriceChange,
    ExchangePriceChange,
)
from db import insert_token_price_changes, insert_exchange_price_changes


token_id_to_outcome_map: Dict[str, str] = {}

# Batch insert buffers
exchange_price_buffer: List[ExchangePriceChange] = []
token_price_buffer: List[TokenPriceChange] = []
BATCH_SIZE = 100  # Insert when buffer reaches this size
FLUSH_INTERVAL_SECONDS = 5  # Flush buffers every N seconds
batch_lock = asyncio.Lock()  # Lock for thread-safe buffer operations


async def flush_exchange_price_buffer() -> None:
    """Flush exchange price buffer to database."""
    global exchange_price_buffer
    async with batch_lock:
        if exchange_price_buffer:
            items_to_insert = exchange_price_buffer.copy()
            exchange_price_buffer.clear()
            if items_to_insert:
                insert_exchange_price_changes(items_to_insert)
                print(
                    f"Flushed {len(items_to_insert)} exchange price changes to database"
                )


async def flush_token_price_buffer() -> None:
    """Flush token price buffer to database."""
    global token_price_buffer
    async with batch_lock:
        if token_price_buffer:
            items_to_insert = token_price_buffer.copy()
            token_price_buffer.clear()
            if items_to_insert:
                insert_token_price_changes(items_to_insert)
                print(f"Flushed {len(items_to_insert)} token price changes to database")


async def flush_all_buffers() -> None:
    """Flush all buffers to database."""
    await flush_exchange_price_buffer()
    await flush_token_price_buffer()


async def add_to_exchange_price_buffer(items: List[ExchangePriceChange]) -> None:
    """Add items to exchange price buffer and flush if threshold reached."""
    global exchange_price_buffer
    async with batch_lock:
        exchange_price_buffer.extend(items)
        if len(exchange_price_buffer) >= BATCH_SIZE:
            items_to_insert = exchange_price_buffer.copy()
            exchange_price_buffer.clear()
            if items_to_insert:
                insert_exchange_price_changes(items_to_insert)
                print(f"Batch inserted {len(items_to_insert)} exchange price changes")


async def add_to_token_price_buffer(items: List[TokenPriceChange]) -> None:
    """Add items to token price buffer and flush if threshold reached."""
    global token_price_buffer
    async with batch_lock:
        token_price_buffer.extend(items)
        if len(token_price_buffer) >= BATCH_SIZE:
            items_to_insert = token_price_buffer.copy()
            token_price_buffer.clear()
            if items_to_insert:
                insert_token_price_changes(items_to_insert)
                print(f"Batch inserted {len(items_to_insert)} token price changes")


async def periodic_buffer_flush() -> None:
    """Periodically flush buffers to database."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        await flush_all_buffers()


def on_message(_: RealTimeDataClient, message: Message) -> None:
    """Handle incoming WebSocket messages."""
    if message["topic"] == "crypto_prices":
        if message["type"] == "subscribe":
            # console.log(JSON.stringify(message, null, 2))
            crypto_prices_subscribe_message = (
                message
            )  # type: CryptoPricesSubscribeMessage
            # set message to CryptoPricesSubscribeMessage
            message = CryptoPricesSubscribeMessage(
                payload=message["payload"],
                timestamp=message["timestamp"],
                topic=message["topic"],
                type=message["type"],
            )
            arr: List[ExchangePriceChange] = []
            for item in message["payload"]["data"]:
                arr.append(
                    ExchangePriceChange(
                        symbol=message["payload"]["symbol"],
                        timestamp=item["timestamp"],
                        price=item["value"],
                    )
                )
            # Add to buffer instead of inserting immediately
            try:
                asyncio.create_task(add_to_exchange_price_buffer(arr))
            except (RuntimeError, AttributeError):
                # Fallback: insert directly if no event loop is available
                insert_exchange_price_changes(arr)
    elif message["topic"] == "crypto_prices_chainlink" and message["type"] == "update":
        message = CryptoPricesChainlinkUpdateMessage(
            payload=message["payload"],
            timestamp=message["timestamp"],
            topic=message["topic"],
            type=message["type"],
        )
        arr: List[ExchangePriceChange] = [
            ExchangePriceChange(
                symbol=message["payload"]["symbol"],
                timestamp=message["payload"]["timestamp"],
                price=message["payload"]["value"],
            )
        ]
        # Add to buffer instead of inserting immediately
        try:
            asyncio.create_task(add_to_exchange_price_buffer(arr))
        except (RuntimeError, AttributeError):
            # Fallback: insert directly if no event loop is available
            insert_exchange_price_changes(arr)

    elif message["topic"] == "clob_market":
        pass
        message_type = message["type"]
        if message_type == "price_change":
            # console.log("clob_market" + "price_change")
            # console.log(JSON.stringify(message, null, 2))
            price_change_message = message  # type: ClobMarketPriceChangeMessage
            # console.log(JSON.stringify(priceChangeMessage, null, 2))
            timestamp = int(price_change_message["payload"]["t"])
            token_changes: List[TokenPriceChange] = []
            for pc in price_change_message["payload"]["pc"]:
                slug_and_outcome = token_id_to_outcome_map.get(pc["a"])
                if slug_and_outcome:
                    slug, outcome = slug_and_outcome.split(":", 1)
                    buy_price = pc["ba"]
                    sell_price = pc["bb"]
                    side = pc["si"]
                    price = float(buy_price) if side == "BUY" else float(sell_price)

                    token_changes.append(
                        TokenPriceChange(
                            slug=slug,
                            outcome=outcome,
                            price=price,
                            side=side,
                            timestamp=timestamp,
                        )
                    )
            # Add all items to buffer instead of inserting immediately
            if token_changes:
                try:
                    asyncio.create_task(add_to_token_price_buffer(token_changes))
                except (RuntimeError, AttributeError):
                    # Fallback: insert directly if no event loop is available
                    insert_token_price_changes(token_changes)


current_client: Optional[RealTimeDataClient] = None
refresh_interval_task: Optional[asyncio.Task] = None
buffer_flush_task: Optional[asyncio.Task] = None


async def fetch_markets_and_token_ids() -> List[str]:
    """
    Fetches markets and extracts tokenIds.

    Returns:
        List of unique token IDs
    """
    print("Fetching active markets...")
    markets = get_polymarket_events(False, 8, True)
    # get only btc markets
    btc_markets = [market for market in markets if "btc" in market.slug.lower()]

    # Extract all unique tokenIds from markets
    token_ids_set = set()
    for market in btc_markets:
        if market.clob_token_ids and market.outcomes:
            try:
                clob_token_ids_arr = json.loads(market.clob_token_ids)
                outcomes_arr = json.loads(market.outcomes)

                for index, token_id in enumerate(clob_token_ids_arr):
                    token_id_to_outcome_map[token_id] = (
                        f"{market.slug}:{outcomes_arr[index]}"
                    )

                print(token_id_to_outcome_map)

                if isinstance(clob_token_ids_arr, list):
                    for token_id in clob_token_ids_arr:
                        if token_id:
                            token_ids_set.add(token_id)
            except Exception as error:
                print(f"Error parsing clobTokenIds for market {market.id}: {error}")

    token_ids = list(token_ids_set)
    print(f"Found {len(token_ids)} unique tokenIds from {len(markets)} active markets")
    return token_ids


def build_subscriptions(token_ids: List[str]) -> List[Dict[str, str]]:
    """
    Builds subscriptions array with tokenIds.

    Args:
        token_ids: List of token IDs to subscribe to

    Returns:
        List of subscription dictionaries
    """
    subscriptions = [
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": '{"symbol":"btc/usd"}',
        },
        # {
        #     "topic": "crypto_prices_chainlink",
        #     "type": "*",
        #     "filters": '{"symbol":"eth/usd"}',
        # },
    ]

    # Add clob_market subscription with all tokenIds
    if len(token_ids) > 0:
        subscriptions.append(
            {
                "topic": "clob_market",
                "type": "price_change",
                "filters": json.dumps(token_ids),
            }
        )

    return subscriptions


async def connect_websocket(subscriptions: List[Dict[str, str]]) -> RealTimeDataClient:
    """
    Connects to the WebSocket server with the given subscriptions.

    Args:
        subscriptions: List of subscription dictionaries

    Returns:
        Connected RealTimeDataClient instance
    """

    # Create onConnect callback that subscribes to all topics
    def on_connect(client: RealTimeDataClient) -> None:
        asyncio.create_task(client.subscribe({"subscriptions": subscriptions}))
        print(f"Subscribed to {len(subscriptions)} topics")

    # Connect to the server
    client = RealTimeDataClient(on_connect=on_connect, on_message=on_message)
    await client.connect()
    print("Connected to the server")
    return client


async def disconnect_current_client() -> None:
    """Gracefully disconnects the current WebSocket client."""
    global current_client
    if current_client:
        print("Disconnecting current WebSocket client...")
        await current_client.disconnect()
        current_client = None
        # Give it a moment to close gracefully
        await asyncio.sleep(1)


async def stop_buffer_flush() -> None:
    """Stop the periodic buffer flush task and flush remaining buffers."""
    global buffer_flush_task
    if buffer_flush_task:
        buffer_flush_task.cancel()
        try:
            await buffer_flush_task
        except asyncio.CancelledError:
            pass
        buffer_flush_task = None
    # Flush any remaining items in buffers
    await flush_all_buffers()
    print("All buffers flushed")


async def refresh_and_reconnect() -> None:
    """Refreshes markets and reconnects WebSocket."""
    try:
        print("\n=== Refreshing markets and reconnecting ===")

        # Disconnect current client
        await disconnect_current_client()

        # Fetch new markets and tokenIds
        token_ids = await fetch_markets_and_token_ids()

        # Build subscriptions
        subscriptions = build_subscriptions(token_ids)

        # Connect new client
        global current_client
        current_client = await connect_websocket(subscriptions)

        print("=== Refresh complete ===\n")
    except Exception as error:
        print(f"Error during refresh and reconnect: {error}")


def is_in_active_window() -> bool:
    """
    Check if current time is in the active monitoring window (12th to 18th minute).

    Returns:
        True if current minute is between 12 and 18 (inclusive)
    """
    current_minute = datetime.now().minute
    return 12 <= current_minute <= 18


async def periodic_refresh() -> None:
    """
    Periodically refreshes markets and reconnects based on time windows.
    - During minutes 12-18: Keep service running (no refresh, just monitor)
    - After minute 18: Fetch new events and refresh, then continue running
    - Before minute 12: Keep service running, wait for active window
    """
    CHECK_INTERVAL_SECONDS = 60  # Check every minute
    last_refresh_minute = -1  # Track when we last refreshed to avoid multiple refreshes

    while True:
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

        current_minute = datetime.now().minute
        current_time = datetime.now().strftime("%H:%M:%S")

        if is_in_active_window():
            # During active window (12-18), just keep monitoring
            print(
                f"[{current_time}] In active monitoring window (minute {current_minute}). Service running..."
            )
            last_refresh_minute = -1  # Reset refresh tracking
        elif current_minute > 18:
            # After minute 18, fetch new events and refresh (only once per cycle)
            if last_refresh_minute != current_minute:
                print(
                    f"[{current_time}] After active window (minute {current_minute}). Fetching new events and refreshing..."
                )
                await refresh_and_reconnect()
                last_refresh_minute = current_minute
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Refresh complete. Service continues running..."
                )
        else:
            # Before minute 12, keep service running and wait for active window
            if current_minute != last_refresh_minute:
                print(
                    f"[{current_time}] Before active window (minute {current_minute}). Service running, waiting for active window (12-18)..."
                )
                last_refresh_minute = current_minute


async def main() -> None:
    """Main entry point."""
    global current_client, refresh_interval_task, buffer_flush_task

    current_time = datetime.now().strftime("%H:%M:%S")
    current_minute = datetime.now().minute

    print(f"[{current_time}] Starting Polymarket monitoring service...")
    print(f"[{current_time}] Active monitoring window: minutes 12-18 of each hour")
    print(f"[{current_time}] Service will refresh events after minute 18 of each hour")
    print(
        f"[{current_time}] Batch insert enabled: batch size={BATCH_SIZE}, flush interval={FLUSH_INTERVAL_SECONDS}s"
    )

    # Initial connection
    token_ids = await fetch_markets_and_token_ids()
    subscriptions = build_subscriptions(token_ids)
    current_client = await connect_websocket(subscriptions)

    # If we're after minute 18, fetch new events immediately
    if current_minute > 18:
        print(
            f"[{current_time}] After active window. Fetching new events and refreshing..."
        )
        await refresh_and_reconnect()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Initial refresh complete.")
    elif is_in_active_window():
        print(
            f"[{current_time}] Currently in active monitoring window. Service running..."
        )
    else:
        print(
            f"[{current_time}] Before active window. Service will start monitoring at minute 12..."
        )

    # Set up periodic refresh task
    refresh_interval_task = asyncio.create_task(periodic_refresh())
    print("Time-based refresh scheduler started. Service will run continuously.")

    # Set up periodic buffer flush task
    buffer_flush_task = asyncio.create_task(periodic_buffer_flush())
    print("Periodic buffer flush task started.")

    # Keep the event loop running
    try:
        # Wait for the refresh task (which runs indefinitely)
        await refresh_interval_task
    except asyncio.CancelledError:
        pass
    finally:
        await stop_buffer_flush()
        if current_client:
            await disconnect_current_client()


def signal_handler(sig, frame):
    """Handle shutdown signals."""
    print("\nShutting down gracefully...")
    global refresh_interval_task, current_client, buffer_flush_task

    if refresh_interval_task:
        refresh_interval_task.cancel()

    # Schedule buffer flush and disconnect in the event loop
    loop = asyncio.get_event_loop()
    if loop.is_running():
        asyncio.create_task(stop_buffer_flush())
        asyncio.create_task(disconnect_current_client())
    else:
        loop.run_until_complete(stop_buffer_flush())
        loop.run_until_complete(disconnect_current_client())


if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    finally:
        # Flush any remaining buffers before exit
        asyncio.run(flush_all_buffers())
        if current_client:
            asyncio.run(disconnect_current_client())
