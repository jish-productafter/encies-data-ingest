from clickhouse_driver import Client

import os
import json
from datetime import datetime
from typing import Any, List, Optional
from pathlib import Path
from dotenv import load_dotenv
from src.model import TokenPriceChange, ExchangePriceChange


# Find .env file by searching from current directory up to project root
def find_env_file():
    current = Path.cwd()
    # Search up to 3 levels up
    for _ in range(3):
        env_file = current / ".env"
        if env_file.exists():
            return env_file, current
        current = current.parent
    # If not found, use current directory
    return Path.cwd() / ".env", Path.cwd()


env_path, project_root = find_env_file()

# Load .env file with explicit path
load_dotenv(dotenv_path=env_path, override=True)

# Debug: Check if .env file exists
if env_path.exists():
    print(f"✓ Found .env file at: {env_path}")
else:
    print(f"⚠ Warning: .env file not found at: {env_path}")
    print(f"   Current working directory: {Path.cwd()}")
    print(f"   Searched from: {project_root}")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))


client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
)


def insert_token_price_changes(token_price_changes: List[TokenPriceChange]) -> None:
    """
    Insert token price changes into ClickHouse.

    Args:
        token_price_changes: List of TokenPriceChange dictionaries
    """
    # Convert list of dictionaries to list of tuples in the correct order
    data = [
        (
            item["slug"],
            item["outcome"],
            item["price"],
            item["side"],
            item["timestamp"],
        )
        for item in token_price_changes
    ]
    print(data)

    data = client.execute(
        "INSERT INTO polymarket.token_price_changes (slug, outcome, price, side, timestamp) VALUES",
        data,
    )
    print(data)


# insertQuery = f"""
#  CREATE TABLE IF NOT EXISTS polymarket.exchange_price_changes
#         (
#             symbol        String,
#             timestamp     UInt64,
#             price       Float64,
#             PRIMARY KEY (symbol, timestamp)
#         )
#         ENGINE = MergeTree
#         ORDER BY (symbol, timestamp)
#         SETTINGS index_granularity = 8192;
#         """


def insert_exchange_price_changes(
    exchange_price_changes: List[ExchangePriceChange],
) -> None:
    data = [
        (item["symbol"], item["timestamp"], item["price"])
        for item in exchange_price_changes
    ]
    print(data)
    data = client.execute(
        "INSERT INTO polymarket.exchange_price_changes (symbol, timestamp, price) VALUES",
        data,
    )
    print(data)


if __name__ == "__main__":
    print(client.execute("SELECT version()"))
    # create_token_price_changes_table()
# def insert_alpha_scores():
#     result = client.execute(
#         f"""
#      INSERT INTO polymarket.trader_alpha_scores
#      SELECT * FROM polymarket.view_calc_alpha;
#      """
#     )
#     print(result)
#     return result


# def add_polymarket_markets(
#     markets: List[PolymarketMarket], batch_size: int = 1000
# ) -> None:
#     """
#     Add Polymarket markets to the database in batches.

#     Args:
#         markets: List of market dictionaries
#         batch_size: Number of markets to process per batch (default: 1000)
#     """
#     print(f"Markets to add: {markets}")

#     if len(markets) == 0:
#         print(
#             f"[{datetime.now().isoformat()}] add_polymarket_markets: No markets to add"
#         )
#         return

#     total_items = len(markets)
#     total_batches = (total_items + batch_size - 1) // batch_size
#     total_inserted = 0

#     print(
#         f"[{datetime.now().isoformat()}] add_polymarket_markets: Will process {total_batches} batch(es)"
#     )

#     for i in range(0, total_items, batch_size):
#         batch = markets[i : i + batch_size]
#         batch_data = []

#         for market in batch:
#             # Helper to ensure string values are properly formatted
#             def get_string(value: Any) -> str:
#                 if value is None:
#                     return ""
#                 return str(value)

#             # Helper to ensure number values are properly formatted
#             def get_number(value: Any) -> float:
#                 if value is None:
#                     return 0.0
#                 try:
#                     num = float(value)
#                     return num if not (num != num) else 0.0  # Check for NaN
#                 except (ValueError, TypeError):
#                     return 0.0

#             # Helper to ensure outcomePrices are numbers
#             def get_outcome_prices(value: Any) -> List[float]:
#                 if value is None:
#                     return []
#                 if not isinstance(value, list):
#                     return []
#                 result = []
#                 for v in value:
#                     try:
#                         num = float(v)
#                         result.append(num if not (num != num) else 0.0)  # Check for NaN
#                     except (ValueError, TypeError):
#                         result.append(0.0)
#                 return result

#             # Helper to ensure outcomes are strings
#             def get_outcomes(value: Any) -> List[str]:
#                 if value is None:
#                     return []
#                 if not isinstance(value, list):
#                     return []
#                 return [str(v) for v in value]

#             batch_data.append(
#                 {
#                     "marketId": get_string(get_value(market, "marketId")),
#                     "slug": get_string(get_value(market, "slug")),
#                     "question": get_string(get_value(market, "question")),
#                     "resolvedTokenId": get_string(get_value(market, "resolvedTokenId")),
#                     "resolutionSource": get_string(
#                         get_value(market, "resolutionSource")
#                     ),
#                     "startDate": normalize_date_to_datetime(
#                         get_value(market, "startDate")
#                     ),
#                     "endDate": normalize_date_to_datetime(get_value(market, "endDate")),
#                     "outcomes": get_outcomes(get_value(market, "outcomes")),
#                     "outcomePrices": get_outcome_prices(
#                         get_value(market, "outcomePrices")
#                     ),
#                     "liquidity": get_number(get_value(market, "liquidity")),
#                     "volume": get_number(get_value(market, "volume")),
#                     "createdAt": normalize_date_to_datetime(
#                         get_value(market, "createdAt")
#                     ),
#                     "closedAt": normalize_date_to_datetime(
#                         get_value(market, "closedAt")
#                     ),
#                 }
#             )

#         # Validate the batch data before inserting
#         batch_num = i // batch_size + 1
#         print(
#             f"[{datetime.now().isoformat()}] add_polymarket_markets: Validating batch {batch_num}/{total_batches}..."
#         )

#         try:
#             # Test JSON serialization to catch any issues early
#             # Convert datetime objects to strings for JSON serialization
#             json_data = []
#             for item in batch_data:
#                 json_item = item.copy()
#                 for date_key in ["startDate", "endDate", "createdAt", "closedAt"]:
#                     if isinstance(json_item[date_key], datetime):
#                         json_item[date_key] = json_item[date_key].isoformat()
#                 json_data.append(json_item)
#             json.dumps(json_data)
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: Batch {batch_num} validation passed"
#             )
#         except Exception as error:
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: Error serializing batch {batch_num} data: {error}"
#             )
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: Problematic batch: {batch_data[:5]}"
#             )
#             raise

#         # Execute batch insert with concurrency control
#         print(
#             f"[{datetime.now().isoformat()}] add_polymarket_markets: Inserting batch {batch_num} into database..."
#         )

#         try:
#             # Convert batch_data to tuples for ClickHouse insert
#             # Assuming the table structure matches the fields in batch_data
#             insert_query = """
#                 INSERT INTO polymarket.markets_closed (
#                     marketId, slug, question, resolvedTokenId, resolutionSource,
#                     startDate, endDate, outcomes, outcomePrices, liquidity, volume,
#                     createdAt, closedAt
#                 ) VALUES
#             """

#             # Prepare data as tuples
#             values = [
#                 (
#                     item["marketId"],
#                     item["slug"],
#                     item["question"],
#                     item["resolvedTokenId"],
#                     item["resolutionSource"],
#                     item["startDate"],
#                     item["endDate"],
#                     item["outcomes"],  # Array type
#                     item["outcomePrices"],  # Array type
#                     item["liquidity"],
#                     item["volume"],
#                     item["createdAt"],
#                     item["closedAt"],
#                 )
#                 for item in batch_data
#             ]

#             client.execute(insert_query, values)
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: Successfully inserted batch {batch_num}"
#             )
#             total_inserted += len(batch)
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: Batch {batch_num}/{total_batches}: "
#                 f"Added {len(batch)} markets (Total: {total_inserted}/{total_items})"
#             )
#         except Exception as error:
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: Error inserting batch {batch_num}: {error}"
#             )
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: Batch size: {len(batch_data)}"
#             )
#             # Convert datetime objects to strings for JSON serialization
#             first_row = batch_data[0] if batch_data else {}
#             json_row = first_row.copy()
#             for date_key in ["startDate", "endDate", "createdAt", "closedAt"]:
#                 if isinstance(json_row.get(date_key), datetime):
#                     json_row[date_key] = json_row[date_key].isoformat()
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: First row: {json.dumps(json_row, indent=2)}"
#             )
#             # Try inserting in smaller batches (10 rows) instead of one at a time
#             small_batch_size = 10
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_markets: Attempting to insert in smaller batches of {small_batch_size}..."
#             )
#             for j in range(0, len(batch_data), small_batch_size):
#                 small_batch = batch_data[j : j + small_batch_size]
#                 try:
#                     small_values = [
#                         (
#                             item["marketId"],
#                             item["slug"],
#                             item["question"],
#                             item["resolvedTokenId"],
#                             item["resolutionSource"],
#                             item["startDate"],
#                             item["endDate"],
#                             item["outcomes"],
#                             item["outcomePrices"],
#                             item["liquidity"],
#                             item["volume"],
#                             item["createdAt"],
#                             item["closedAt"],
#                         )
#                         for item in small_batch
#                     ]
#                     client.execute(insert_query, small_values)
#                 except Exception as row_error:
#                     print(
#                         f"[{datetime.now().isoformat()}] add_polymarket_markets: Error inserting small batch "
#                         f"{j // small_batch_size + 1} in batch {batch_num}: {row_error}"
#                     )
#                     # Convert datetime objects to strings for JSON serialization
#                     json_small_batch = []
#                     for item in small_batch:
#                         json_item = item.copy()
#                         for date_key in [
#                             "startDate",
#                             "endDate",
#                             "createdAt",
#                             "closedAt",
#                         ]:
#                             if isinstance(json_item[date_key], datetime):
#                                 json_item[date_key] = json_item[date_key].isoformat()
#                         json_small_batch.append(json_item)
#                     print(
#                         f"[{datetime.now().isoformat()}] add_polymarket_markets: Problematic batch data: "
#                         f"{json.dumps(json_small_batch, indent=2)}"
#                     )
#                     # Skip problematic rows and continue
#                     print(
#                         f"[{datetime.now().isoformat()}] add_polymarket_markets: Skipping problematic small batch and continuing..."
#                     )
#             # Don't throw - continue with other batches

#     print(
#         f"[{datetime.now().isoformat()}] add_polymarket_markets: Successfully added {total_inserted} markets to the database "
#         f"in {total_batches} batch(es)"
#     )


# def get_newest_started_market() -> Optional[datetime]:
#     result = client.execute(
#         f"""
#         SELECT startDate FROM polymarket.markets_closed ORDER BY startDate DESC LIMIT 1
#         """
#     )
#     try:
#         if result and len(result) > 0 and len(result[0]) > 0:
#             if type(result[0][0]) == datetime:
#                 print(
#                     f"[{datetime.now().isoformat()}] get_newest_started_market: Result: {result[0][0].isoformat()}"
#                 )
#                 return result[0][0]
#             else:
#                 return None
#         else:
#             return None
#     except Exception as e:
#         # Optionally log or print the error, but do not throw
#         print(f"[{datetime.now().isoformat()}] get_newest_started_market: Error: {e}")
#         return None


# def get_market_ids_to_ingest(limit: int = 10) -> List[str]:
#     result = client.execute(
#         f"""
#         SELECT marketId FROM polymarket.markets_closed WHERE marketId NOT IN (SELECT DISTINCT marketId FROM polymarket.trades) ORDER BY startDate DESC LIMIT {limit}
#         """
#     )
#     try:
#         if result and len(result) > 0 and len(result[0]) > 0:
#             return [item[0] for item in result]
#         else:
#             return []
#     except Exception as e:
#         print(f"[{datetime.now().isoformat()}] get_market_ids_to_ingest: Error: {e}")
#         return []


# def add_polymarket_trades(
#     trades: List[PolymarketTrade], batch_size: int = 1000
# ) -> None:
#     """
#     Add Polymarket trades to the database in batches.

#     Args:
#         trades: List of trade dictionaries
#         batch_size: Number of trades to process per batch (default: 1000)
#     """
#     if len(trades) == 0:
#         print(f"[{datetime.now().isoformat()}] add_polymarket_trades: No trades to add")
#         return

#     total_items = len(trades)
#     total_batches = (total_items + batch_size - 1) // batch_size
#     total_inserted = 0

#     print(
#         f"[{datetime.now().isoformat()}] add_polymarket_trades: Will process {total_batches} batch(es)"
#     )

#     for i in range(0, total_items, batch_size):
#         batch = trades[i : i + batch_size]
#         batch_data = []

#         for trade in batch:
#             # Helper to ensure string values are properly formatted
#             def get_string(value: Any) -> str:
#                 if value is None:
#                     return ""
#                 return str(value)

#             # Helper to ensure number values are properly formatted
#             def get_number(value: Any) -> float:
#                 if value is None:
#                     return 0.0
#                 try:
#                     num = float(value)
#                     return num if not (num != num) else 0.0  # Check for NaN
#                 except (ValueError, TypeError):
#                     return 0.0

#             # Helper to ensure integer values are properly formatted
#             def get_integer(value: Any) -> int:
#                 if value is None:
#                     return 0
#                 try:
#                     return int(value)
#                 except (ValueError, TypeError):
#                     return 0

#             # Normalize timestamp to datetime
#             timestamp_value = get_value(trade, "timestamp")
#             timestamp_datetime = normalize_date_to_datetime(timestamp_value)

#             batch_data.append(
#                 {
#                     "transactionHash": get_string(get_value(trade, "transactionHash")),
#                     "proxyWallet": get_string(get_value(trade, "proxyWallet")),
#                     "timestamp": timestamp_datetime,
#                     "marketId": get_string(get_value(trade, "marketId")),
#                     "size": get_number(get_value(trade, "size")),
#                     "price": get_number(get_value(trade, "price")),
#                     "tokenId": get_string(get_value(trade, "tokenId")),
#                     "side": get_string(get_value(trade, "side")),
#                     "outcomeIndex": get_integer(get_value(trade, "outcomeIndex")),
#                     "title": get_string(get_value(trade, "title")),
#                     "slug": get_string(get_value(trade, "slug")),
#                     "icon": get_string(get_value(trade, "icon")),
#                     "eventSlug": get_string(get_value(trade, "eventSlug")),
#                     "outcome": get_string(get_value(trade, "outcome")),
#                     "name": get_string(get_value(trade, "name")),
#                     "pseudonym": get_string(get_value(trade, "pseudonym")),
#                     "bio": get_string(get_value(trade, "bio")),
#                     "profileImage": get_string(get_value(trade, "profileImage")),
#                     "profileImageOptimized": get_string(
#                         get_value(trade, "profileImageOptimized")
#                     ),
#                 }
#             )

#         # Validate the batch data before inserting
#         batch_num = i // batch_size + 1
#         print(
#             f"[{datetime.now().isoformat()}] add_polymarket_trades: Validating batch {batch_num}/{total_batches}..."
#         )

#         try:
#             # Test JSON serialization to catch any issues early
#             # Convert datetime objects to strings for JSON serialization
#             json_data = []
#             for item in batch_data:
#                 json_item = item.copy()
#                 if isinstance(json_item["timestamp"], datetime):
#                     json_item["timestamp"] = json_item["timestamp"].isoformat()
#                 json_data.append(json_item)
#             json.dumps(json_data)
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: Batch {batch_num} validation passed"
#             )
#         except Exception as error:
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: Error serializing batch {batch_num} data: {error}"
#             )
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: Problematic batch: {batch_data[:5]}"
#             )
#             raise

#         # Execute batch insert with concurrency control
#         print(
#             f"[{datetime.now().isoformat()}] add_polymarket_trades: Inserting batch {batch_num} into database..."
#         )

#         try:
#             # Convert batch_data to tuples for ClickHouse insert
#             insert_query = """
#                 INSERT INTO polymarket.trades (
#                     transactionHash, proxyWallet, timestamp, marketId, size, price,
#                     tokenId, side, outcomeIndex, title, slug, icon, eventSlug,
#                     outcome, name, pseudonym, bio, profileImage, profileImageOptimized
#                 ) VALUES
#             """

#             # Prepare data as tuples
#             values = [
#                 (
#                     item["transactionHash"],
#                     item["proxyWallet"],
#                     item["timestamp"],
#                     item["marketId"],
#                     item["size"],
#                     item["price"],
#                     item["tokenId"],
#                     item["side"],
#                     item["outcomeIndex"],
#                     item["title"],
#                     item["slug"],
#                     item["icon"],
#                     item["eventSlug"],
#                     item["outcome"],
#                     item["name"],
#                     item["pseudonym"],
#                     item["bio"],
#                     item["profileImage"],
#                     item["profileImageOptimized"],
#                 )
#                 for item in batch_data
#             ]

#             client.execute(insert_query, values)
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: Successfully inserted batch {batch_num}"
#             )
#             total_inserted += len(batch)
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: Batch {batch_num}/{total_batches}: "
#                 f"Added {len(batch)} trades (Total: {total_inserted}/{total_items})"
#             )
#             # Calculate alpha scores after successful batch insert
#             insert_alpha_scores()
#         except Exception as error:
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: Error inserting batch {batch_num}: {error}"
#             )
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: Batch size: {len(batch_data)}"
#             )
#             # Convert datetime objects to strings for JSON serialization
#             first_row = batch_data[0] if batch_data else {}
#             json_row = first_row.copy()
#             if isinstance(json_row.get("timestamp"), datetime):
#                 json_row["timestamp"] = json_row["timestamp"].isoformat()
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: First row: {json.dumps(json_row, indent=2)}"
#             )
#             # Try inserting in smaller batches (10 rows) instead of one at a time
#             small_batch_size = 10
#             print(
#                 f"[{datetime.now().isoformat()}] add_polymarket_trades: Attempting to insert in smaller batches of {small_batch_size}..."
#             )
#             for j in range(0, len(batch_data), small_batch_size):
#                 small_batch = batch_data[j : j + small_batch_size]
#                 try:
#                     small_values = [
#                         (
#                             item["transactionHash"],
#                             item["proxyWallet"],
#                             item["timestamp"],
#                             item["marketId"],
#                             item["size"],
#                             item["price"],
#                             item["tokenId"],
#                             item["side"],
#                             item["outcomeIndex"],
#                             item["title"],
#                             item["slug"],
#                             item["icon"],
#                             item["eventSlug"],
#                             item["outcome"],
#                             item["name"],
#                             item["pseudonym"],
#                             item["bio"],
#                             item["profileImage"],
#                             item["profileImageOptimized"],
#                         )
#                         for item in small_batch
#                     ]
#                     client.execute(insert_query, small_values)
#                 except Exception as row_error:
#                     print(
#                         f"[{datetime.now().isoformat()}] add_polymarket_trades: Error inserting small batch "
#                         f"{j // small_batch_size + 1} in batch {batch_num}: {row_error}"
#                     )
#                     # Convert datetime objects to strings for JSON serialization
#                     json_small_batch = []
#                     for item in small_batch:
#                         json_item = item.copy()
#                         if isinstance(json_item["timestamp"], datetime):
#                             json_item["timestamp"] = json_item["timestamp"].isoformat()
#                         json_small_batch.append(json_item)
#                     print(
#                         f"[{datetime.now().isoformat()}] add_polymarket_trades: Problematic batch data: "
#                         f"{json.dumps(json_small_batch, indent=2)}"
#                     )
#                     # Skip problematic rows and continue
#                     print(
#                         f"[{datetime.now().isoformat()}] add_polymarket_trades: Skipping problematic small batch and continuing..."
#                     )
#             # Don't throw - continue with other batches

#     print(
#         f"[{datetime.now().isoformat()}] add_polymarket_trades: Successfully added {total_inserted} trades to the database "
#         f"in {total_batches} batch(es)"
#     )


# def backfill_from_current_trades(last_market_id: str) -> None:
#     query = f"""
#     INSERT INTO polymarket.trades
#     (
#         transactionHash,
#         proxyWallet,
#         timestamp,
#         marketId,
#         size,
#         price,
#         tokenId,
#         side,
#         outcomeIndex,
#         title,
#         slug,
#         icon,
#         eventSlug,
#         outcome,
#         name,
#         pseudonym,
#         bio,
#         profileImage,
#         profileImageOptimized
#     )

#     SELECT
#         transactionHash,
#         proxyWallet,
#         timestamp,
#         conditionId AS marketId,
#         size,
#         price,
#         asset AS tokenId,
#         side,
#         outcomeIndex,
#         title,
#         slug,
#         icon,
#         eventSlug,
#         outcome,
#         name,
#         pseudonym,
#         bio,
#         profileImage,
#         profileImage AS profileImageOptimized   -- or transform if needed
#     FROM polymarket.current_trades
#     where conditionId = '{last_market_id}' AND
#         transactionHash NOT IN
#             (select transactionHash from polymarket.trades where marketId = '{last_market_id}')
#     """
#     try:
#         print(
#             f"[{datetime.now().isoformat()}] backfill_from_current_trades: Executing query: {query}"
#         )
#         insert_response = client.execute(query)
#         print(
#             f"[{datetime.now().isoformat()}] backfill_from_current_trades: Insert response: {insert_response}"
#         )
#         delete_current_trades_for_market(last_market_id)
#     except Exception as e:
#         print(
#             f"[{datetime.now().isoformat()}] backfill_from_current_trades: Error: {e}"
#         )
#         raise


# def delete_current_trades_for_market(market_id: str) -> None:
#     query = f"""
#     DELETE FROM polymarket.current_trades
#     WHERE conditionId = '{market_id}'
#     """
#     try:
#         print(
#             f"[{datetime.now().isoformat()}] delete_current_trades: Executing query: {query}"
#         )
#         delete_response = client.execute(query)
#         print(
#             f"[{datetime.now().isoformat()}] delete_current_trades: Delete response: {delete_response}"
#         )
#     except Exception as e:
#         print(f"[{datetime.now().isoformat()}] delete_current_trades: Error: {e}")
#         raise


# def delele_old_trades():
#     client.execute(
#         """
#     DELETE FROM polymarket.trades WHERE marketId IN (
#     SELECT marketId
#     FROM polymarket.markets_closed
#     WHERE startDate <= now() - INTERVAL 3 DAY
#     )
#     """
#     )


# def delete_old_markets():
#     client.execute(
#         """
# DELETE FROM polymarket.markets_closed WHERE startDate <= now() - INTERVAL 3 DAY
# """
#     )
