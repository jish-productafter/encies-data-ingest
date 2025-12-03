"""
API client for fetching Polymarket events from gamma-api.polymarket.com
"""
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests


class Market:
    """Represents a Polymarket market/event from gamma-api.polymarket.com"""
    
    def __init__(self, data: Dict[str, Any]):
        """Initialize Market from API response data."""
        self.id = data.get("id", "")
        self.question = data.get("question", "")
        self.condition_id = data.get("conditionId", "")
        self.slug = data.get("slug", "")
        self.resolution_source = data.get("resolutionSource", "")
        self.end_date = data.get("endDate", "")
        self.start_date = data.get("startDate", "")
        self.image = data.get("image", "")
        self.icon = data.get("icon", "")
        self.description = data.get("description", "")
        self.outcomes = data.get("outcomes", "")  # JSON string
        self.outcome_prices = data.get("outcomePrices", "")  # JSON string
        self.volume = data.get("volume")
        self.liquidity = data.get("liquidity")
        self.active = data.get("active", False)
        self.closed = data.get("closed", False)
        self.market_maker_address = data.get("marketMakerAddress", "")
        self.created_at = data.get("createdAt", "")
        self.updated_at = data.get("updatedAt", "")
        self.closed_time = data.get("closedTime")
        self.new = data.get("new", False)
        self.featured = data.get("featured", False)
        self.archived = data.get("archived", False)
        self.restricted = data.get("restricted", False)
        self.group_item_threshold = data.get("groupItemThreshold", "")
        self.question_id = data.get("questionID", "")
        self.uma_end_date = data.get("umaEndDate")
        self.enable_order_book = data.get("enableOrderBook", False)
        self.order_price_min_tick_size = data.get("orderPriceMinTickSize", 0)
        self.order_min_size = data.get("orderMinSize", 0)
        self.uma_resolution_status = data.get("umaResolutionStatus", "")
        self.volume_num = data.get("volumeNum")
        self.end_date_iso = data.get("endDateIso", "")
        self.start_date_iso = data.get("startDateIso", "")
        self.has_reviewed_dates = data.get("hasReviewedDates", False)
        self.volume_1wk = data.get("volume1wk")
        self.volume_1mo = data.get("volume1mo")
        self.volume_1yr = data.get("volume1yr")
        self.clob_token_ids = data.get("clobTokenIds", "")  # JSON string
        self.volume_1wk_clob = data.get("volume1wkClob")
        self.volume_1mo_clob = data.get("volume1moClob")
        self.volume_1yr_clob = data.get("volume1yrClob")
        self.volume_clob = data.get("volumeClob")
        self.accepting_orders = data.get("acceptingOrders", False)
        self.neg_risk = data.get("negRisk", False)
        self.events = data.get("events", [])
        self.ready = data.get("ready", False)
        self.funded = data.get("funded", False)
        self.accepting_orders_timestamp = data.get("acceptingOrdersTimestamp")
        self.cyom = data.get("cyom", False)
        self.pager_duty_notification_enabled = data.get("pagerDutyNotificationEnabled", False)
        self.approved = data.get("approved", False)
        self.rewards_min_size = data.get("rewardsMinSize", 0)
        self.rewards_max_spread = data.get("rewardsMaxSpread", 0)
        self.spread = data.get("spread", 0)
        self.automatically_resolved = data.get("automaticallyResolved", False)
        self.last_trade_price = data.get("lastTradePrice")
        self.best_bid = data.get("bestBid")
        self.best_ask = data.get("bestAsk")
        self.automatically_active = data.get("automaticallyActive", False)
        self.clear_book_on_start = data.get("clearBookOnStart", False)
        self.show_gmp_series = data.get("showGmpSeries", False)
        self.show_gmp_outcome = data.get("showGmpOutcome", False)
        self.manual_activation = data.get("manualActivation", False)
        self.neg_risk_other = data.get("negRiskOther", False)
        self.uma_resolution_statuses = data.get("umaResolutionStatuses", "")  # JSON string
        self.pending_deployment = data.get("pendingDeployment", False)
        self.deploying = data.get("deploying", False)
        self.rfq_enabled = data.get("rfqEnabled", False)
        self.event_start_time = data.get("eventStartTime")
        self.holding_rewards_enabled = data.get("holdingRewardsEnabled", False)
        self.fees_enabled = data.get("feesEnabled", False)


def format_date_for_api(date: str) -> str:
    """
    Formats a date string for the Polymarket API.
    
    Args:
        date: Date string to format
        
    Returns:
        Formatted date string in ISO format
    """
    date_obj = datetime.fromisoformat(date.replace('Z', '+00:00'))
    return date_obj.isoformat()


class RateLimiter:
    """Rate limiter to control the number of requests within a time window."""
    
    def __init__(self, max_requests: int = 5, window_ms: int = 10000):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed in the window
            window_ms: Time window in milliseconds
        """
        self.request_timestamps: List[float] = []
        self.max_requests = max_requests
        self.window_ms = window_ms
    
    def wait_for_slot(self) -> None:
        """Waits for an available slot in the rate limit window."""
        now = time.time() * 1000  # Current time in milliseconds
        window_start = now - self.window_ms
        
        # Remove timestamps outside the current window
        self.request_timestamps = [
            timestamp for timestamp in self.request_timestamps
            if timestamp > window_start
        ]
        
        # If we're at the limit, wait until the oldest request expires
        if len(self.request_timestamps) >= self.max_requests:
            oldest_timestamp = self.request_timestamps[0]
            wait_time = oldest_timestamp + self.window_ms - now + 1  # +1ms buffer
            
            if wait_time > 0:
                timestamp = datetime.now().isoformat()
                print(f"[{timestamp}] RateLimiter: Rate limit reached, waiting {wait_time}ms")
                time.sleep(wait_time / 1000)  # Convert ms to seconds
                # Recursively check again after waiting
                return self.wait_for_slot()
        
        # Add current request timestamp
        self.request_timestamps.append(time.time() * 1000)


# Singleton rate limiter instance
_rate_limiter = RateLimiter()


def rate_limited_fetch(url: str) -> requests.Response:
    """
    Rate-limited fetch wrapper to prevent API rate limit issues.
    
    Args:
        url: URL to fetch
        
    Returns:
        Response object
    """
    _rate_limiter.wait_for_slot()
    return requests.get(url)


def get_polymarket_events(
    closed: bool = True,
    data_limit: int = 1000,
    ascending: bool = False
) -> List[Market]:
    """
    Fetches Polymarket events from the API.
    
    Args:
        closed: Whether to fetch closed markets (default: True)
        data_limit: Maximum number of events to fetch (default: 1000)
        ascending: Whether to sort in ascending order (default: False)
        
    Returns:
        List of Market objects
    """
    timestamp = datetime.now().isoformat()
    print(f"[{timestamp}] getPolymarketEvents: Starting (closed={closed})")
    
    url = f"https://gamma-api.polymarket.com/markets?tag_id=102467&closed={closed}&order=id&limit={data_limit}&ascending={ascending}"
    
    print(f"[{timestamp}] getPolymarketEvents: Fetching from {url}")
    
    try:
        response = rate_limited_fetch(url)
        
        if response.status_code != 200:
            error_timestamp = datetime.now().isoformat()
            print(f"[{error_timestamp}] getPolymarketEvents: HTTP error! status: {response.status_code}")
            raise Exception(f"HTTP error! status: {response.status_code}")
        
        data = response.json()
        success_timestamp = datetime.now().isoformat()
        data_length = len(data) if isinstance(data, list) else "unknown"
        print(f"[{success_timestamp}] getPolymarketEvents: Successfully fetched {data_length} events")
        
        return [Market(market_data) for market_data in data]
    except Exception as error:
        error_timestamp = datetime.now().isoformat()
        print(f"[{error_timestamp}] getPolymarketEvents: Error fetching events: {error}")
        raise

