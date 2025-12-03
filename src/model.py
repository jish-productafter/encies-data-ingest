"""
Type definitions for Polymarket real-time data client.
"""

from dataclasses import dataclass
from typing import TypedDict, Optional, List, Dict, Any
from enum import Enum


@dataclass
class ClobApiKeyCreds:
    """API key credentials for CLOB authentication."""

    key: str
    secret: str
    passphrase: str


@dataclass
class GammaAuth:
    """Authentication details for Gamma authentication."""

    address: str


class SubscriptionItem(TypedDict, total=False):
    """Single subscription item in a subscription message."""

    topic: str
    type: str
    filters: Optional[str]
    clob_auth: Optional[ClobApiKeyCreds]
    gamma_auth: Optional[GammaAuth]


class SubscriptionMessage(TypedDict):
    """Message structure for subscription requests."""

    subscriptions: List[SubscriptionItem]


class MessagePayload(TypedDict):
    """Base payload structure for messages."""

    pass


class Message(TypedDict):
    """Represents a real-time message received from the WebSocket server."""

    topic: str
    type: str
    timestamp: int
    payload: Dict[str, Any]
    connection_id: str


class ConnectionStatus(Enum):
    """Represents websocket connection status."""

    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"


class CryptoPricesSubscribePayload(TypedDict):
    """Payload for crypto prices subscribe message."""

    data: List[Dict[str, float]]  # List of {timestamp: number, value: number}
    symbol: str


class CryptoPricesSubscribeMessage(TypedDict):
    """Crypto prices subscribe message."""

    payload: CryptoPricesSubscribePayload
    timestamp: int
    topic: str
    type: str


class CryptoPricesChainlinkUpdatePayload(TypedDict):
    """Payload for crypto prices chainlink update message."""

    full_accuracy_value: str
    symbol: str
    timestamp: int
    value: float


class CryptoPricesChainlinkUpdateMessage(TypedDict):
    """Crypto prices chainlink update message."""

    connection_id: str
    payload: CryptoPricesChainlinkUpdatePayload
    timestamp: int
    topic: str
    type: str


class ClobMarketPriceChangeItem(TypedDict):
    """Single price change item in CLOB market price change message."""

    a: str  # tokenId
    ba: str  # buy price
    bb: str  # sell price
    h: str
    p: str
    s: str
    si: str  # side


class ClobMarketPriceChangePayload(TypedDict):
    """Payload for CLOB market price change message."""

    m: str
    pc: List[ClobMarketPriceChangeItem]
    t: str  # timestamp


class ClobMarketPriceChangeMessage(TypedDict):
    """CLOB market price change message."""

    connection_id: str
    payload: ClobMarketPriceChangePayload
    timestamp: int
    topic: str
    type: str


class TokenPriceChange(TypedDict):
    """Token price change."""

    slug: str
    outcome: str
    price: float
    side: str
    timestamp: int


class ExchangePriceChange(TypedDict):
    """Exchange price change."""

    symbol: str
    timestamp: int
    price: float