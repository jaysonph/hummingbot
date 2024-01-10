from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

REST_URL = "https://api.bybit.com"
HEALTH_CHECK_ENDPOINT = "/v5/market/time"
CANDLES_ENDPOINT = "/v5/market/kline"

WSS_URL = "wss://stream.bybit.com/v5/public/linear"

INTERVALS = bidict({
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1M": "M"
})

IP_LIMIT = "IP_LIMIT"

RATE_LIMITS = [
    RateLimit(IP_LIMIT, limit=120, time_interval=5),
    RateLimit(CANDLES_ENDPOINT, limit=120, time_interval=5, linked_limits=[LinkedLimitWeightPair(IP_LIMIT)]),
    RateLimit(HEALTH_CHECK_ENDPOINT, limit=120, time_interval=5, linked_limits=[LinkedLimitWeightPair(IP_LIMIT)]),
]
