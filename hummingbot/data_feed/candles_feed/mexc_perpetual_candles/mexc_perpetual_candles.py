import asyncio
import logging
from typing import Any, Dict, Optional

import numpy as np

from hummingbot.core.network_iterator import NetworkStatus, safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.data_feed.candles_feed.mexc_perpetual_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.logger import HummingbotLogger

import math


class MexcPerpetualCandles(CandlesBase):
    _logger: Optional[HummingbotLogger] = None
    _vol_scale_factor: float = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        super().__init__(trading_pair, interval, max_records)

    @property
    def name(self):
        return f"mexc_perpetual_{self._trading_pair}"

    @property
    def rest_url(self):
        return CONSTANTS.REST_URL

    @property
    def wss_url(self):
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self):
        return self.rest_url + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self):
        return self.rest_url + CONSTANTS.CANDLES_ENDPOINT

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS
    
    @property
    def vol_scale_factor(self):
        return self._vol_scale_factor

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(url=self.health_check_url,
                                             throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT)
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair):
        return trading_pair.replace("-", "_")
    
    def _get_vol_scale(self, open_price, amount, volume):
        '''
        Volume from MEXC futures api is not in token unit. (e.g. 10000 meaning 1 BTC for BTC_USDT, 100 meaning 1 ETH for ETH_USDT)
        Calculate the scaling factor for volume, the volume over this factor will be equal to the correct amount in token unit
        - args:
            - open_price (float): open price
            - amount (float): quote asset volume, volume expressed in quote asset value (e.g. USDT for BTC/USDT)
            - volume (float): volume in unit of the asset (e.g. BTC for BTC/USDT)
        '''
        open_price_mag = math.floor(math.log(open_price, 10))
        qs_price_mag = math.floor(math.log(amount/volume, 10))
        self._vol_scale_factor = 10**(open_price_mag-qs_price_mag)

    async def fetch_candles(self,
                            start_time: Optional[int] = None,
                            end_time: Optional[int] = None,
                            limit: Optional[int] = 2000):
        rest_assistant = await self._api_factory.get_rest_assistant()
        params = {"interval": self.intervals[self.interval], "limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        candles = await rest_assistant.execute_request(url=self.candles_url + f"/{self._ex_trading_pair}",
                                                       throttler_limit_id=CONSTANTS.CANDLES_ENDPOINT,
                                                       params=params)
        
        data = candles['data']

        if self.vol_scale_factor is None:
            self._get_vol_scale(data["open"][0], data["amount"][0], data["vol"][0])
        
        # Convert timestamp to millisecond timestamp
        data['time'] = [t*1000 for t in data['time']]

        # Convert volume to real number of BTC
        data['vol'] = [v/self.vol_scale_factor for v in data['vol']]

        data_arr = np.array(list(zip(*[data['time'], data['open'], data['high'], data['low'], data['close'], data['vol'], data['amount']])))

        return data_arr.astype(float)

    async def fill_historical_candles(self):
        max_request_needed = (self._candles.maxlen // 2000) + 1
        requests_executed = 0
        while not self.is_ready:
            missing_records = self._candles.maxlen - len(self._candles)
            end_timestamp = int(self._candles[0][0]/1000)
            try:
                if requests_executed < max_request_needed:
                    # we have to add one more since, the last row is not going to be included
                    candles = await self.fetch_candles(end_time=end_timestamp, limit=min(2000, missing_records + 1))
                    # we are computing again the quantity of records again since the websocket process is able to
                    # modify the deque and if we extend it, the new observations are going to be dropped.
                    missing_records = self._candles.maxlen - len(self._candles)
                    self._candles.extendleft(candles[-(missing_records + 1):-1][::-1])
                    requests_executed += 1
                else:
                    self.logger().error(f"There is no data available for the quantity of "
                                        f"candles requested for {self.name}.")
                    raise
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when getting historical klines. Retrying in 1 seconds...",
                )
                await self._sleep(1.0)

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the candles events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            candle_params = {"symbol": self._ex_trading_pair, "interval": self.intervals[self.interval]}
            payload = {
                "method": "sub.kline",
                "param": candle_params,
                "id": 1
            }
            subscribe_candles_request: WSJSONRequest = WSJSONRequest(payload=payload)

            await ws.send(subscribe_candles_request)
            self.logger().info("Subscribed to public klines...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to public klines...",
                exc_info=True
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data: Dict[str, Any] = ws_response.data
            if data is not None and data.get("channel") == "push.kline":  # data will be None when the websocket is disconnected
                timestamp = data["data"]["t"] * 1000  # convert to millisecond
                open = data["data"]["o"]
                low = data["data"]["l"]
                high = data["data"]["h"]
                close = data["data"]["c"]
                volume = data["data"]["q"]
                quote_asset_volume = data["data"]["a"]
                n_trades = None
                taker_buy_base_volume = None
                taker_buy_quote_volume = None

                # Rescale volume to the correct token unit
                if self.vol_scale_factor is None:
                    await self.fetch_candles(limit=1)
                volume = volume / self.vol_scale_factor

                if len(self._candles) == 0:
                    self._candles.append(np.array([timestamp, open, high, low, close, volume,
                                                   quote_asset_volume, n_trades, taker_buy_base_volume,
                                                   taker_buy_quote_volume]))
                    safe_ensure_future(self.fill_historical_candles())
                elif timestamp > int(self._candles[-1][0]):
                    # TODO: validate also that the diff of timestamp == interval (issue with 1M interval).
                    self._candles.append(np.array([timestamp, open, high, low, close, volume,
                                                   quote_asset_volume, n_trades, taker_buy_base_volume,
                                                   taker_buy_quote_volume]))
                elif timestamp == int(self._candles[-1][0]):
                    self._candles.pop()
                    self._candles.append(np.array([timestamp, open, high, low, close, volume,
                                                   quote_asset_volume, n_trades, taker_buy_base_volume,
                                                   taker_buy_quote_volume]))
