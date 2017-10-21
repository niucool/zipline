#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
from collections import namedtuple, defaultdict
from time import sleep
from math import fabs

from six import itervalues
import pandas as pd
import numpy as np

import datetime

from zipline.gens.feeders.feeder import Feeder
import zipline.protocol as zp
from zipline.api import symbol as symbol_lookup
from zipline.errors import SymbolNotFound
from logbook import Logger

from alpha_vantage.timeseries import TimeSeries

if sys.version_info > (3,):
    long = int

log = Logger('AV Feeder')


class AVFeeder(Feeder):
    def __init__(self, av_key):
        self._av_key = av_key

        self.currency = 'USD'

        self._subscribed_assets = []
        self._assets_cache = {}

        super(self.__class__, self).__init__()

    def av_get_live_data(self, symbol, frequency='1m'):
        #max_retries = 3
        ts = TimeSeries(key=self._av_key, output_format='pandas')
        output_size = 'compact'
        if frequency == '1d':
            intraday = False
        else:
            intraday = True

        # ts has a built-in retry
        # for i in range(max_retries):
        if True:
            try:
                if intraday:
                    his_data, meta_data = ts.get_intraday(
                        symbol=symbol, interval='1min', outputsize=output_size)
                else:
                    his_data, meta_data = ts.get_daily(
                        symbol=symbol, outputsize=output_size)
                his_data.index = pd.to_datetime(
                    his_data.index).tz_localize('UTC')
                return his_data
            except Exception, e:
                log.warning(('AV Exception(%s): %s' % (symbol, str(e), )))
                # time.sleep(2)
                pass

        return None

    @property
    def subscribed_assets(self):
        return self._subscribed_assets

    def subscribe_to_market_data(self, asset, data_frequency='1m'):
        symbol = str(asset.symbol)
        time_now = datetime.datetime.now()
        # add asset to subscribe assets
        if asset not in self.subscribed_assets:
            self._subscribed_assets.append(asset)

        if symbol in self._assets_cache and data_frequency in self._assets_cache[symbol]:
            df = self._assets_cache[symbol][data_frequency]
            if df['last_update'] and (
                (time_now - df['last_update']).total_seconds() < 55) and (
                    df['data'] is not None):
                return

        if symbol not in self._assets_cache:
            self._assets_cache[symbol] = {}
        if data_frequency not in self._assets_cache[symbol]:
            self._assets_cache[symbol][data_frequency] = {
                'last_update': None, 'data': None}
        live_data = self.av_get_live_data(symbol, data_frequency)
        if live_data is not None:
            self._assets_cache[symbol][data_frequency]['data'] = live_data
            self._assets_cache[symbol][data_frequency]['last_update'] = time_now
        else:
            # use old data instead or set to None?
            # self._assets_cache[symbol][data_frequency]['data'] = None
            pass

    def get_spot_value(self, assets, field, dt, data_frequency):
        symbol = str(assets.symbol)
        self.subscribe_to_market_data(assets, data_frequency)

        # if field == 'price':
        #     field = 'close'

        bars = self._assets_cache[symbol][data_frequency]['data']
        if bars is None:
            return np.NaN

        last_event_time = bars.index[-1]

        minute_start = (last_event_time - pd.Timedelta('1 min')) \
            .time()
        minute_end = last_event_time.time()

        if bars.empty:
            return pd.NaT if field == 'last_traded' else np.NaN
        else:
            if field == 'price':
                return bars.close.iloc[-1]
            elif field == 'last_traded':
                return last_event_time or pd.NaT

            minute_df = bars.between_time(minute_start, minute_end,
                                          include_start=True, include_end=True)
            if minute_df.empty:
                return np.NaN
            else:
                return minute_df[field].iloc[-1]

    def get_last_traded_dt(self, asset):
        self.subscribe_to_market_data(asset)

        return self._assets_cache[asset.symbol]['1m']['data'].index[-1]

    def get_realtime_bars(self, assets, frequency):
        # if frequency == '1m':
        #     resample_freq = '1 Min'
        # elif frequency == '1d':
        #     resample_freq = '24 H'
        # else:
        #     raise ValueError("Invalid frequency specified: %s" % frequency)

        df = pd.DataFrame()
        for asset in assets:
            symbol = str(asset.symbol)
            self.subscribe_to_market_data(asset)

            # trade_prices = self._tws.bars[symbol]['last_trade_price']
            # trade_sizes = self._tws.bars[symbol]['last_trade_size']
            # ohlcv = trade_prices.resample(resample_freq).ohlc()
            # ohlcv['volume'] = trade_sizes.resample(resample_freq).sum()

            if self._assets_cache[symbol][frequency]['data'] is None:
                continue

            # Add asset as level 0 column; ohlcv will be used as level 1 cols
            ohlcv.columns = pd.MultiIndex.from_product([[asset, ],
                                                        ohlcv.columns])

            df = pd.concat([df, ohlcv], axis=1)

        return df
