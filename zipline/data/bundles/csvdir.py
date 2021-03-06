"""
Module for building a complete dataset from local directory with csv files.
"""
import os
import sys

import logbook
from numpy import empty
from pandas import DataFrame, read_csv, Index, Timedelta, NaT, concat

import requests
import yfinance
from pandas_datareader.data import DataReader
from pandas_datareader._utils import RemoteDataError


from zipline.utils.calendars import register_calendar_alias
from zipline.utils.cli import maybe_show_progress

logger = logbook.Logger(__name__)


def csvdir_equities(tframes=['daily'], start=None, end=None):
    """
    Generate an ingest function for custom data bundle

    Parameters
    ----------
    tframe: list or tuple, optional
        The data time frames ('minute', 'daily')
    start : datetime, optional
        The start date to query for. By default this pulls the full history
        for the calendar.
    end : datetime, optional
        The end date to query for. By default this pulls the full history
        for the calendar.
    Returns
    -------
    ingest : callable
        The bundle ingest function for the given set of symbols.
    Examples
    --------
    This code should be added to ~/.zipline/extension.py
    .. code-block:: python
       from zipline.data.bundles import csvdir_equities, register
       register('custom-csvdir-bundle',
                csvdir_equities(sys.environ['CSVDIR'],
                ['daily', 'minute']))

    Notes
    -----
    Environment variable CSVDIR must contain path to the directory with the
    following structure:
        daily/<symbol>.cvs files
        minute/<symbol>.csv files
    for each symbol.
    """

    return CSVDIRBundle(tframes, start, end).ingest

#@retry(tries=3, delay=1, backoff=2)
def RetryingDataReader(*args, **kwargs):
    return DataReader(*args, **kwargs)

def download_splits_and_dividends(symbols, metadata):
    # return None, None
    
    print(metadata)
    adjustments = []
    splits_df = None
    for sid, symbol in enumerate(symbols):
        try:
            print("Downloading splits for %s" % symbol)
            ticker = yfinance.Ticker(symbol)
            df = ticker.splits.to_frame(name='ratio')
            df = df[(df.index >= metadata.ix[sid].start_date) & (df.index <= metadata.ix[sid].end_date)]
        except:
            print("No data returned from Yahoo for %s" % symbol)
            df = DataFrame(columns=['ratio'])

        if not df.empty:
            df['sid'] = sid
            adjustments.append(df)

    if adjustments:
        splits_df = concat(adjustments)
        splits_df.index.name = 'effective_date'
        splits_df.reset_index(inplace=True)

    adjustments = []
    dividends_df = None
    for sid, symbol in enumerate(symbols):
        try:
            print("Downloading dividends for %s" % symbol)
            ticker = yfinance.Ticker(symbol)
            df = ticker.dividends.to_frame(name='amount')
            df = df[(df.index >= metadata.ix[sid].start_date) & (df.index <= metadata.ix[sid].end_date)]
        except:
            print("No data returned from Yahoo for %s" % symbol)
            df = DataFrame(columns=['amount'])

        if not df.empty:
            df['sid'] = sid
            adjustments.append(df)
    
    # we do not have this data in the yahoo dataset
    if adjustments:
        dividends_df = concat(adjustments)
        dividends_df['record_date'] = NaT
        dividends_df['declared_date'] = NaT
        dividends_df['pay_date'] = NaT
        dividends_df.index.name = 'ex_date'
        dividends_df.reset_index(inplace=True)

    return splits_df, dividends_df

class CSVDIRBundle:
    """
    Wrapper class to enable write access to self.splits and self.dividends
    from _pricing_iter method.
    """

    def __init__(self, tframes, start, end):
        self.tframes = tframes
        self.start = start
        self.end = end

        self.show_progress = None
        self.symbols = None
        self.metadata = None
        self.csvdir = None

        self.splits = None
        self.dividends = None

    def ingest(self, environ, asset_db_writer, minute_bar_writer,
               daily_bar_writer, adjustment_writer, calendar, start_session,
               end_session, cache, show_progress, output_dir):

        csvdir = os.environ.get('CSVDIR')
        if not csvdir:
            logger.error("CSVDIR environment variable is not set")
            sys.exit(1)

        if not os.path.isdir(csvdir):
            logger.error("%s is not a directory" % csvdir)
            sys.exit(1)

        for tframe in self.tframes:
            ddir = os.path.join(csvdir, tframe)
            if not os.path.isdir(ddir):
                logger.error("%s is not a directory" % ddir)

        for tframe in self.tframes:
            ddir = os.path.join(csvdir, tframe)

            self.symbols = sorted(item.split('.csv')[0]
                                  for item in os.listdir(ddir)
                                  if item.endswith('.csv'))
            if not self.symbols:
                logger.error("no <symbol>.csv files found in %s" % ddir)
                sys.exit(1)

            self.csvdir = ddir

            dtype = [('start_date', 'datetime64[ns]'),
                     ('end_date', 'datetime64[ns]'),
                     ('auto_close_date', 'datetime64[ns]'),
                     ('symbol', 'object')]
            self.metadata = DataFrame(empty(len(self.symbols), dtype=dtype))

            self.show_progress = show_progress

            if tframe == 'minute':
                writer = minute_bar_writer
            else:
                writer = daily_bar_writer

            writer.write(self._pricing_iter(), show_progress=show_progress)

            # Hardcode the exchange to "CSVDIR" for all assets and (elsewhere)
            # register "CSVDIR" to resolve to the NYSE calendar, because these
            # are all equities and thus can use the NYSE calendar.
            self.metadata['exchange'] = "CSVDIR"

        asset_db_writer.write(equities=self.metadata)

        # niucool: download splits and dividends from yahoo
        # code from: https://gist.github.com/tibkiss/d917ff389d479f32fff303f53893c3c5
        # adjustment_writer.write(splits=self.splits, dividends=self.dividends)

        splits, dividends = download_splits_and_dividends(
            self.symbols,
            self.metadata)

        adjustment_writer.write(splits=splits, dividends=dividends)

    def _pricing_iter(self):
        with maybe_show_progress(self.symbols, self.show_progress,
                                 label='Loading custom pricing data: ') as it:
            for sid, symbol in enumerate(it):
                logger.debug('%s: sid %s' % (symbol, sid))

                dfr = read_csv(os.path.join(self.csvdir, '%s.csv' % symbol),
                               parse_dates=[0], infer_datetime_format=True,
                               index_col=0).sort_index()

                # the start date is the date of the first trade and
                # the end date is the date of the last trade
                start_date = dfr.index[0]
                end_date = dfr.index[-1]

                # The auto_close date is the day after the last trade.
                ac_date = end_date + Timedelta(days=1)
                self.metadata.iloc[sid] = start_date, end_date, ac_date, symbol

                if 'split' in dfr.columns:
                    if self.splits is None:
                        self.splits = DataFrame()
                    tmp = dfr[dfr['split'] != 1.0]['split']
                    split = DataFrame(data=tmp.index.tolist(),
                                      columns=['effective_date'])
                    split['ratio'] = tmp.tolist()
                    split['sid'] = sid

                    index = Index(range(self.splits.shape[0],
                                        self.splits.shape[0] + split.shape[0]))
                    split.set_index(index, inplace=True)
                    self.splits = self.splits.append(split)

                if 'dividend' in dfr.columns:
                    if self.dividends is None:
                        self.dividends = DataFrame()
                    # ex_date   amount  sid record_date declared_date pay_date
                    tmp = dfr[dfr['dividend'] != 0.0]['dividend']
                    div = DataFrame(data=tmp.index.tolist(),
                                    columns=['ex_date'])
                    div['record_date'] = NaT
                    div['declared_date'] = NaT
                    div['pay_date'] = NaT
                    div['amount'] = tmp.tolist()
                    div['sid'] = sid
                    ind = Index(range(self.dividends.shape[0],
                                      self.dividends.shape[0] + div.shape[0]))
                    div.set_index(ind, inplace=True)
                    if self.dividends is None:
                        self.dividends = DataFrame()
                    self.dividends = self.dividends.append(div)

                yield sid, dfr


register_calendar_alias("CSVDIR", "NYSE")