
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
from statsmodels.stats.stattools import medcouple
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from utilities import compute, compute_loop
import managerSQL

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


class DataProcessing:
    def __init__(self, params):
        self.params = params['data_processing']
        self.sql_manager = managerSQL.ManagerSQL(params['db'])
        if self.params['compute_raw_factors']:
            if self.params['clean_raw_factors']:
                self.sql_manager.clean_table('reg_factors')
            self.elements = self.get_elements()
            self.equity = self._get_equity()
            self.shares = self._get_shares()
        if self.params['scale_factors']:
            if self.params['clean_scaled_factors']:
                #self.sql_manager.clean_table('reg_factors')
                pass
            self.start_date = self.params['start_date']
            self.end_date = self.params['end_date']
            self.dates = self.get_dates()

    def get_elements(self):
        symbols_all = self.sql_manager.select_column_list('symbol', 'symbols')
        symbols_fund = self.sql_manager.select_distinct_column_list('symbol', 'reg_factors')
        symbols = [s for s in symbols_all if s not in symbols_fund]
        return symbols

    def get_dates(self):
        cal = CustomBusinessDay(calendar=USFederalHolidayCalendar())
        return pd.DatetimeIndex(start=self.start_date, end=self.end_date, freq=cal)

    def process(self):
        """ Main execution of DataProcessing class. """
        # Compute daily returns and factor exposures
        if self.params['compute_raw_factors']:
            compute(self.elements, self.compute_raw_factors)

        # Scale factors
        if self.params['scale_factors']:
            compute(self.dates, self.process_cross_section)

    def compute_raw_factors(self, symbol):
        """ Compute price to book value of symbol and upload to database. """
        t0 = datetime.now()

        try:
            df_prices = self.sql_manager.select_query("select * from prices where symbol = '" + symbol + "'")

            if df_prices.shape[0] > 0:

                # Returns
                df_prices_1d = df_prices.copy()
                cols = ['date', 'adjclose']
                df_prices_1d = df_prices_1d[cols]
                df_prices_1d['date'] = (df_prices_1d.date + BDay(1)).dt.date
                s1d = ('', '_1d')
                df_prices = df_prices.merge(df_prices_1d, left_on='date', right_on='date', how='left', suffixes=s1d)
                df_prices['ret'] = np.log(df_prices.adjclose) - np.log(df_prices.adjclose_1d)
                df_prices.drop(columns=['adjclose_1d'], inplace=True)
                col_remains = ['symbol', 'date', 'ret']

                # Equity
                df_equity = self.equity[self.equity.symbol == symbol].sort_values('ddate', ascending=False)
                df_prices['equity'] = np.nan
                for row in df_equity.index:
                    ddate = df_equity.ddate[row]
                    equity = df_equity.equity[row]
                    df_prices.loc[(df_prices.date > ddate) & pd.isna(df_prices.equity), 'equity'] = equity
                col_remains.append('equity')

                # Shares
                df_shares = self.shares[self.shares.symbol == symbol].sort_values('ddate', ascending=False)
                df_prices['shares'] = np.nan
                for row in df_shares.index:
                    ddate = df_shares.ddate[row]
                    shares = df_shares.shares_basic[row]
                    df_prices.loc[(df_prices.date > ddate) & pd.isna(df_prices.shares), 'shares'] = shares

                # Market Price
                # Adjust by dividends paid since last equity published...
                df_prices['mcap'] = df_prices['close'].multiply(df_prices['shares'])
                col_remains.append('mcap')

                # Price to Book Value
                df_prices['pb'] = df_prices['mcap'].divide(df_prices['equity'])
                col_remains.append('pb')

                # Momentum
                df_prices_12m = df_prices.copy()
                df_prices_1m = df_prices.copy()
                cols = ['date', 'adjclose']
                df_prices_12m = df_prices_12m[cols]
                df_prices_1m = df_prices_1m[cols]
                df_prices_12m['date'] = (df_prices_12m.date + BDay(260)).dt.date
                df_prices_1m['date'] = (df_prices_1m.date + BDay(20)).dt.date
                s12m = ('', '_12m')
                s1m = ('', '_1m')
                df_prices = df_prices.merge(df_prices_12m, left_on='date', right_on='date', how='left', suffixes=s12m)
                df_prices = df_prices.merge(df_prices_1m, left_on='date', right_on='date', how='left', suffixes=s1m)
                df_prices['mom'] = np.log(df_prices.adjclose_1m) - np.log(df_prices.adjclose_12m)
                df_prices['mom'] = df_prices.mom.fillna(method='ffill', limit=5)
                df_prices.drop(columns=['adjclose_12m', 'adjclose_1m'], inplace=True)
                col_remains.append('mom')

                # Clean
                df_prices = df_prices[col_remains]
                df_prices.dropna(inplace=True)

                if df_prices.shape[0] > 0:
                    # Upload data to db
                    self.sql_manager.upload_df('reg_factors', df_prices)

            t1 = datetime.now()
            print('Processing successful for {0} ({1:.2f} sec)'.format(symbol, (t1 - t0).total_seconds()))

        except Exception as e:
            t1 = datetime.now()
            print('Processing failed for {0} ({1:.2f} sec)'.format(symbol, (t1 - t0).total_seconds()))
            print(e)

    def _get_equity(self):
        query = \
            """
            select 
                symbol, cast(ddate as date) ddate, cast(filed as date) filed, equity 
                from 
                ( 
                    select 
                        symbol, ddate, filed, equity, 
                        row_number() over(partition by symbol, ddate order by filed) rnk 
                        from sec_sub a 
                        inner join sec_cik_symbol b 
                        on a.cik = b.cik 
                        left join sec_num_bal c 
                        on a.adsh = c.adsh and c.uom = 'USD' 
                        where not equity is null 
                ) a 
            where rnk = 1 
            order by symbol, ddate 
            """
        df = self.sql_manager.select_query(query)
        return df

    def _get_shares(self):
        query = \
            """
            select 
                symbol, cast(ddate as date) ddate, cast(filed as date) filed, shares_basic 
                from 
                ( 
                    select 
                        symbol, ddate, filed, shares_basic, 
                        row_number() over(partition by symbol, ddate order by filed) rnk 
                        from sec_sub a 
                        inner join sec_cik_symbol b 
                        on a.cik = b.cik 
                        left join sec_num_shr c 
                        on a.adsh = c.adsh 
                        where not shares_basic is null and shares_basic <> 0 
                ) a 
            where rnk = 1 
            order by symbol, ddate 
            """
        df = self.sql_manager.select_query(query)
        return df

    def process_cross_section(self, date):
        t0 = datetime.now()
        date_str = str(date.date())

        try:
            query = "select * from reg_factors where date = '" + date_str + "'"
            df = self.sql_manager.select_query(query)

            if df.shape[0] > 0:
                if 'equity' in df.columns:
                    df.drop(columns=['equity'], inplace=True)

                # Outliers
                df['weight'] = 1
                df = self.remove_outliers(df, a=3)

                # Scaling
                df = self.scale_factors(df, cols=['mcap', 'pb', 'mom'])

                # Upload data to db
                self.sql_manager.upload_df('reg_factors_scaled', df)

            t1 = datetime.now()
            print('Processing successful for {0} ({1:.2f} sec)'.format(date_str, (t1 - t0).total_seconds()))

        except Exception as e:
            t1 = datetime.now()
            print('Processing failed for {0} ({1:.2f} sec)'.format(date_str, (t1 - t0).total_seconds()))
            print(e)

    def scale_factors(self, df, cols):
        w = df.weight / df.weight.sum()
        mu = df[cols].mul(w, axis=0).sum(axis=0)
        delta = df[cols] - mu
        std = delta.mul(delta, axis=0).mul(w, axis=0).sum(axis=0).apply(np.sqrt)
        df[cols] = delta.div(std, axis=1)
        return df

    def remove_outliers(self, df, a=1.5, n_sample=10000):
        """ Based on https://wis.kuleuven.be/stat/robust/papers/2008/outlierdetectionskeweddata-revision.pdf"""
        mc = medcouple(df.ret)
        percentiles = np.percentile(df.ret, [25, 75])
        q1 = percentiles[0]
        q3 = percentiles[1]
        iqr = q3 - q1
        if mc > 0:
            lo = q1 - a * np.exp(-4 * mc) * iqr
            up = q3 + a * np.exp(3 * mc) * iqr
        else:
            lo = q1 - a * np.exp(-3 * mc) * iqr
            up = q3 + a * np.exp(4 * mc) * iqr
        df.loc[df.ret < lo, 'weight'] = 0
        df.loc[df.ret > up, 'weight'] = 0
        return df
