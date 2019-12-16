
import pandas as pd
import numpy as np
from concurrent import futures
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import managerSQL
from utilities import compute, compute_loop

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


class DataProcessing:
    def __init__(self, params):
        self.params = params['data_processing']
        self.sql_manager = managerSQL.ManagerSQL(params['db'])
        if self.params['clean']:
            self.sql_manager.clean_table('reg_factors')
        self.elements = self.get_elements()
        self.equity = self._get_equity()
        self.shares = self._get_shares()

    def get_elements(self):
        symbols_all = self.sql_manager.select_column_list('symbol', 'symbols')
        symbols_fund = self.sql_manager.select_distinct_column_list('symbol', 'reg_factors')
        symbols = [s for s in symbols_all if s not in symbols_fund]
        return symbols

    def process(self):
        """ Main execution of DataProcessing class. """
        # Compute daily returns and factor exposures
        if self.params['compute_factors']:
            compute(self.elements, self.compute_factors)

    def compute_factors(self, symbol):
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
