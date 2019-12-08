
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent import futures
import managerSQL


class DataProcessing:
    def __init__(self):
        self.sql_manager = managerSQL.ManagerSQL()
        self.elements = self.get_elements()
        self.equity = self._get_equity()
        self.shares = self._get_shares()

    def get_elements(self):
        symbols_all = self.sql_manager.select_column_list('symbol', 'symbols')
        symbols_fund = self.sql_manager.select_distinct_column_list('symbol', 'prices_fundamentals')
        symbols = [s for s in symbols_all if s not in symbols_fund]
        return symbols

    def compute_pb(self, max_workers=6):
        print("\nProcessing symbols in parallel")
        ex = futures.ThreadPoolExecutor(max_workers=max_workers)
        args = self.elements
        ex.map(self.compute_pb_symbol, args)

    def compute_pb_loop(self, max_workers=6):
        print("\nProcessing symbols one by one")
        for symbol in self.elements:
            self.compute_pb_symbol(symbol)

    def compute_pb_symbol(self, symbol):
        """ Compute price to book value of symbol and upload to database. """
        t0 = datetime.now()

        df_prices = self.sql_manager.select_query("select * from prices where symbol = '" + symbol + "'")
        df_equity = self.equity[self.equity.symbol == symbol].sort_values('ddate', ascending=False)
        df_shares = self.shares[self.shares.symbol == symbol].sort_values('ddate', ascending=False)
        df_prices['equity'] = np.nan
        df_prices['shares'] = np.nan

        # Equity
        for row in df_equity.index:
            ddate = df_equity.ddate[row]
            equity = df_equity.equity[row]
            df_prices.loc[(df_prices.date > ddate) & pd.isna(df_prices.equity), 'equity'] = equity

        # Shares
        for row in df_shares.index:
            ddate = df_shares.ddate[row]
            shares = df_shares.shares_basic[row]
            df_prices.loc[(df_prices.date > ddate) & pd.isna(df_prices.shares), 'shares'] = shares

        # Upload data to db
        self.sql_manager.upload_df('prices_fundamentals', df_prices)

        t1 = datetime.now()
        print('Processing successful for {0} ({1:.2f} sec)'.format(symbol, (t1 - t0).total_seconds()))

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


