
import pandas as pd
import numpy as np
from statsmodels.stats.stattools import medcouple
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from utilities import compute, compute_loop
import managerSQL


class Regression:
    def __init__(self, params):
        self.params = params['regression']
        self.sql_manager = managerSQL.ManagerSQL(params['db'])
        self.start_date = self.params['start_date']
        self.end_date = self.params['end_date']
        self.dates = self.get_dates()

    def get_dates(self):
        cal = CustomBusinessDay(calendar=USFederalHolidayCalendar())
        return pd.DatetimeIndex(start=self.start_date, end=self.end_date, freq=cal)

    def process(self):
        compute_loop(self.dates, self.fit_regression())

    def fit_regression(self, date):
        query = "select * from reg_factors where date = '" + str(date) + "'"
        df = self.sql_manager.select_query(query)
        if 'equity' in df.columns:
            df.drop(columns=['equity'], inplace=True)

        # Outliers
        df['weight'] = 1
        df = self.remove_outliers(df, a=3)

        # Scaling
        df = self.scale_factors(df, cols=['mcap', 'pb', 'mom'])
        print("Yeah!")

    def scale_factors(self, df, cols):
        w = df.weight/df.weight.sum()
        mu = df[cols].multiply(w).sum(axis=0) # crashed
        std = (df[cols]-mu).dot(df[cols]-mu).multiply(w).sum(axis=0)
        df[cols] = (df[cols]-mu).divide(std)
        return df

    def remove_outliers(self, df, a=1.5, n_sample=10000):
        """ Based on https://wis.kuleuven.be/stat/robust/papers/2008/outlierdetectionskeweddata-revision.pdf"""
        # If computed on a personal computer, next line will probably be necessary
        df_sample = df.sample(n_sample, random_state=0)
        mc = medcouple(df_sample.ret)
        percentiles = np.percentile(df_sample.ret, [25, 75])
        q1 = percentiles[0]
        q3 = percentiles[1]
        iqr = q3 - q1
        if mc > 0:
            lo = q1 - a*np.exp(-4*mc)*iqr
            up = q3 + a*np.exp(3*mc)*iqr
        else:
            lo = q1 - a*np.exp(-3*mc)*iqr
            up = q3 + a*np.exp(4*mc)*iqr
        df.loc[df.ret < lo, 'weight'] = 0
        df.loc[df.ret > up, 'weight'] = 0
        return df