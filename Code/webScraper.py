
import pandas as pd
import numpy as np
from datetime import datetime
import requests
import json
from concurrent import futures
from pandas.tseries.offsets import BDay
import pandas_datareader as pdr
import zipfile
import io
from utilities import compute, compute_loop
import managerSQL

pd.options.mode.chained_assignment = None


# noinspection PyAttributeOutsideInit
class WebScraper:
    def __init__(self, scraper_config, sql_config):
        self.scraper_config = scraper_config
        self.limit_reached = False
        self.sql_manager = managerSQL.ManagerSQL(sql_config)
        self.today = pd.datetime.today().date()
        self.last_business_date = (self.today - BDay(1)).date()
        self.min_date = pd.to_datetime('1960-01-01').date()

    def build(self):
        self.elements = self.get_elements_to_download()

    def get_elements_to_download(self):
        """ Saves in self.elements a list with the symbols to scrape and the last date in the db.
            Overwrite if necessary.
        """
        df = self.sql_manager.select('symbols_last_date').iloc[:, :]
        lst = df.reindex().values.tolist()
        return lst

    def process(self):
        compute(self.elements, self.scrape)

    def scrape(self, args):
        """ Overwrite this method with child class definition. """
        pass


# noinspection PyAttributeOutsideInit
class IexScraper(WebScraper):
    def build(self):
        super().build()
        self.base_url = r'https://cloud.iexapis.com/stable/stock/'
        self.api_key = self.scraper_config['api_key']

    def scrape(self, args):
        symbol = args[0]
        last_date = args[1]
        if not self.limit_reached:
            if last_date is None or last_date < self.last_business_date:
                horizon = self.get_horizon(last_date)
                daily_url = self.base_url + symbol + '/chart/' + horizon + '?token=' + self.api_key
                try:
                    # Download data
                    t0 = datetime.now()
                    with requests.Session() as s:
                        download = s.get(daily_url)
                        decoded_content = download.content.decode('utf-8')
                        data = json.loads(decoded_content)
                        data = pd.DataFrame(data)[['date', 'open', 'close', 'volume']]
                        if last_date is not None:
                            data = data[pd.to_datetime(data.date) > pd.Timestamp(last_date)]
                        data.rename(columns={
                            'date': 'trade_date',
                            'open': 'open_price',
                            'close': 'close_price'}, inplace=True)
                        data['symbol'] = symbol
                        data['data_source'] = 'iex'
                        columns = ['symbol', 'trade_date', 'open_price', 'close_price', 'volume', 'data_source']
                        data = data[columns]

                    # Upload data to db
                    self.sql_manager.upload_df('prices', data)

                    t1 = datetime.now()
                    print('Download successful for {0} ({1:.2f} sec)'.format(symbol, (t1 - t0).total_seconds()))

                except Exception as e:
                    print('Download failed for {}. Tried: {}'.format(symbol, daily_url))
                    print(e)

    def get_horizon(self, last_date):
        # 5y, 2y, 1y, ytd, 6m, 3m, 1m, 5d
        # 1mm, 5dm
        if last_date is None:
            return 'max'
        days = (self.today - last_date).days
        if days >= 365*2:
            return '5y'
        elif days >= 365:
            return '2y'
        elif days >= 30*6:
            return '1y'
        elif days >= 30*3:
            return '6m'
        elif days >= 5:
            return '1m'
        return '5d'


# noinspection PyAttributeOutsideInit
class TiingoScraper(WebScraper):
    def build(self):
        super().build()
        self.api_key = self.scraper_config['api_key']

    def scrape(self, args):
        symbol = args[0]
        last_date = args[1]
        if not self.limit_reached:
            if last_date is None or last_date < self.last_business_date:
                try:
                    t0 = datetime.now()
                    if last_date is None:
                        min_date = self.min_date
                    else:
                        min_date = last_date + BDay(1)
                    data = pdr.get_data_tiingo(symbol, min_date, self.last_business_date, api_key=self.api_key)
                    # adjClose adjHigh adjLow adjOpen adjVolume close divCash high low open splitFactor volume
                    cols = ['symbol', 'date', 'open', 'close', 'adjClose', 'divCash', 'volume', 'splitFactor']
                    data = data.reset_index()[cols]
                    data['date'] = data['date'].dt.date
                    data.rename(columns={'splitFactor': 'split'}, inplace=True)
                    data.columns = [col.lower() for col in data.columns]

                    # Upload data to db
                    self.sql_manager.upload_df('prices', data)

                    t1 = datetime.now()
                    print('Download successful for {0} ({1:.2f} sec)'.format(symbol, (t1 - t0).total_seconds()))

                except Exception as e:
                    print('Download failed for {}'.format(symbol))
                    print(e)


# noinspection PyAttributeOutsideInit
class SecScraper(WebScraper):
    def build(self):
        self.sub_files = self.sql_manager.select_distinct_column_list('query', 'sec_sub')
        df = self.sql_manager.select('sec_tags_main').iloc[:, :]
        self.tags = {df.iloc[i, 0]: df.iloc[i, 1] for i in df.index}
        self.col_bal = list(df[df.tab == 'bal']['col'])
        self.col_res = list(df[df.tab == 'res']['col'])
        self.col_shr = list(df[df.tab == 'shr']['col'])
        self.elements = self.get_elements_to_download()

    def get_elements_to_download(self):
        now = datetime.now()
        this_year = now.year
        this_month = now.month
        lst = []
        for year in range(2009, this_year + 1):
            for quarter in range(1, 5):
                if year == this_year:
                    quarter_flt = this_month/3.0
                    if quarter >= quarter_flt:
                        break
                period = str(year) + 'q' + str(quarter)
                if period not in self.sub_files:
                    lst.append([period])
        if ['2009q1'] in lst:
            lst.remove(['2009q1'])
        return lst

    def scrape(self, args):
        period = args[0]
        sec_quarter_url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/' + period + '.zip'

        try:
            print('Downloading {0}'.format(period))
            t0 = datetime.now()
            with requests.Session() as s:
                download = s.get(sec_quarter_url)
                zip_file = zipfile.ZipFile(io.BytesIO(download.content))
                zip_file.extractall()
                sub_file = zip_file.open('sub.txt')
                num_file = zip_file.open('num.txt')
                sub_type = {'adsh': str, 'cik': int, 'name': str, 'sic': object, 'countryba': str, 'stprba': str,
                            'fye': str, 'form': str, 'period': str, 'fy': object, 'fp': str, 'filed': str}
                num_type = {'adsh': str, 'tag': str, 'version': str, 'ddate': str, 'qtrs': int, 'uom': str,
                            'coreg': str, 'value': float}
                sub_df = pd.read_csv(sub_file, sep='\t', encoding='ISO-8859-1', dtype=sub_type)
                num_df = pd.read_csv(num_file, sep='\t', encoding='ISO-8859-1', dtype=num_type)

                if period not in self.sub_files:
                    # Submission data set
                    self.upload_sub(sub_df, period)

                    # Number data set
                    self.upload_num(num_df, period)

                    t1 = datetime.now()
                    print('Download successful for {0} ({1:.2f} sec)'.format(period, (t1 - t0).total_seconds()))

        except Exception as e:
            print('Download failed for {}'.format(period))
            print(e)

    def upload_sub(self, sub_df, period):
        """
        adsh: Identifier of the submission.
        cik: Central Index Key. Identifier of the registrant.
        name: Name of the registrant.
        sic: Standard Industrial Classification. Identifier for type of business.
        countryba: Country of registrant's business address.
        stprba: State or province of registrant's business address.
        fye: Fiscal year end date (mmdd).
        form: Submission type.
        period: Balance Sheet Date (yymmdd)
        fy: Fiscal year (yyyy).
        fp: Fiscal period focus (FY, Q1, Q2, Q3, Q4, H1, H2, M9, T1, T2, T3, M8, CY)
        filed: Date of the registrant's filing (yymmdd)
        """
        columns = ['adsh', 'cik', 'name', 'sic', 'countryba', 'stprba', 'fye', 'form', 'period', 'fy', 'fp', 'filed']
        data = sub_df[columns]
        data.loc[:, 'query'] = period
        data.replace('nan', np.nan, inplace=True)
        values = {'sic': 0, 'fy': 0}
        data.fillna(value=values, inplace=True)
        data.dropna(subset=['adsh', 'cik', 'fye', 'form'], inplace=True)
        values = {'cik': int, 'sic': int, 'fy': int, 'period': str, 'fye': str, 'filed': str}
        data = data.astype(values)
        data['period'] = data['period'].apply(lambda r: r[:8])
        data['fye'] = data['fye'].apply(lambda r: r[:4])

        # Upload data to db
        self.sql_manager.upload_df('sec_sub', data)

        print('\tDownload successful for sub {0}'.format(period))

    def upload_num(self, num_df, period):
        """
        adsh: Identifier of the submission.
        tag: Identifier (name) for an account.
        version: Accounting standard.
        ddate: End date for the data value, rounded to the nearest month end.
        qtrs: Number of quarters represented. 0 indicates a point-in-time value.
        uom: Unit of measure for the value (currency).
        coreg: Coregistrant of the parent company registrant.
        value: The value.
        """
        columns = ['adsh', 'tag', 'version', 'ddate', 'qtrs', 'uom', 'coreg', 'value']
        data = num_df[columns]
        data = data[data.tag.isin(self.tags.keys())]
        data.dropna(subset=['adsh', 'version', 'tag', 'ddate', 'qtrs', 'uom', 'value'], inplace=True)
        values = {'ddate': str, 'qtrs': int, 'coreg': str}
        data = data.astype(values)
        data_shr = data[data.uom == 'shares']
        data_mon = data[data.uom != 'shares']
        data_bal = data_mon[data_mon.qtrs == 0]
        data_res = data_mon[data_mon.qtrs != 0]

        # Pivot
        index_bal = ['adsh', 'uom', 'ddate']
        index_res = ['adsh', 'uom', 'ddate', 'qtrs']
        index_shr = ['adsh', 'ddate', 'qtrs']
        data_bal_pvt = data_bal.pivot_table(values='value', index=index_bal, columns='tag', aggfunc=np.mean)
        data_res_pvt = data_res.pivot_table(values='value', index=index_res, columns='tag', aggfunc=np.mean)
        data_shr_pvt = data_shr.pivot_table(values='value', index=index_shr, columns='tag', aggfunc=np.mean)
        data_bal_pvt.rename(columns=self.tags, inplace=True)
        data_res_pvt.rename(columns=self.tags, inplace=True)
        data_shr_pvt.rename(columns=self.tags, inplace=True)
        data_bal_pvt = data_bal_pvt.div(1000)
        data_res_pvt = data_res_pvt.div(1000)
        data_shr_pvt = data_shr_pvt.div(1000)
        data_bal_pvt = data_bal_pvt[self.col_bal].reset_index()
        data_res_pvt = data_res_pvt[self.col_res].reset_index()
        data_shr_pvt = data_shr_pvt[self.col_shr].reset_index()

        # Upload data to db
        self.sql_manager.upload_df('sec_num_bal', data_bal_pvt)
        self.sql_manager.upload_df('sec_num_res', data_res_pvt)
        self.sql_manager.upload_df('sec_num_shr', data_shr_pvt)

        print('\tDownload successful for num {0}'.format(period))

    def clean(self):
        print('Cleaning SEC tables')
        self.sql_manager.clean_table('sec_sub')
        self.sql_manager.clean_table('sec_num_bal')
        self.sql_manager.clean_table('sec_num_res')
        self.sql_manager.clean_table('sec_num_shr')
