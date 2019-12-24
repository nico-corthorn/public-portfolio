
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class ManagerSQL:
    def __init__(self, sql_params):
        db_name = sql_params['db_name']
        user = sql_params['user']
        password = sql_params['password']
        port = sql_params['port']

        # Connexion for downloading data
        self.cnxn = psycopg2.connect(host="localhost", database=db_name, user=user, password=password)
        self.cursor = self.cnxn.cursor()

        # Connexion for uploading data
        self.db_url = 'postgresql://' + user + ':' + password + '@localhost/' + db_name
        self.con = create_engine(self.db_url)

    def select(self, table):
        """ Returns table as DataFrame. """
        sql = 'select * from '+table
        df = pd.read_sql(sql, self.cnxn)
        return df

    def select_query(self, query):
        """ Returns query output as DataFrame. """
        df = pd.read_sql(query, self.cnxn)
        return df

    def select_column_list(self, column, table):
        """ Returns column values as list. """
        sql = 'select '+column+' from '+table+' order by '+column
        df = pd.read_sql(sql, self.cnxn)
        lst = [element for element in df[column]]
        return lst

    def select_distinct_column_list(self, column, table):
        """ Returns unique values of column as list. """
        sql = 'select distinct '+column+' from '+table+' order by '+column
        df = pd.read_sql(sql, self.cnxn)
        lst = [element for element in df[column]]
        return lst

    def select_as_dictionary(self, column_key, column_value, table):
        """ Return dictionary column_key: column_value, column_key must have unique values. """
        sql = 'select '+column_key+', '+column_value+' from '+table
        df = pd.read_sql(sql, self.cnxn)
        assert df[column_key].is_unique, "Column "+column_key+" doesn't have unique values."
        return df.set_index(column_key).to_dict()[column_value]

    def upload_df(self, table, df):
        """ Uploads data frame to table. Appends information. """
        df.to_sql(name=table, con=self.con, if_exists='append', index=False)

    def query(self, query):
        """ Executes query. Doesn't return anything.
            Intended for customized delete queries. """
        self.cursor.execute(query)
        self.cnxn.commit()

    def clean_table(self, table):
        """ Delete all information from the table. """
        self.cursor.execute('delete from ' + table)
        self.cnxn.commit()
