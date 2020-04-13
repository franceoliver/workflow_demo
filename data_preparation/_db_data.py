import pandas as pd
import re
import psycopg2
import psycopg2.extras


class DBData():
    def __init__(self, schema='wage_trust_demo'):
        self._CSTR = {
            'user': 'postgres',
            'password': 'docker',
            'host': '127.0.0.1',
            'port': '9876'
        }
        self._schema = schema

    def _col_name_clean(self, col_name):
        col_name = re.sub(r"[^A-Za-z0-9]", " ", col_name).lower()
        col_name = re.sub(r"[\s]+", " ", col_name)
        col_name = col_name.replace(' ', '_')
        col_name = f'_{col_name}'

        return col_name

    def create_table(self, df, col_json, col_time, p_key, table_name):
        df.columns = [self._col_name_clean(c) for c in df.columns]
        col_json = [self._col_name_clean(c) for c in col_json]
        col_time = [self._col_name_clean(c) for c in col_time]
        p_key = [self._col_name_clean(c) for c in p_key]

        fields = []

        for col in df.columns:
            if col in col_json:
                field = col + ' ' + 'json'

            elif col in col_time:
                field = col + ' ' + 'timestamp'

            else:
                if df[col].dtype == 'int64':
                    field = col + ' ' + 'integer'

                elif df[col].dtype == 'bool':
                    field = col + ' ' + 'BOOLEAN'

                elif df[col].dtype == 'float64':
                    field = col + ' ' + 'float'

                elif df[col].dtype == 'datetime64[ns]':
                    field = col + ' ' + 'timestamp'

                else:
                    field = col + ' ' + 'text'

            fields.append(field)

        if len(p_key) > 0:
            fields.append(' PRIMARY KEY ({})'.format(','.join(p_key)))

        query_txt = 'CREATE TABLE IF NOT EXISTS {}.{} ({});'.format(self._schema, table_name, ','.join(fields))

        with psycopg2.connect(**self._CSTR) as conn:
            with conn.cursor() as cur:
                cur.execute(query_txt)

    def _insert_value_prep(self, row):
        return tuple([None if (c != c) | pd.isnull(c) else str(c) for c in row.values])

    def insert_data(self, df, table_name):
        # Collect table column names
        query_txt = f'''
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{self._schema}'
                AND table_name   = '{table_name}'
        '''
        with psycopg2.connect(**self._CSTR) as conn:
            tab_col_df = pd.read_sql(query_txt, con=conn)

        tab_col_name = tab_col_df['column_name'].to_list()

        # prepare insert query txt
        ins = f'''insert into {'.'.join([self._schema, table_name])} ''' + \
              str(tuple(tab_col_name)).replace("'", "\"") + ''' values %s '''

        ivals = df.apply(self._insert_value_prep, axis=1).to_list()

        with psycopg2.connect(**self._CSTR) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DELETE from {self._schema}.{table_name}')
                psycopg2.extras.execute_values(cur, ins, ivals, page_size=ivals.__len__())


    def retrieve_data(self, table_name):
        query_txt = f"SELECT * FROM {self._schema}.{table_name}"

        with psycopg2.connect(**self._CSTR) as conn:
            with conn.cursor() as cur:
                df = pd.read_sql(query_txt, con=conn)

        return df
