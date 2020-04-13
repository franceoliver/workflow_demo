import pandas as pd
import psycopg2
import psycopg2.extras

class DBData():
    def __init__(self, schema='wage_trust_demo'):
        self._CSTR = {
            'user'      : 'postgres',
            'password'  : 'docker',
            'host'      : 'wage_trust_db'
        }
        self._schema = schema
        
    def _col_name_clean(self, col_name):
        col_name = col_name.lower()
            
        if ' ' in col_name:
            col_name = col_name.replace(' ','_')

        if '/' in col_name:
            col_name = col_name.replace('/','_')
        
        if "'" in col_name:
            col_name = col_name.replace("'", "\"")
        
        return col_name
        
    
    def create_table(self, df, col_json, col_time, p_key, table_name):
        fields = []

        for col in df.columns:
            col_name = self._col_name_clean(col)
            
            if col.lower() in col_json:
                field = col_name + ' ' + 'json'

            elif col.lower() in col_time:
                field = col_name + ' ' + 'TIMESTAMPTZ'

            else:
                if df[col].dtype == 'object':
                    field = col_name + ' ' + 'text'

                if df[col].dtype == 'int64':
                    field = col_name + ' ' + 'integer'

                if df[col].dtype == 'bool':
                    field = col_name + ' ' + 'BOOLEAN'

                if df[col].dtype == 'float64':
                    field = col_name + ' ' + 'float'

            fields.append(field)

        if len(p_key) > 0:
            fields.append(' PRIMARY KEY ({})'.format(','.join(p_key)))

        query_txt = 'CREATE TABLE IF NOT EXISTS {}.{} ({});'.format(self._schema, table_name, ','.join(fields))

        with psycopg2.connect(**self._CSTR) as conn:
            with conn.cursor() as cur:
                cur.execute(query_txt)
                conn.commit()


    def insert_data(self,df, table_name):
        #Collect table column names
        query_txt = f'''
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{self._schema}'
                AND table_name   = '{table_name}'
        '''
        with psycopg2.connect(**self._CSTR) as conn:
            with conn.cursor() as cur:
                tab_col_df = pd.read_sql(query_txt, con=conn)
        tab_col_name = tab_col_df['column_name'].to_list()
        
        #create df column name check dict
        df_col_name = {self._col_name_clean(col): col for col in df.columns}
        
        #refine table column names with df col names
        res_tab_col = [col for col in tab_col_name if col in df_col_name.keys()]
        
        #prepare insert query txt
        ins = f'''insert into {'.'.join([self._schema, table_name])} ''' + \
              str(tuple(res_tab_col)).replace("'", "\"") + ''' values %s '''
        
        

        ivals = []
        for _, row in df.iterrows():
            tmp_val = []
            for col in res_tab_col:
                tmp_val.append(str(row[df_col_name[col]]))
                
            ivals.append(tuple(tmp_val))

        print(ins)
        print(ivals)
        with psycopg2.connect(**self._CSTR) as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, ins, ivals, page_size=ivals.__len__())
                conn.commit()
        
        
    def retrieve_data(self, table_name):
        query_txt = f"SELECT * FROM {self._schema}.{table_name}"

        with psycopg2.connect(**self._CSTR) as conn:
            with conn.cursor() as cur:
                df = pd.read_sql(query_txt, con=conn)

        return df
            
