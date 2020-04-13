import os
import pandas as pd
import tabula as tb
from _db_data import DBData


class DumpData:
    def __init__(self, schema):
        self._RAW_DATA_PATH = '../Data/raw/'
        self._SPT_DATA_PATH = '../Data/support/'
        self._db_obj = DBData(schema=schema)

    def dump_raw_data(self):
        if os.path.exists(self._RAW_DATA_PATH):
            for root, dirs, files in os.walk(self._RAW_DATA_PATH):
                for file in files:
                    df = pd.DataFrame()
                    file_path = os.path.join(root, file)
                    if file.endswith('.csv'):
                        df = pd.read_csv(file_path)
                        txt = '.csv'

                    elif file.endswith('.xlsx'):
                        df = pd.read_excel(file_path,  encoding="utf-16")
                        txt = '.xlsx'

                    if df.shape[0]:
                        fname = file.replace(txt, '').replace(' ', '_').lower()
                        table_name = f'raw_{fname}'

                        self._db_obj.create_table(df, [], [], [], table_name)
                        self._db_obj.insert_data(df, table_name)
                        print(f'Dump {table_name} successfully!')
        else:
            print('Cannot find raw data path!')

    def dump_spt_data(self):
        if os.path.exists(self._SPT_DATA_PATH):
            for root, dirs, files in os.walk(self._SPT_DATA_PATH):
                for file in files:
                    if file.endswith('csv'):
                        file_path = os.path.join(root, file)
                        df = pd.read_csv(file_path)
                        fname = file.replace('.csv', '').split('/')[-1]
                        table_name = f'spt_{fname}'
                        self._db_obj.insert_data(df, table_name)
                        print(f'Dump {table_name} successfully!')
        else:
            print('Cannot find support data path!')

    def dump_pdf_table(self, pdf_path, pages=2):
        if os.path.exists(pdf_path):
            for root, dirs, files in os.walk(pdf_path):
                for file in files:
                    if file.endswith('.pdf'):
                        file_path = os.path.join(root, file)
                        table_name = f'spt_br_{file[:-4]}'.lower()
                        dfs = tb.read_pdf(file_path, pages=pages)

                        try:
                            self._db_obj.create_table(dfs[0], [], [], [], table_name)
                            self._db_obj.insert_data(dfs[0], table_name)
                            print(f'save {table_name} successfully!')

                        except:
                            print(f'failed to save {table_name}!')

        else:
            print('Cannot find pdf path!')


dd = DumpData(schema='winter_2')
dd.dump_raw_data()
dd.dump_spt_data()
dd.dump_pdf_table(pdf_path='../Data/support/base_rate/')