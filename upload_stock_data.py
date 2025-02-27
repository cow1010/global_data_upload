import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilenames
from sqlalchemy import create_engine

'''
---------------------- SET ENGINE ----------------------
'''
REDSHIFT_ENGINE = create_engine("postgresql://data_user:Duser2022!#@prd-dt-redshift.conhugwtudej.ap-northeast-2.redshift.amazonaws.com:5439/fnf")
# LOCAL_POSTGRESQL_ENGINE = create_engine("postgresql+psycopg2://postgres:1111@172.0.2.93:5432/postgres")
AWS_POSTGRESQL_ENGINE = create_engine("postgresql+psycopg2://postgres:fnf##)^2020!@f-dt-process-env.cuhyn2zzixjx.ap-northeast-2.rds.amazonaws.com:35430/postgres")
# ------------------------------------------------------

'''
---------------------- SET PARAMETER ----------------------
'''
sheet_list = None
TARGET_SCHEMA = 'excel'
TARGET_TABLE = 'temp_barcode_stock_global'

COLUMN_LIST_RAW = [
    'DT', 'Date', 'Barcode', 'Stock Quantity',
]
COLUMN_LIST = [
    'distrbt_cd', 'dt', 'barcode', 'stock_qty',
]
COLUMN_LIST_FINAL = [
    'distrbt_cd', 'dt', 'barcode', 'stock_qty',
]

DATA_SHEET_NAME = 'DATA'
INSERT_ENGINE = AWS_POSTGRESQL_ENGINE
# -----------------------------------------------------------

def open_files():
    # 파일 GUI로 열기
    Tk().withdraw()
    open_file_name = askopenfilenames()
    print('READ ', open_file_name)
    return open_file_name

def read_excel2df(open_file_name):
    # excel -> dataframe
    df = pd.read_excel(open_file_name, sheet_name=DATA_SHEET_NAME)  # get all sheets : None
    # df = pd.read_csv(open_file_name, header=None, encoding='utf-16')
    # df.columns = COLUMN_LIST_RAW 
    # df = df[COLUMN_LIST]
    df = df[COLUMN_LIST_RAW]    # 이상한 컬럼명으로 인해
    df.columns = COLUMN_LIST    # 컬럼명 설정
    print(df)
    return df

def add_column2df_and_fill_with_same_data(df, add_column_name, fill_data):
    df[add_column_name] = [fill_data for i in range(len(df))]
    return df

def update_df2db(df, schema, table_name, engine):
    # ----- 전처리 -----
    # column 설정
    # df.columns = COLUMN_LIST
    # df = add_column2df_and_fill_with_same_data(df, 'upload_date', '2021-09-01')
    # df = df.reindex(columns=COLUMN_LIST_FINAL)
    # print(df)

    # df -> db
    df.to_sql(name=table_name,
               con=engine, 
               schema=schema, 
               if_exists='append',
               index=False,
               chunksize=10000,
               method='multi',
               )
    # engine.execute('commit;')
    print(f'insert into {schema}.{table_name} data complete') 


if __name__ == '__main__':
    try:
        print('START')
        files = open_files()
        for file in files:
            df = read_excel2df(file)
            update_df2db(df, TARGET_SCHEMA, TARGET_TABLE, INSERT_ENGINE)
        print('END')

    except Exception as e:
        print(e)