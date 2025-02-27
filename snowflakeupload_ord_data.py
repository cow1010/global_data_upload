import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilenames
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

'''
---------------------- SET CONNECTION ----------------------
'''
conn = snowflake.connector.connect(
    user='milkyway',
    password='Miky2024!',
    account='gv28284.ap-northeast-2.aws',
    database='FNF',
    warehouse='DEV_WH'
)

# Set schema
conn.cursor().execute("USE SCHEMA EXCEL")

'''
---------------------- SET PARAMETER ----------------------
'''
sheet_list = None
TARGET_SCHEMA = 'EXCEL'
TARGET_TABLE = 'PERP_ORD_GLOBAL'

COLUMN_LIST_RAW = [
    'PO_NO', 'DISTRBT_CD', 'STYLE_CD', 'COLOR_CD', 'SKU', 'SIZE_CD', 'TAG_PRICE', 'SALE_QTY', 'TAG_AMT', 'SESN','DOMAIN', 'DROP_ITEM'
]
COLUMN_LIST = [
    'po_no', 'distrbt_cd', 'style_cd', 'color_cd', 'sku', 'size_cd', 'tag_price', 'sale_qty', 'tag_amt', 'sesn', 'domain', 'drop_item'
]
COLUMN_LIST_FINAL = [
    'po_no', 'distrbt_cd', 'style_cd', 'color_cd', 'sku', 'size_cd', 'tag_price', 'sale_qty', 'tag_amt', 'sesn', 'domain', 'drop_item'
]

DATA_SHEET_NAME = 'DATA'
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
    df = df[COLUMN_LIST_RAW]    # 컬럼명 필터링
    df.columns = COLUMN_LIST    # 컬럼명 설정
    df.columns = map(str.lower, df.columns)  # 컬럼명을 소문자로 변환
    print(df)
    return df

def add_column2df_and_fill_with_same_data(df, add_column_name, fill_data):
    df[add_column_name] = [fill_data for i in range(len(df))]
    return df

def update_df2db(df, schema, table_name, connection):
    # df -> db
    success, nchunks, nrows, _ = write_pandas(connection, df, table_name, schema=schema)
    if success:
        print(f'insert into {schema}.{table_name} data complete')
    else:
        print(f'failed to insert data into {schema}.{table_name}')

if __name__ == '__main__':
    try:
        print('START')
        files = open_files()
        for file in files:
            df = read_excel2df(file)
            update_df2db(df, TARGET_SCHEMA, TARGET_TABLE, conn)
        print('END')

    except Exception as e:
        print(e)
    finally:
        conn.close()
