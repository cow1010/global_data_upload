import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilenames
from sqlalchemy import create_engine

'''
---------------------- SET ENGINE ----------------------
'''
REDSHIFT_ENGINE = create_engine("postgresql://data_user:Duser2022!#@prd-dt-redshift.conhugwtudej.ap-northeast-2.redshift.amazonaws.com:5439/fnf")
# LOCAL_POSTGRESQL_ENGINE = create_engine("postgresql+psycopg2://postgres:1111@172.0.2.93:5432/postgres")
AWS_POSTGRESQL_ENGINE = create_engine("postgresql+psycopg2://postgres:fnf##)^2020!@fnf-process.ch4iazthcd1k.ap-northeast-2.rds.amazonaws.com:35430/postgres")
# ------------------------------------------------------


'''
---------------------- SET PARAMETER ----------------------
'''
sheet_list = None
TARGET_SCHEMA = 'excel'
TARGET_TABLE = 'temp_global_orderlist'

COLUMN_LIST_RAW = [
'BRAND', 'Territory', 'Season', 'STYLE', 'COLOR', 'SIZE', 'Tag', 'Qty', 'AMT(tag)', 'ETA Korea/ETD CO', 'ETA Local', 'REMARK 1', 'REMARK 2', 'ETD',
]
COLUMN_LIST = [
'brd_cd', 'distrbt_cd', 'sesn', 'part_cd', 'color_cd', 'size_cd', 'tag_price', 'qty', 'tag_amt', 'eta_korea', 'eta_local', 'remark1', 'remark2', 'etd',
]
COLUMN_LIST_FINAL = [
'brd_cd', 'distrbt_cd', 'sesn', 'part_cd', 'color_cd', 'size_cd', 'tag_price', 'qty', 'tag_amt', 'eta_korea', 'eta_local', 'remark1', 'remark2', 'etd',
]


DATA_SHEET_NAME = 'ORDERLIST'
INSERT_ENGINE = AWS_POSTGRESQL_ENGINE
# -----------------------------------------------------------

def open_files():
    # íì¼ GUIë¡ ì í
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
    # set_param(df)
    df = df[COLUMN_LIST_RAW]    # íìí ì¹¼ë¼ë§ ìë¥´ê¸°
    df.columns = COLUMN_LIST    # ì¹¼ë¼ëª ë°ê¾¸ê¸°
    print(df)
    return df


def add_column2df_and_fill_with_same_data(df, add_column_name, fill_data):
    df[add_column_name] = [fill_data for i in range(len(df))]
    return df



def update_df2redshift(file, dataset, engine, schema, table_name):
    #df = read_excel2df()    
    #dataset = df


    ## ì  ìí¸ì ìë ë´ì© í©ì¹ê¸°
    #dataset = pd.DataFrame()
    #for sheet in sheet_list:
    #    dataset = pd.concat([dataset, df[sheet]])
    #print('GOT ALL DATA')
    
    # ----- ì ì²ë¦¬ -----
    # COLUMN ì¤ì 
    # dataset.columns = COLUMN_LIST
    # dataset = add_column2df_and_fill_with_same_data(dataset, 'distrbt_cd', 'TH')
    # dataset = add_column2df_and_fill_with_same_data(dataset, 'on_off_cls', 'on')
    dataset = dataset.reindex(columns=COLUMN_LIST_FINAL)
    print(dataset)
    
    ## kwd_nm_google ì ì¸íê³  ëì´ì°ê¸° ì ê±°
    #""" for col in dataset.columns[:-1]:
    #    dataset[col] = dataset[col].str.replace(" ", "")
    #print('DELETE SPACE WITHOUT GOOGLE KEYWORD COMPLETE') """
    #
    ## ì¤ë³µ ì ê±°
    #""" dataset = dataset.drop_duplicates(ignore_index=True)
    #print('DROP DUPLICATES COMPLETE') """
    ## -----------------
    #
    # ----- REDSHIFT ìë¡ë -----
    # truncate
    query_truncate = f"""
        truncate table {schema}.{table_name};
    """
    # engine.execute(query_truncate)
    # print(f'TRUNCATE TABLE {schema}.{table_name} COMPLETE')


    # insert
    dataset.to_sql(name = f'{table_name}',   # insert_temp_table
                    con = engine,
                    schema = f'{schema}',
                    if_exists = 'append',
                    index = False,
                    chunksize=10000,
                    method='multi'
                    )
    engine.execute('commit;')
    print(f'insert into {schema}.{table_name} data complete')  # insert_temp_table
    # --------------------------




if __name__ == '__main__':
    try:
        print('START')
        files = open_files()
        for file in files:
            df = read_excel2df(file)
            update_df2redshift(file, df, INSERT_ENGINE, TARGET_SCHEMA, TARGET_TABLE)


        print('FINISH')
    
    except Exception  as e:
        print(e)