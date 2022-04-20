import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from config import *


def read_from_table(mycursor, table_name, query):
    mycursor.execute(query)
    table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    return table


def read_all_table(mycursor, table_name):
    mycursor.execute(f"SELECT * FROM {table_name}")
    table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    return table


def update_record_to_sql(mydb, mycursor, table_name, col_name_to_set, value_to_update, index_cols: list,
                         index_values: list, only_print_query=False):
    query = f"UPDATE {table_name} set {col_name_to_set} = {value_to_update} WHERE "
    i = 0
    for index_col, index_value in zip(index_cols, index_values):
        if i > 0:
            query += " AND "
        if type(index_value) == dt.date:
            query += f"{index_col} = '{index_value}'"
        else:
            query += f"{index_col} = {index_value}"
        i += 1
    if only_print_query:
        print(query)
    else:
        mycursor.execute(query)
        mydb.commit()


def update_record_to_sql_many_values(mydb, mycursor, table_name, cols_name_to_set: list, values_to_update: list,
                                     index_cols: list,
                                     index_values: list, only_print_query=False):
    query = f"UPDATE {table_name} set "
    j = 0
    last_value = len(cols_name_to_set)
    for col_name_to_set, value_to_update in zip(cols_name_to_set, values_to_update):
        if j > 0:
            query += " , "
        query += f"{col_name_to_set} = {value_to_update}"
        j += 1

    query += ' WHERE '
    i = 0
    for index_col, index_value in zip(index_cols, index_values):
        if i > 0:
            query += " AND "
        if type(index_value) == dt.date:
            query += f"{index_col} = '{index_value}'"
        else:
            query += f"{index_col} = {index_value}"
        i += 1
    if only_print_query:
        print(query)
    else:
        mycursor.execute(query)
        mydb.commit()


def update_sql_table(table_name, df, index_col, if_exists='append'):
    engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}")
    # mycursor.execute(f"DROP TABLE {table_name}_test")
    df.set_index(index_col).to_sql(name=table_name, con=engine, if_exists=if_exists)
    # REMEMBERR!!! uncomment this section
    # mycursor.execute(f"DROP TABLE {table_name}")
    # df.set_index(index_col).to_sql(name=table_name,con=engine)
