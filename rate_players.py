import pandas as pd
import numpy as np
import datetime as dt
import mysql.connector
from config import *
from stqdm import stqdm
HOST = "hta-project.cf9mllj1rhry.us-east-2.rds.amazonaws.com"
USER = 'Sagi'
PASSWORD = "HTAproject2022"
DB = 'hta_project'
import time

now = dt.datetime.now().date()


def read_all_table(mycursor, table_name):
    mycursor.execute(f"SELECT * FROM {table_name}")
    table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    return table


def return_rank(row, weights):

    position = row[POSITION_COL]
    d_w_to_att = weights[['attribute', position]].set_index('attribute').to_dict()[position]

    sum = 0
    for k, v in d_w_to_att.items():
        if k not in d_w_to_att or k not in row:
            # print(k,"not in att_to_weight or not in row")
            continue
        if k in REDUCE_RATING_COLUMNS:
            pass
            #old version: sum -= row[k] * float(d_w_to_att[k])
        else:
            # try:
            sum += row[k] * float(d_w_to_att[k])
            # except:
            #     print(k)
            #     print(v)
            #     print(row[k])
            #     print(d_w_to_att[k])
            #     raise Exception('bla')
    return sum


def update_record_to_sql_many_values(mydb, mycursor, table_name, cols_name_to_set:list, values_to_update:list, index_cols: list,
                         index_values: list, only_print_query=False):
    query = f"UPDATE {table_name} set "
    j=0
    last_value = len(cols_name_to_set)
    for col_name_to_set, value_to_update in zip(cols_name_to_set,values_to_update):
        if j>0:
            query +=" , "
        query += f"{col_name_to_set} = {value_to_update}"
        j+=1


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

def normalization_x(x,mean_rank,std_rank):
    return (x-mean_rank)/std_rank

def get_normalized_ranks(df,weights_,col_to_set_new_rank,game_rank_col=None):
    # Set new rank according to the corresponding weights
    df[col_to_set_new_rank] = df.apply(lambda x: return_rank(x, weights_), axis=1)
    if game_rank_col!= None:
        df[game_rank_col] = df[col_to_set_new_rank]


    # Normalize new (by position)
    # ------------------------
    pos_to_mean_dict,pos_to_std_dict =  list(df.groupby('position').agg({col_to_set_new_rank:
                                                                             ['mean','std']}).to_dict().values())

    df[col_to_set_new_rank] = df.apply(lambda x: normalization_x(x[col_to_set_new_rank],
                                                                 pos_to_mean_dict[x['position']],
                                                                 pos_to_std_dict[x['position']]), axis=1)

    return df

def normalize_features_per_pos(df,pos,col_to_norm):
    df_position=df[df['position']==pos].copy()
    for col in col_to_norm:
        df_position[col]=(df_position[col] - df_position[col].mean()) / df_position[col].std()
    return df_position

def run_rate_players_app(rank_all_players = False,rank_likelihood = True):
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    all_games = read_all_table(mycursor, PLAYER_IN_GAME_TABLE)
    weights = read_all_table(mycursor, WEIGHTS_TABLE)
    likelihood_weights = read_all_table(mycursor, LIKELIHOOD_WEIGHTS_TABLE)

    weights.replace('Ñ\x81hances_created', 'сhances_created', inplace=True)
    if rank_all_players:
        all_games[GAME_RANK_COL] = None
    #todo: change the normalization over all ranks: all_gams[mean] all_gams[std]
    #todo: if we rank part of the games, apply standartization over all ranks

    null_game_rank_df = all_games[(all_games[POSTERIOR_RANK].isnull()) |
                                  (all_games[LIKELIHOOD_RANK].isnull()) |
                                  (all_games[GAME_RANK_COL].isnull())].copy()

    #normalize columns of performance in game
    col_to_norm = list(null_game_rank_df.drop(['player_id', 'game_date', 'opponent_id', 'position','date_downloaded'], axis=1).columns)
    normalize_df_list = []
    for pos in POSITIONS:
        normalize_df_list.append(normalize_features_per_pos(null_game_rank_df, pos,col_to_norm))
    normalize_df = pd.concat(normalize_df_list)

    # normalize ranks
    get_normalized_ranks(normalize_df,weights,POSTERIOR_RANK,GAME_RANK_COL)

    if rank_likelihood:
        get_normalized_ranks(normalize_df, likelihood_weights, LIKELIHOOD_RANK)
    print('Algorithm is done')
    data = []
    stmt = f"UPDATE {PLAYER_IN_GAME_TABLE} SET game_rank = %s , game_rank_posterior = %s , game_rank_likelihood = %s  WHERE player_id = %s AND game_date =  %s" #"INSERT INTO employees (first_name, hire_date) VALUES (%s, %s)"
    for i, row in stqdm(null_game_rank_df.iterrows(), total=null_game_rank_df.shape[0]):
        data.append((row[GAME_RANK_COL],row[POSTERIOR_RANK],row[LIKELIHOOD_RANK],row[PRIMARY_KEYS_PLAYER_IN_GAME[0]],str(row[PRIMARY_KEYS_PLAYER_IN_GAME[1]])))
        # update_record_to_sql_many_values(mydb, mycursor, table_name=PLAYER_IN_GAME_TABLE, cols_name_to_set=[GAME_RANK_COL,POSTERIOR_RANK,LIKELIHOOD_RANK],
        #                      values_to_update=[row[GAME_RANK_COL],row[POSTERIOR_RANK],row[LIKELIHOOD_RANK]], index_cols=PRIMARY_KEYS_PLAYER_IN_GAME,
        #                      index_values=row[PRIMARY_KEYS_PLAYER_IN_GAME].to_list(), only_print_query=True)
    print("writing to DB")
    start = time.time()
    mycursor.executemany(stmt, data)
    end = time.time()
    print(end - start)
    mydb.commit()
    mydb.close()
    return len(normalize_df)


