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


now = dt.datetime.now().date()

# # If config works - delete------------------
# POSITIONS = ["LD", "LM", "RM", "RD", "DM", "CM", "CD", "F"]
# LEAGUE_TABLE = 'league'
# PLAYER_IN_GAME_TABLE = 'player_in_game'
# WEIGHTS_TABLE = 'att_to_weight'
# LIKELIHOOD_WEIGHTS_TABLE ='att_to_weight_likelihood'
# TEAM_IN_LEAGUE_TABLE = 'team_in_league'
# GAME_RANK_COL = 'game_rank'
# PRIMARY_KEYS_PLAYER_IN_GAME = ["player_id","game_date"]
# POSITION_COL = 'position'
# POSTERIOR_RANK = 'game_rank_posterior'
# LIKELIHOOD_RANK = 'game_rank_likelihood'
# REDUCE_RATING_COLUMNS = ['red_cards']
#---------------------------------------------


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
            print(k,"not in att_to_weight or not in row")
            continue
        if k in REDUCE_RATING_COLUMNS:
            sum -= row[k] * float(d_w_to_att[k])
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
    # Normalize
    mean_rank, std_rank = df[col_to_set_new_rank].mean(), df[col_to_set_new_rank].std()
    df[col_to_set_new_rank] = df[col_to_set_new_rank].apply(
        lambda x: normalization_x(x, mean_rank, std_rank))
    return df



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
    # todo: rank all players with standatization by position!!

    null_game_rank_df = all_games[(all_games[POSTERIOR_RANK].isnull()) | (all_games[LIKELIHOOD_RANK].isnull()) | (all_games[GAME_RANK_COL].isnull())].copy()
    get_normalized_ranks(null_game_rank_df,weights,POSTERIOR_RANK,GAME_RANK_COL)

    if rank_likelihood:
        get_normalized_ranks(null_game_rank_df, likelihood_weights, LIKELIHOOD_RANK)
    print('Algorithm is done')
    for i, row in stqdm(null_game_rank_df.iterrows(), total=null_game_rank_df.shape[0]):
        update_record_to_sql_many_values(mydb, mycursor, table_name=PLAYER_IN_GAME_TABLE, cols_name_to_set=[GAME_RANK_COL,POSTERIOR_RANK,LIKELIHOOD_RANK],
                             values_to_update=[row[GAME_RANK_COL],row[POSTERIOR_RANK],row[LIKELIHOOD_RANK]], index_cols=PRIMARY_KEYS_PLAYER_IN_GAME,
                             index_values=row[PRIMARY_KEYS_PLAYER_IN_GAME].to_list(), only_print_query=False)
    mydb.close()
    return len(null_game_rank_df)



