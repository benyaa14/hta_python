import numpy as np
import mysql.connector
import update_weights as uw
from streamlit_gui import *
import rate_players as rp
import league_regression as lr
import transfermarket_request as tr
import json
from nav_bar import HEADER, CONTENT
from PIL import Image
from main_functions import *
from stqdm import stqdm
stqdm.pandas()
import streamlit.components.v1 as components
from fuzzywuzzy import fuzz
import requests
from bs4 import BeautifulSoup
import time
import collections
now = dt.datetime.now().date()

page_structure = [1, 3, 1]
MIN_PLAYER_MATCHING_SCORE = 60
SHLOMY_AZULAY_INDEX_P_ID = 267
INDICATION_FOR_MANUALLY_ADDED_PLAYER_NAME = 200
PROBLEMATIC_PLAYERS = [SHLOMY_AZULAY_INDEX_P_ID]

def fix_broken_file_name(player_file_name):
    """
    :param player_file_name: player in game excel file name that was uploaded to the system
    :return: new file name without special chars.

    some of the files downloaded with special chars and we want to remove them out to get the same file name structure for all files.
     """
    for c in ['$', '~']:
        player_file_name = str(player_file_name).replace(c, '').strip()
    return player_file_name


def split_player_name_from_file(player_file_name):
    """

    :param player_file_name: player in game excel file name that was uploaded to the system
    :return:player_name: The player's name
    Example player_file_name : 12_11_2021_R. Boakye.xlsx --> returns : R. Boakye
    """
    # TODO : '04_12_2021_I. N_Diaye.xlsx' this file can be one of the files we get. so... we need to change the logic over here to read player name in a better way
    player_name = str(player_file_name.split('_')[-1].split('.xlsx')[0])
    return player_name


def read_player_games(player_file_name):
    """
    This function reads excel file and returns it as pandas dataframe.
    :param player_file_name: player in game excel file name that was uploaded to the system
    :return: all_players_df : dataframe with player in game data.
    """
    try:
        all_players_df = pd.read_excel(player_file_name, engine='openpyxl')
    except:
        raise Exception(f"{player_file_name} dosen't exist")
    all_players_df['p_name'] = split_player_name_from_file(player_file_name.name)
    return all_players_df


def apply_filters_and_remove_nulls_and_chars(all_players_df):
    """
    The data we gather includes '-' as 0, total actions for each players, and numbers as strings.
    This function changes the old data types to the types in our database and drops the total actions which we don't want to save.
    In addition, players with instat_index = 0 probably didn't play enough minutes in the game so we would like to drop those rows too.
    :param all_players_df: dataframe with player in game data
    :return: filtered dataframe with player in game data
    """
    all_players_df = all_players_df.reset_index().drop(['index', 'Unnamed: 1'], axis=1)
    all_players_df = all_players_df.replace('-', 0)
    all_players_df = change_per_to_int(all_players_df)
    filter_conditions = (all_players_df['Opponent'] != 'Total') & (all_players_df['InStat Index'] > 0)
    all_players_df = all_players_df[filter_conditions].copy()
    return all_players_df


def change_per_to_int(all_players_df):
    """
    Takes df and transforms '%' columns to int

    :param all_players_df: dataframe with player in game data
    :return: transformed all_players_df
    """
    # Takes df and returns it '%' columns to int
    cols = list(all_players_df.columns)
    l_percent = []
    for i in cols:
        if '%' in str(i):
            l_percent.append(i)
    for col in l_percent:
        all_players_df[col] = all_players_df[col].apply(
            lambda x: int(str(x).replace('%', '')) if str(x) != 'nan' else x)
    return all_players_df


def change_column_names_for_coding(cols):
    """
    The player in game file is received with columns that are not fit to coding.
    this function will change the columns name so we could work with it
    :param cols: player in game columns
    :return: player in game columns for coding
    """
    new_cols = []
    for col in cols:
        col = col.replace(' ', '_')
        col = col.replace('%', 'per')
        col = col.lower()
        for c in [',', '/', '.', '(', ')', '-', "'"]:
            col = col.replace(c, '')
        new_cols.append(col)
    return new_cols


def return_date_as_dt(date):
    """
    Player in a game file includes 2 types of date structure (as strings):
        1. "dd/mm" for the current year
        2. "dd/mm/yy" for former years
    The function gets date as a string and returns it as datetime
    :param date: date as string data type
    :return: data as datetime data type
    """
    if len(date.split('/')) == 2:
        date = date + '/' + str(now.year)[-2:]
    date = date[0:6] + '20' + date[6:]
    date_time_obj = dt.datetime.strptime(date, '%d/%m/%Y').date()
    return date_time_obj


def create_game_result_columns(all_players_df):
    """
    The function will split the game score into 2 columns:
        gf = Goal For
        ga = Goal Against
    :param all_players_df: dataframe with player in game data
    :return: all_players_df: dataframe with player in game data that the score column was splited to gf, ga
    """
    all_players_df['gf'] = all_players_df['score'].apply(lambda x: (int(x.split(':')[0])))  #
    all_players_df['ga'] = all_players_df['score'].apply(lambda x: (int(x.split(':')[1])))  #
    all_players_df.drop('score', axis=1, inplace=True)


def read_many_tables_into_one(players_file_name_list):
    """
    This function will take list of excel files and will merge them into one player in game file that is ready to
    be uploaded to the DB
    :param players_file_name_list: list of the new excel files to upload
    :return: merged files as pandas dataframe
    """

    if len(players_file_name_list) > 0:
        for j, player_file_name in enumerate(players_file_name_list):
            # EXAMPLE player_file_name : '12_11_2021_R. Boakye.xlsx'
            # player_file_name = fix_broken_file_name(player_file_name)
            if j == 0:  # Read the first df
                all_players_df = read_player_games(player_file_name)
            else:  # Read all other dfs
                tmp_df = read_player_games(player_file_name)
                all_players_df = pd.concat([tmp_df, all_players_df])
    all_players_df = apply_filters_and_remove_nulls_and_chars(all_players_df).copy()
    all_players_df.columns = change_column_names_for_coding(list(all_players_df.columns))
    all_players_df['date_downloaded'] = now
    all_players_df['date'] = all_players_df['date'].apply(return_date_as_dt)
    all_players_df.rename({'date': 'game_date'}, axis=1, inplace=True)
    all_players_df[GAME_RANK_COL] = None
    all_players_df[LIKELIHOOD_RANK] = None
    all_players_df[POSTERIOR_RANK] = None

    create_game_result_columns(all_players_df)

    return all_players_df


def get_main_pos_and_positions_count(name: str, gb_player_position: pd.DataFrame, on_id=False):
    """

    :param name: player name
    :param gb_player_position:
    :param on_id:
    :return: 1. max_pos = main position for the player
              2. positions = position dict {position:count,...}
    """

    if on_id:
        player_rows = gb_player_position[gb_player_position['player_id'] == name].copy()
    else:
        player_rows = gb_player_position[gb_player_position['key'] == name].copy()

    player_rows.sort_values('per_pos', inplace=True)
    d_pos_to_count = player_rows[['position', 'count']].set_index('position').to_dict()['count']
    max_pos = player_rows['position'].iloc[-1]
    return max_pos, json.dumps(d_pos_to_count)


def count_games_in_pos(players_in_game, players_df, col_to_group_by='player_id'):
    sum_games = players_in_game[col_to_group_by].value_counts().to_dict()
    players = players_in_game.groupby([col_to_group_by, 'position'])['position'].size().to_frame().rename(
        {'position': 'count'}, axis=1).reset_index()
    players['per_pos'] = players.apply(lambda x: x['count'] / sum_games[x[col_to_group_by]], axis=1)
    unique_players = pd.DataFrame(players[col_to_group_by].unique(), columns=[col_to_group_by])
    unique_players[['position', 'all_positions']] = unique_players.apply(
        lambda x: get_main_pos_and_positions_count(x[col_to_group_by], players, on_id=True), axis=1,
        result_type='expand')

    return unique_players


def create_new_players_table(all_players_df):
    """
    Gets player in game file and doing the following steps:
        1.  Generate grouped by player dataframe with the columns:
            a) 'position': The player's main position
            b) 'all_positions': dictionary with all the positions the player played in and the number of games as value:
                                example: {'DM' : 32, 'CB' : 12,...}
            c) 'wage','nationality','market_value','p_rank','last_rank_date','DOB' : as None (to be added later in the process)
            d) 'last_game_date': the player's last game


    :param all_players_df: dataframe with player in game data
    :return: unique_players: grouped by player dataframe withe the relevant aggregated columns
    """
    all_players_df['key'] = all_players_df['p_name_transfer'].astype(str) + '||' + all_players_df['p_name'].astype(str)
    sum_games = all_players_df['key'].value_counts().to_dict()

    players = all_players_df.groupby(['key', 'position'])['position'].size().to_frame().rename({'position': 'count'},
                                                                                               axis=1).reset_index()
    players['per_pos'] = players.apply(lambda x: x['count'] / sum_games[x['key']], axis=1)
    unique_players = pd.DataFrame(players['key'].unique(), columns=['key'])
    unique_players[['position', 'all_positions']] = unique_players.apply(
        lambda x: get_main_pos_and_positions_count(x['key'], players), axis=1, result_type='expand')
    unique_players = unique_players.merge(all_players_df.groupby('key').agg({'p_name_transfer_matching_score':'first'}).reset_index()
                                          ,on='key',how='left')

    unique_players[['p_name_transfer', 'p_name_instat']] = unique_players.apply(lambda x: x['key'].split('||'),
                                                                                result_type='expand', axis=1)
    # unique_players['p_name_instat'] = unique_players['p_name_transfer'].apply(lambda x: json.dumps({i:1 for i in
    #                                                                                      list(all_players_df[all_players_df['p_name_transfer'] == x]['p_name'].unique() ) } ))
    unique_players['wage'] = None
    unique_players['nationality'] = None
    unique_players['market_value'] = None
    unique_players['last_game_date'] = unique_players['key'].apply(
        lambda x: all_players_df[all_players_df['key'] == x]['game_date'].max())
    unique_players['height'] = None
    unique_players['weight'] = None
    unique_players['foot'] = None
    unique_players['p_rank'] = None
    unique_players['last_rank_date'] = None
    unique_players['DOB'] = None

    return unique_players


def assign_id_to_new_players(last_id, temp_players_df,
                             players_name_to_assign_new_id):  # Test: add players that exsiting in the system and check the results
    """

    :param last_id: the last player id in the system
    :param temp_players_df: aggregated player df (in the process not the one that is currently in the db)
    :param players_name_to_assign_new_id: The new players' name to assign id to.
    :return: players_to_add : player agg dataframe only with the new players to insert to the DB
    """

    new_ids = np.arange(last_id + 1, last_id + 1 + len(players_name_to_assign_new_id))
    players_to_add = temp_players_df[temp_players_df['key'].isin(players_name_to_assign_new_id)].copy()
    players_to_add['p_id'] = new_ids
    return players_to_add


# def append_all_positions_counter_to_old_players(players_table, players_in_game):
#   # players_in_game
#   pass

def get_team_id_from_team_name(team_name, df_teams):
    t_id = df_teams[df_teams['t_name_transfer'] == team_name]['t_id']
    if len(t_id) == 0:
        t_id = df_teams[df_teams['t_name_transfer'] == team_name]['t_id']
    t_id = t_id.iloc[0]
    return t_id


def get_player_id_from_player_name(player_name, df_player):
    return df_player[df_player['key'] == player_name]['p_id'].iloc[0]


def append_players_table_with_new_players(mycursor, player_table_name, temp_players_df, players_in_game):
    """
    The function gets new aggregated player df and compares it with the players table in the DB.
    It will the the following steps:
        1. Find the new players assign new id for each one of them and append the old players dataframe
        2. Assign to the players_in_game df the player id instead of its name
        3. Drop the player name from players_in_game

    :param mycursor: DB cursor
    :param player_table_name: players table name in the DB
    :param temp_players_df: grouped by player df, with the relevant columns
    :param players_in_game: dataframe with player in game data
    :return: new_player_table: agg player df with the new players
            players_in_game: updated players_in_game file (with player id instead of player name)
            players_to_add : list of player ids to insert to the system
    """
    # Find the new players to add to the DB
    mycursor.execute(f"SELECT * FROM {player_table_name}")
    players_table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)

    players_table['key'] = players_table['p_name_transfer'].astype(str) + '||'+ players_table['p_name_instat'].astype(str)

    new_players_name = set(temp_players_df['key'].unique())
    old_players_name = set(players_table['key'].unique())

    players_name_to_assign_new_id = list(
        new_players_name - old_players_name)
    existing_name_in_the_table = new_players_name & old_players_name

    # Assign id the the new players
    if len(players_table) == 0 :
        players_to_add = assign_id_to_new_players(0, temp_players_df,
                                                  players_name_to_assign_new_id)

    else:
        players_to_add = assign_id_to_new_players(players_table['p_id'].max(), temp_players_df,
                                                  players_name_to_assign_new_id)
    new_player_table = pd.concat([players_table, players_to_add])

    # Assign to the players_in_game df the player id instead of its name
    players_in_game['player_id'] = players_in_game['key'].apply(
        lambda x: get_player_id_from_player_name(x, new_player_table))
    players_in_game.drop(['p_name','p_name_transfer','p_name_transfer_matching_score','key'], axis=1, inplace=True)
    new_player_table.drop('key',axis=1,inplace=True)
    return new_player_table, players_in_game, players_to_add['p_id'].to_list()
    # update_sql_table('player',df)


def assign_id_to_new_teams(last_id, temp_df, names_to_assign_id_list, id_col):
    new_ids = np.arange(last_id + 1, last_id + 1 + len(names_to_assign_id_list))
    temp_df[id_col] = new_ids

def update_the_count_of_matched_games_per_team(t_name_transfer,instat_db_dict_as_serie,opponent_name_instat_list,cnt_matches_per_opponent_team_series):
    d_cnt_matches = dict()
    if len(instat_db_dict_as_serie) > 0 :
        instat_db_dict =  json.loads(instat_db_dict_as_serie.iloc[0])
    else :
        instat_db_dict = dict()
    for team_name_instat in opponent_name_instat_list:
        cnt_matches = instat_db_dict.get(team_name_instat,0) + cnt_matches_per_opponent_team_series.loc[t_name_transfer]
        d_cnt_matches[team_name_instat] = cnt_matches
    return json.dumps(d_cnt_matches)


def get_team_table_from_the_new_data(all_players_in_game):
    gb_o_transfer = all_players_in_game.groupby(['o_team_transfer']).agg({'opponent': 'value_counts'}).rename(
        {"opponent": 'instat_value_counts'}, axis=1).reset_index().rename(
        {"o_team_transfer": "t_name_transfer", 'opponent': 't_name_instat'}, axis=1)
    missing_teams = set(all_players_in_game['t_name_transfer'].unique()) - set(
        gb_o_transfer['t_name_transfer'].to_list())
    gb_t_transfer = pd.concat([gb_o_transfer, pd.DataFrame(missing_teams, columns=['t_name_transfer'])])
    gb_t_transfer = gb_t_transfer.groupby('t_name_transfer').agg({'t_name_instat': list, 'instat_value_counts': list})
    gb_t_transfer['t_name_instat_'] = gb_t_transfer.apply(
        lambda x: {x['t_name_instat'][i]: x['instat_value_counts'][i] for i in range(len(x['t_name_instat'])) if
                   str(x['t_name_instat'][i]) != 'nan'}, axis=1)
    team_table_from_the_new_data = gb_t_transfer.drop(['t_name_instat', 'instat_value_counts'], axis=1).rename(
        {'t_name_instat_': 't_name_instat_new'}, axis=1)
    return team_table_from_the_new_data


def get_updated_instat_name(db_table, new_table_from_data):
    team_table_to_update = db_table.merge(new_table_from_data, on='t_name_transfer', how='right')
    team_table_to_update['t_name_instat'] = team_table_to_update.apply(
        lambda x: get_new_name_instat_dict(x['t_name_instat_old'], x['t_name_instat_new']), axis=1)
    team_table_to_update = team_table_to_update[
        team_table_to_update['t_name_instat_old'].eq(team_table_to_update['t_name_instat']) == False].copy()
    team_table_to_update.drop(['t_name_instat_old', 't_name_instat_new'], axis=1, inplace=True)
    return team_table_to_update


def assign_new_ids(df_in_the_db, new_updated_df, id_col):
    if len(df_in_the_db) == 0:
        new_first_id_to_assign = 0
    else:
        new_first_id_to_assign = df_in_the_db[id_col].max() + 1
    new_teams_transfer_name = new_updated_df[new_updated_df[id_col].isnull()]['t_name_transfer'].to_list()
    new_updated_df = new_updated_df.reset_index()
    new_updated_df.drop('index', axis=1, inplace=True)
    if len(new_teams_transfer_name):
        indx_to_assign = new_updated_df[new_updated_df['t_name_transfer'].isin(new_teams_transfer_name)].index
        new_ids = np.arange(new_first_id_to_assign, new_first_id_to_assign + len(indx_to_assign))
        new_updated_df.loc[indx_to_assign, id_col] = new_ids
    newly_added_teams = new_updated_df[new_updated_df['t_name_transfer'].isin(new_teams_transfer_name)].copy()
    return new_updated_df, newly_added_teams


def get_new_name_instat_dict(d_names_old, d_names_new):
    if str(d_names_old) == 'nan':
        d_names_old = dict()
    if str(d_names_new) == 'nan':
        d_names_new = dict()
    counter = collections.Counter()
    names_dicts = [d_names_old, d_names_new]
    for d in names_dicts:
        counter.update(d)
    res = dict(counter)
    return res


def get_all_new_team_table(new_updated_df, team_table):
    updated_ids = list(new_updated_df['t_id'].unique())
    team_table = team_table[team_table['t_id'].isin(updated_ids) == False].copy()
    return pd.concat([team_table, new_updated_df]).drop(['t_name_instat_old'], axis=1)

def create_new_team_table(team_table_name, players_in_game):
    """
     The func will do following steps:
        1. Find the new teams, assign new id for each one of them and append the old teams dataframe
        2. Assign to the players_in_game df the team id instead of the opponent team name
        3. Drop the opponent team name from players_in_game
    :param mycursor: DB cursor
    :param team_table_name: teams table name in the DB
    :param players_in_game: dataframe with player in game data
    :return:
    """
    mydb, mycursor = connect_to_the_DB()
    mycursor.execute(f"SELECT * FROM {team_table_name}")
    team_table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names).rename({"t_name_instat":"t_name_instat_old"},axis=1)
    team_table['t_name_instat_old'] = team_table['t_name_instat_old'].apply(lambda x: json.loads(x))
    team_table_from_the_new_data = get_team_table_from_the_new_data(players_in_game)
    df_teams_updated_instat_name = get_updated_instat_name(team_table,team_table_from_the_new_data)
    df_teams_updated_instat_name['t_name_instat'] = df_teams_updated_instat_name['t_name_instat'].apply(lambda x:json.dumps(x))

    new_updated_df,newly_added_teams = assign_new_ids(team_table,df_teams_updated_instat_name,'t_id')
    all_new_team_table = get_all_new_team_table(new_updated_df, team_table)

    players_in_game['opponent_id'] = players_in_game['o_team_transfer'].apply(
        lambda x: get_team_id_from_team_name(x, all_new_team_table))
    players_in_game['t_id'] = players_in_game['t_name_transfer'].apply(
        lambda x: get_team_id_from_team_name(x, all_new_team_table))
    players_in_game.drop(['o_team_transfer', 't_name_transfer','opponent'], axis=1, inplace=True)
    disconnect_from_the_db(mycursor, mydb)
    return players_in_game, newly_added_teams,new_updated_df


def update_sql_table(table_name, df, index_col):
    engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}")
    # mycursor.execute(f"DROP TABLE {table_name}_test")
    df.set_index(index_col).to_sql(name=table_name, con=engine, if_exists='append')
    # REMEMBERR!!! uncomment this section
    # mycursor.execute(f"DROP TABLE {table_name}")
    # df.set_index(index_col).to_sql(name=table_name,con=engine)


# def read_all_table(mycursor, table_name):
#     mycursor.execute(f"SELECT * FROM {table_name}")
#     table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
#     return table
#
#
# def update_record_to_sql(mydb, mycursor, table_name, col_name_to_set, value_to_update, index_cols: list,
#                          index_values: list, only_print_query=False):
#     query = f"UPDATE {table_name} set {col_name_to_set} = {value_to_update} WHERE "
#     i = 0
#     for index_col, index_value in zip(index_cols, index_values):
#         if i > 0:
#             query += " AND "
#         if type(index_value) == dt.date:
#             query += f"{index_col} = '{index_value}'"
#         else:
#             query += f"{index_col} = {index_value}"
#         i += 1
#     if only_print_query:
#         print(query)
#     else:
#         mycursor.execute(query)
#         mydb.commit()


def update_position_and_all_position_columns(mydb, mycursor, new_player_table_updated_positions_count, new_player_table,
                                             new_players_to_add_ids_list):
    old_players_with_new_count = new_player_table_updated_positions_count[
        (new_player_table_updated_positions_count['p_id'].isin(new_players_to_add_ids_list) == False) &
        (new_player_table_updated_positions_count['all_positions'].notnull())]
    old_players_with_old_count = new_player_table[new_player_table['p_id'].isin(new_players_to_add_ids_list) == False]

    columns_to_update_in_player_table = old_players_with_new_count[
        old_players_with_new_count['all_positions'].apply(lambda x: sorted(x)).eq(
            old_players_with_old_count['all_positions'].apply(lambda x: sorted(x))) == False][
        ['p_id', 'all_positions', 'position']]
    if len(columns_to_update_in_player_table) > 0:
        for i, row in columns_to_update_in_player_table.iterrows():
            for col_to_update in ['all_positions', 'position']:
                update_record_to_sql(mydb, mycursor, table_name=PLAYER_TABLE, col_name_to_set=col_to_update,
                                     value_to_update=json.dumps(row[col_to_update]), index_cols=['p_id'],
                                     index_values=[row['p_id']], only_print_query=False)


def create_new_player_in_game_df(all_players_in_game_df, player_in_game_table_name='player_in_game'):
    """

    :param all_players_in_game_df: dataframe with player in game data
    :param player_in_game_table_name: player in game table name in the DB
    :return: new_player_in_game: new player in game dataframe that will be in the DB after it will be updated
            old_players_in_game: the old table of players_in_game that is currently in the DB
    """
    mydb , mycursor = connect_to_the_DB()
    old_players_in_game = read_all_table(mycursor, player_in_game_table_name)
    new_player_in_game = pd.concat([old_players_in_game, all_players_in_game_df])
    new_player_in_game = new_player_in_game[
        new_player_in_game.duplicated(subset=['player_id', 'game_date'], keep='first') == False].copy()

    return new_player_in_game, old_players_in_game


def get_page_as_soup(page):
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    pageTree = requests.get(page, headers=headers)
    if pageTree.ok == False:
        raise Exception(f"You try to reach this page:{page}\n but there is no response from the website")
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    return pageSoup


def get_all_games_df(all_games_on_date_soup, date):
    res = all_games_on_date_soup.find_all('tr', {'class': 'begegnungZeile'})
    df_matches_in_day = pd.DataFrame(columns=['home', 'score', 'away'])
    for match in res:
        df_matches_in_day = df_matches_in_day.append(
            pd.read_html("<table>" + str(match) + "</table>")[0][[4, 5, 6]].rename({4: 'home', 5: 'score', 6: 'away'},
                                                                                   axis=1))
        # Save df_matches_in_day to a DB table if it doesn't exist
    mapping_short_to_long_team_name = map_team_short_name_to_long_name(all_games_on_date_soup)
    df_matches_in_day['home'] = df_matches_in_day['home'].apply(lambda x: mapping_short_to_long_team_name.get(x))
    df_matches_in_day['away'] = df_matches_in_day['away'].apply(lambda x: mapping_short_to_long_team_name.get(x))
    df_matches_in_day['date'] = date
    return df_matches_in_day


def get_list_of_team_candidates(opt_game_results, df_games_in_day):
    filtered_df = df_games_in_day[df_games_in_day['score'].isin(opt_game_results)]
    candidates = set(list(filtered_df['home'].unique()) + list(filtered_df['away'].unique()))
    return list(candidates)


def find_the_best_candidate(candidates, opponent_name):
    cand_to_score = dict()
    for candidate in candidates:
        cand_to_score[candidate] = fuzz.ratio(candidate, opponent_name)
        # cand_to_score[candidate] = fuzz.UWRatio(candidate, opponent_name)

    max_value = max(cand_to_score, key=cand_to_score.get)
    return max_value, cand_to_score[max_value]


def find_players_team(opponent_team_name_transfer_market, df_games_in_day):
    cand = list(set(df_games_in_day[(df_games_in_day['home'] == opponent_team_name_transfer_market) |
                                    (df_games_in_day['away'] == opponent_team_name_transfer_market)][
                        ['home', 'away']].iloc[0].to_list()) -
                {opponent_team_name_transfer_market})
    if len(cand) == 0:
        print("didn't find cand")
    return cand[0]


def map_team_short_name_to_long_name(all_games_on_date_soup):
    all_a = all_games_on_date_soup.find_all('a')
    d_short_to_long = dict()
    for a in all_a:
        try:
            d_short_to_long[a.text] = a['title']
        except:
            d_short_to_long[a.text] = None
    return d_short_to_long


def date_check(date, function='get_transfermarket_team_name'):
    if function == 'get_transfermarket_team_name':
        if type(date) not in [dt.date] or date > dt.datetime.now().date():
            raise Exception(
                f"get_transfermarket_team_name2 --> date error. you have added this value: {date} as a date")


# find the minimum amount of days to read
def read_games_on_date_from_transfermarket_and_update_the_table(date):
    date_check(date, function='get_transfermarket_team_name')
    page = f"https://www.transfermarkt.co.uk/aktuell/waspassiertheute/aktuell/new/datum/{str(date)}?land=&art="
    all_games_on_date_soup = get_page_as_soup(page)
    df_games_in_day = get_all_games_df(all_games_on_date_soup, date)
    update_sql_table(table_name='game_on_date', df=df_games_in_day, index_col=['date', 'home'])
    return len(df_games_in_day)


def get_games_on_date_as_df(date, mycursor, mydb):
    date_check(date, function='get_transfermarket_team_name')
    # if it doesn't exist in the DB --> read it from transfermarket
    query = f"select * from game_on_date where date = '{date}'"
    df_game_on_date = read_from_table(mycursor, PLAYER_IN_GAME_TABLE, query)
    if len(df_game_on_date) == 0:  # read the games df from transfermarket
        new_rows = read_games_on_date_from_transfermarket_and_update_the_table(date)
        if new_rows > 0:
            mydb.close()
            mydb = mysql.connector.connect(
                host=HOST, user=USER, password=PASSWORD, database=DB
            )
            mycursor = mydb.cursor()
            df_game_on_date = read_from_table(mycursor, PLAYER_IN_GAME_TABLE, query)
            mydb.close()
    return df_game_on_date


def get_transfermarket_team_name(date: dt.datetime, opponent_name: str, gf: int, ga: int):
    # connection to the DB
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    # Check that the date is in good format
    date_check(date, function='get_transfermarket_team_name')

    opt_game_results = [f"{str(int(gf))}:{str(int(ga))}", f"{str(int(ga))}:{str(int(gf))}"]

    df_games_in_day = get_games_on_date_as_df(date, mycursor, mydb)

    candidates = get_list_of_team_candidates(opt_game_results, df_games_in_day)
    if len(candidates) == 0:
        # Didn't find any team to match
        return None,None,None


    opponent_team_name_transfer_market, score = find_the_best_candidate(candidates, opponent_name)

    team = find_players_team(opponent_team_name_transfer_market, df_games_in_day)

    return team, opponent_team_name_transfer_market, score


def get_season_from_date(date):
    date_check(date)
    if 8 <= date.month <= 12:
        return date.year
    return date.year - 1


def change_team_format_to_read_in_transfermarket(team):
    # change team name to url string : 'hapoel tel' --> 'hapoel+tel'
    team = team.replace('-', ' ')
    team_str = team.lower().replace(' ', '+')
    return team_str


def get_list_of_teams(pageSoup):
    teams = pageSoup.find_all("td", {"class": "hauptlink"})
    idx_to_remove = 0
    for i, team_ in enumerate(teams):
        if "verein" not in str(team_):  # Indicates that this is a team
            idx_to_remove = i + 1
        else:
            break

    teams = teams[idx_to_remove:]
    return teams


def get_list_of_players(pageSoup):
    full_name_list = []
    tables = pageSoup.find_all('table', {'class': 'items'})
    if len(tables) > 0:
        players = pd.read_html(str(tables))
        for player in players[1:]:
            full_name_list.append(player.iloc[0][1])
    return full_name_list


def get_player_list_as_df(team, season, players_name_list):
    df_players = pd.DataFrame(players_name_list, columns=['p_name'])
    df_players['team'] = team
    df_players['season'] = season
    return df_players


def get_team_squad_within_season_from_transfermarket(team, season):
    team_str = change_team_format_to_read_in_transfermarket(team)
    page = f"https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query={team_str}"
    pageSoup = get_page_as_soup(page)
    team_list = get_list_of_teams(pageSoup)
    if len(team_list) > 0:
        teams_url = get_url_from_soup_list(team_list[:1])
        if len(teams_url) > 0:
            # https://www.transfermarkt.co.uk/hapoel-tel-aviv/startseite/verein/1017?saison_id=2020 --> https://www.transfermarkt.co.uk/hapoel-tel-aviv/kader/verein/1017/saison_id/2022/plus/1
            pageSoup = get_page_as_soup(f"{teams_url[0].replace('startseite', 'kader')}/saison_id/{season}/plus/1")
            players_name_list = get_list_of_players(pageSoup)
            if len(players_name_list) > 0:
                df_team = get_player_list_as_df(team, season, players_name_list)
                return df_team
            else:
                return "No players for the team"
        else:
            return "No url to the team"
    else:
        return "No teams to match"


def get_url_from_soup_list(soup_list):
    link_list = []
    for link in soup_list:  # each record = <td class="hauptlink"><a href="/fc-paris-saint-germain/startseite/verein/583" title="Paris Saint-Germain">Paris Saint-Germain</a></td>
        link_list.append("http://www.transfermarkt.co.uk" + link.find_all('a')[0]['href'])
    return link_list


def get_squad_in_season_as_df(season, team_name_transfer, mycursor, mydb):
    # if it doesn't exist in the DB --> read it from transfermarket
    query = f"select * from squad_in_season_transfer where season = {season} and team = '{team_name_transfer}' "
    df_team_in_season = read_from_table(mycursor, PLAYER_IN_GAME_TABLE, query)
    if len(df_team_in_season) == 0:  # read the team df from transfermarket

        df_team_in_season = get_team_squad_within_season_from_transfermarket(team_name_transfer, season)
        if type(df_team_in_season) == str:
            st.error(f"{df_team_in_season} = {team_name_transfer}")
        else:
            update_sql_table(table_name='squad_in_season_transfer', df=df_team_in_season, index_col=['p_name', 'season'])
            # update db
            if len(df_team_in_season) > 0:
                mydb.close()
                mydb = mysql.connector.connect(
                    host=HOST, user=USER, password=PASSWORD, database=DB
                )
                mycursor = mydb.cursor()
                df_team_in_season = read_from_table(mycursor, PLAYER_IN_GAME_TABLE, query)
                mydb.close()

    return df_team_in_season


def match_instat_player_name_to_transfermarket(p_name, team_transfer, year):
    player_transfer_name = matching_score = None
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    df_team = get_squad_in_season_as_df(year, team_transfer, mycursor, mydb)
    if len(df_team) > 0 and type(df_team)!=str:
        player_transfer_name, matching_score = find_the_best_candidate(list(df_team['p_name'].to_list()), p_name)
    return player_transfer_name, matching_score

def get_set_teams_without_match_to_league(df_pig,til_df):
    mydb,mycursor = connect_to_the_DB()
    all_teams_in_the_system_set = set(
        list(df_pig['t_id'].unique()) + list(df_pig['opponent_id'].unique()))
    team_ids_in_til_table = set(til_df['t_id'].unique())
    team_ids_without_league_in_til = all_teams_in_the_system_set - team_ids_in_til_table
    disconnect_from_the_db(mycursor,mydb)
    return team_ids_without_league_in_til

def get_set_teams_without_match_to_league_in_the_current_season(til_df):
    current_season = get_season_from_date(dt.date(dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day))
    t_id_max_season = til_df.groupby('t_id')['season_y'].max()
    t_id_to_update_season = set(t_id_max_season[t_id_max_season < current_season ].index)
    return t_id_to_update_season

def find_between_2_teams_the_closest_and_get_matching_score(opponent,home,away):
    opponent,home,away = str(opponent),str(home),str(away)
    score_home = np.array([fuzz.ratio(opponent, home),fuzz.partial_ratio(opponent, home),fuzz.QRatio(opponent, home),fuzz.UWRatio(opponent, home)
                              ,fuzz.UQRatio(opponent, home)]).mean()

    score_away = np.array([fuzz.ratio(opponent, away),fuzz.partial_ratio(opponent, away),fuzz.QRatio(opponent, away),fuzz.UWRatio(opponent, away)
                              ,fuzz.UQRatio(opponent, away)]).mean()

    # score_home = fuzz.UWRatio(str(opponent),str(home))
    # score_away = fuzz.UWRatio(str(opponent),str(away))

    if score_home > score_away:
        return [home,score_home,away]
    else:
        return [away,score_away,home]

def get_best_match_from_the_existing_game_on_date(all_players_in_game):
    all_players_in_game_df = all_players_in_game.copy()
    mydb, mycursor = connect_to_the_DB()
    game_on_date_df = read_all_table(mycursor,GAME_ON_DATE)
    all_players_in_game_df['score'] = (all_players_in_game_df['gf'].astype(str) + ':' + all_players_in_game_df['ga'].astype(str)).apply(lambda x: str(x).strip())
    game_on_date_df['opposite_score'] = game_on_date_df['score'].apply(lambda x: x[::-1].strip())

    joined_by_score_date = all_players_in_game_df.merge(game_on_date_df,how='inner',left_on=['game_date','score'], right_on=['date','score'])
    joined_by_score2_date = all_players_in_game_df.merge(game_on_date_df,how='inner',left_on=['game_date','score'], right_on=['date','opposite_score'])
    joined_by_score_date = pd.concat([joined_by_score_date,joined_by_score2_date])[['game_date','score','opposite_score','opponent','home','away']]
    joined_by_score_date[['o_team_transfer','matching_score_o_team','t_name_transfer']] = joined_by_score_date.progress_apply(lambda x:find_between_2_teams_the_closest_and_get_matching_score(x['opponent'],x['home'],x['away']),
                                                                                                                                                                axis=1,result_type= 'expand')



    joined_by_score_date.sort_values(['game_date','opponent','matching_score_o_team'],inplace=True)
    df_game_date_to_opponent = joined_by_score_date.groupby(['game_date','opponent']).agg({'o_team_transfer':'last',
                                                                                           'matching_score_o_team':'last',
                                                                                           't_name_transfer':'last'
                                                                                           })
    all_players_in_game_with_teams = all_players_in_game.merge(df_game_date_to_opponent, on=['game_date','opponent'], how = 'left')

    return all_players_in_game_with_teams


def get_best_match_from_the_existing_squad_in_year(unique_p):
    unique_p_df = unique_p.copy()
    mydb, mycursor = connect_to_the_DB()
    squad_in_season = read_all_table(mycursor,SQUAD_IN_SEASON)
    unique_p_df['season'] = unique_p_df['game_date'].apply(get_season_from_date)

    joined_by_season_and_team = unique_p_df[['p_name','t_name_transfer','season']].merge(squad_in_season.rename({'p_name':'p_name_transfer'},axis=1),left_on = ['t_name_transfer','season'],right_on = ['team','season'],how='inner')

    if len(joined_by_season_and_team)==0:
        unique_p_df['p_name_transfer'] = None
        unique_p_df['p_name_transfer_matching_score'] = None
        unique_p_df['p_name_transfer'] = None
        return unique_p_df
    joined_by_season_and_team['p_name_transfer_matching_score'] = joined_by_season_and_team.progress_apply(lambda x: np.array([fuzz.ratio(x['p_name'],x['p_name_transfer']),fuzz.partial_ratio(x['p_name'],x['p_name_transfer']),
                                                                                                                        fuzz.QRatio(x['p_name'],x['p_name_transfer']),
                                                                                                                        fuzz.UWRatio(x['p_name'],x['p_name_transfer']),
                                                                                                                        fuzz.UQRatio(x['p_name'],x['p_name_transfer'])
                                                                                                                            ]).mean(),axis=1)

    joined_by_season_and_team.sort_values(['p_name','p_name_transfer_matching_score'],inplace=True)
    df_player_to_team_player = joined_by_season_and_team.groupby(['p_name']).agg({'p_name_transfer':'last',
                                                                                           'p_name_transfer_matching_score':'last'
                                                                                           })
    unique_player_with_name = unique_p_df.merge(df_player_to_team_player, on='p_name', how = 'left')
    disconnect_from_the_db(mycursor,mydb)
    return unique_player_with_name

def drop_existing_raw_player_in_game(all_players_in_game_raw_new_df,update_db=True):
    mydb,mycursor = connect_to_the_DB()
    all_players_in_game_raw_old_df = read_all_table(mycursor,PLAYER_IN_GAME_RAW_TABLE)
    disconnect_from_the_db(mycursor,mydb)
    duplicate_raw_data = all_players_in_game_raw_old_df.merge(all_players_in_game_raw_new_df[['game_date', 'p_name']],
                                                              on=['game_date', 'p_name'], how='inner')
    duplicate_raw_data['key'] = duplicate_raw_data['p_name'] + '+' + duplicate_raw_data['game_date'].astype(str)
    all_players_in_game_raw_new_df['key'] = all_players_in_game_raw_new_df['p_name'] + '+' + \
                                            all_players_in_game_raw_new_df['game_date'].astype(str)
    all_players_in_game_df_droped_duplicates = all_players_in_game_raw_new_df[
        all_players_in_game_raw_new_df['key'].isin(duplicate_raw_data['key'].to_list()) == False].drop('key', axis=1)
    if update_db:
        update_sql_table(PLAYER_IN_GAME_RAW_TABLE,all_players_in_game_df_droped_duplicates,['p_name'])
    return all_players_in_game_df_droped_duplicates

def run(players_file_name_list, mydb, mycursor, update_teams_and_leagues=False, update_db=False):
    """
    The function get the excel files doing the following steps:
        1. Read the files to a dataframe
        2. Find the new players that aren't exist in the DB and assign new player id for them
        3. Find the new players that aren't exist in the DB and assign new player id for them
        4. Update 'main position' and 'positions' dict in player table
        5. Update tables: teams, players, player in game,
    :param mydb:
    :param players_file_name_list: List of excel files from uploaded via the GUI
    :param mycursor: cursor of the DB
    :param update_db: True = write to the DB; False = Run and don't write anything to the DB
    """
    st.info('Uploading files to the DB')
    startt = time.time()
    # all_players_in_game_df_raw = pd.read_csv(players_file_name_list[0])[:].copy() # todo: delete later
    # all_players_in_game_df_raw['game_date'] = all_players_in_game_df_raw['game_date'].apply(lambda x: dt.date(int(x.split('/')[2]), int(x.split('/')[1]),int(x.split('/')[0])))# todo: delete later
    all_players_in_game_df_raw = read_many_tables_into_one(players_file_name_list) # todo uncomment later
    # all_players_in_game_df = all_players_in_game_df_raw.copy()#drop_existing_raw_player_in_game(all_players_in_game_df_raw)
    all_players_in_game_df = drop_existing_raw_player_in_game(all_players_in_game_df_raw,update_db=False)
    # all_players_in_game_df_raw.to_csv('check_all_players_in_game_df_raw.csv')
    # all_players_in_game_df.to_csv('check_all_players_in_game_df.csv')

    if len(all_players_in_game_df)==0:
        st.error("All raw data is already exist in the database")
        return

    # ========== 1) add transfer market team names for each game =======================================================
    all_players_in_game_df = get_best_match_from_the_existing_game_on_date(all_players_in_game_df)
    all_players_in_game_df_missing_transfer_teams = all_players_in_game_df[all_players_in_game_df['o_team_transfer'].isnull()].copy()
    all_players_in_game_df = all_players_in_game_df[all_players_in_game_df['o_team_transfer'].notnull()].copy()
    if len(all_players_in_game_df_missing_transfer_teams) > 0:
        all_players_in_game_df_missing_transfer_teams[['t_name_transfer', 'o_team_transfer', 'matching_score_o_team']] \
            = all_players_in_game_df_missing_transfer_teams.progress_apply(lambda x:get_transfermarket_team_name(x['game_date'],x['opponent'],int(x['gf']),int(x['ga'])),result_type='expand',axis=1)
        all_players_in_game_df = pd.concat([all_players_in_game_df
                                               ,all_players_in_game_df_missing_transfer_teams[all_players_in_game_df_missing_transfer_teams['o_team_transfer'].notnull()]])
    all_players_in_game_df_missing_transfer_teams = all_players_in_game_df_missing_transfer_teams[
        all_players_in_game_df_missing_transfer_teams['o_team_transfer'].isnull()]

                # ========== Extract relevant dataframes for player matching step ===========
    match_player_to_team_missing = all_players_in_game_df_missing_transfer_teams.groupby('p_name').agg({'t_name_transfer': 'last'})

    match_player_to_team = all_players_in_game_df.groupby('p_name').agg({'t_name_transfer': 'value_counts'}).rename(
        {'t_name_transfer': 'count'}, axis=1).reset_index().sort_values('count').groupby('p_name').agg(
        {'t_name_transfer': 'last'})
    unique_players_df = match_player_to_team.reset_index().merge(
        all_players_in_game_df[['p_name', 't_name_transfer', 'game_date']], on=['p_name', 't_name_transfer'],
        how='left').drop_duplicates(subset=['p_name', 't_name_transfer'], keep='last')
    unique_players_df['season'] = unique_players_df['game_date'].apply(get_season_from_date)

                # ========== =================================================================== ===========
                # ============================== update team table ========================================
    all_players_in_game_df, new_teams_table,team_df_columns_to_update = create_new_team_table(TEAM_TABLE_NAME,
                                                                    all_players_in_game_df)

    # ==============================================================================================================================


    #============================== select unique players and find their name in transfermarket ==============================


    unique_players_df = get_best_match_from_the_existing_squad_in_year(unique_players_df)
    unique_players_df_tag_manually_transfer_players = unique_players_df[unique_players_df['p_name_transfer_matching_score']<MIN_PLAYER_MATCHING_SCORE]

    unique_players_df_missing_transfer_players = unique_players_df[unique_players_df['p_name_transfer'].isnull()].copy()

    unique_players_df = unique_players_df[(unique_players_df['p_name_transfer'].notnull())].copy()


    if len(unique_players_df_missing_transfer_players) >0:
        unique_players_df_missing_transfer_players[['p_name_transfer', 'p_name_transfer_matching_score']] = unique_players_df_missing_transfer_players.progress_apply(lambda x:
                                                                                                          match_instat_player_name_to_transfermarket(
                                                                                                              x['p_name'],x['t_name_transfer'],x['season']),
                                                                                                                                                                      result_type='expand', axis=1
                                                                                                                                                                      )
        unique_players_df = pd.concat([unique_players_df, unique_players_df_missing_transfer_players])
    unique_players_df_missing_transfer_players = unique_players_df_missing_transfer_players[unique_players_df_missing_transfer_players['p_name_transfer'].isnull()]


    unique_players_df_missing_teams = match_player_to_team_missing.reset_index().merge(
        all_players_in_game_df_missing_transfer_teams[['p_name', 't_name_transfer', 'game_date']], on=['p_name', 't_name_transfer'],
        how='left').drop_duplicates(subset=['p_name', 't_name_transfer'], keep='last')


    problematic_players_df = pd.concat([unique_players_df_tag_manually_transfer_players,unique_players_df_missing_transfer_players,unique_players_df_missing_teams])

    all_players_in_game_df = all_players_in_game_df.merge(unique_players_df[['p_name','p_name_transfer','p_name_transfer_matching_score']],on = 'p_name', how = 'left')
    problematic_player_in_game = pd.concat([
        all_players_in_game_df[all_players_in_game_df['p_name'].isin(list(problematic_players_df['p_name'].unique()))]
        , all_players_in_game_df_missing_transfer_teams])

    players_df = create_new_players_table(all_players_in_game_df)
    new_player_table, all_players_in_game_df, new_players_to_add_ids_list = append_players_table_with_new_players(
        mycursor, PLAYER_TABLE, players_df, all_players_in_game_df)
    if len(problematic_players_df) > 0 or len(problematic_player_in_game)>0:
        st.session_state['problematic_players_df'] = problematic_players_df
        st.session_state['problematic_player_in_game'] = problematic_player_in_game

    new_player_in_game, old_players_in_game = create_new_player_in_game_df(all_players_in_game_df)
    #  Update 'main position' and 'positions' dict in player table
    new_player_table_updated_positions_count = new_player_table.drop(['position', 'all_positions'], axis=1).merge(
        count_games_in_pos(new_player_in_game,
                           new_player_table, col_to_group_by='player_id')[['player_id', 'position', 'all_positions']]
        , left_on='p_id', right_on='player_id', how='left').drop('player_id', axis=1)

    new_player_table_updated_positions_count_new_players_to_append = new_player_table_updated_positions_count[
        new_player_table_updated_positions_count['p_id'].isin(new_players_to_add_ids_list)]



    # Check if there are duplicates before adding the new insert to table: 'player_in_game'
    tmp_table_to_insert = all_players_in_game_df.merge(old_players_in_game[['player_id', 'game_date']].reset_index(),
                                                       on=['player_id', 'game_date'], how='left')

    # If the new index column will be NaN --> the row is not in the DB
    player_in_game_to_insert_sql = tmp_table_to_insert[tmp_table_to_insert['index'].isnull()].drop('index', axis=1)

    # Update team in league table
    teams_to_match_league = get_teams_to_match_league(tmp_table_to_insert)



    # # Update tables
    if update_db:
        update_sql_table(TEAM_TABLE_NAME, new_teams_table, 't_id')
        st.info("Updated TEAM_TABLE")
        for i,row in team_df_columns_to_update.iterrows():
            update_record_to_sql(mydb, mycursor, TEAM_TABLE_NAME, 't_name_instat', json.dumps(row['t_name_instat']), ['t_id'],[row['t_id']], only_print_query = False)
        st.info("Updated TEAM_TABLE t_name_instat column")
        update_sql_table(PLAYER_TABLE, new_player_table_updated_positions_count_new_players_to_append, 'p_id')
        st.info("Updated PLAYER_TABLE")
        update_sql_table(PLAYER_IN_GAME_TABLE, player_in_game_to_insert_sql, ['player_id', 'game_date'])

        st.info("Updated PLAYER_IN_GAME_TABLE")
        update_position_and_all_position_columns(mydb, mycursor, new_player_table_updated_positions_count,
                                                 new_player_table,
                                                 new_players_to_add_ids_list)
        st.info("Updated PLAYER_TABLE all_positions column")
        st.info("Updating TEAM_IN_LEAGUE")
        if len(teams_to_match_league) > 0:
            tr.run_team_in_league_matching(team_ids=teams_to_match_league)
        st.success("Updated DB")
        st.subheader('The new updated data:')
        end = time.time()
        drop_existing_raw_player_in_game(all_players_in_game_df_raw, update_db=True)
        print("Total time=",end - startt)
        show_problematic_datasets_for_download(key='download-csv2')

def get_teams_to_match_league(df_pig):
    mydb, mycursor = connect_to_the_DB()
    til_df = read_all_table(mycursor,TEAM_IN_LEAGUE_TABLE)
    team_ids_without_league_in_til = get_set_teams_without_match_to_league(df_pig, til_df)
    t_id_to_update_season = get_set_teams_without_match_to_league_in_the_current_season(til_df)
    teams_to_match_league = list(team_ids_without_league_in_til | t_id_to_update_season)
    if dt.datetime.now().month == 8: # it takes time for transfermarket to update the league. so, if it's the beginning of august we should read the last season
        teams_to_match_league = list(team_ids_without_league_in_til)
    print("Teams to match league:")
    print(teams_to_match_league)
    print("***")
    disconnect_from_the_db(mycursor,mydb)
    return teams_to_match_league


def get_list_of_players_to_check():
    mydb,mycursor = connect_to_the_DB()
    player_table = read_all_table(mycursor,PLAYER_TABLE)
    transfer_gt_1 = player_table['p_name_transfer'].value_counts() > 1
    transfer_gt_1 = list(transfer_gt_1[transfer_gt_1].index)
    instat_gt_1 = player_table['p_name_instat'].value_counts() > 1
    instat_gt_1 = list(instat_gt_1[instat_gt_1].index)
    multiple_players = player_table[
        (player_table['p_name_transfer'].isin(transfer_gt_1)) | (player_table['p_name_instat'].isin(instat_gt_1)) | (
                    player_table['p_name_transfer_matching_score'] < 60) | (player_table['p_name_transfer'].isnull())]
    list_of_players = tuple(multiple_players['p_id'].to_list())
    query = f"""SELECT p.p_id, p.p_name_instat ,p.p_name_transfer ,p.p_name_transfer_matching_score ,t.t_name_instat ,t.t_name_transfer,pig.position,pig.game_date
              FROM player p, player_in_game pig, teams t
              WHERE 
              pig.player_id = p.p_id
              AND pig.t_id = t.t_id 
              AND pig.player_id in {list_of_players}"""
    res = read_from_table(mycursor, 'f', query)
    gb_res = res.groupby(['p_id']).agg({'t_name_transfer': list, 'position': list, 'game_date': list})
    disconnect_from_the_db(mycursor, mydb)
    return multiple_players.set_index(['p_id']).drop('position', axis=1).join(gb_res, how='outer')

def show_problematic_datasets_for_download(key = 'download-csv'):
    # todo : if there is a new line in the file : alert
    problematic_players_df_calculation = get_list_of_players_to_check()
    st.session_state['problematic_players_df_calculation'] = problematic_players_df_calculation
    for problematic_str in ['problematic_player_in_game', 'problematic_players_df','problematic_players_df_calculation']:
        if problematic_str in st.session_state:
            problematic_df = st.session_state[problematic_str]
            @st.cache
            def convert_df(df):
                return df.to_csv().encode('utf-8')
            csv = convert_df(problematic_df)
            st.download_button(
                f"Click to Download {problematic_str}",
                csv,
                f"{problematic_str}.csv",
                "text/csv",
                key=key
            )


def show_st_image(img_file_name, caption=None):
    image = Image.open(img_file_name)
    st.image(image, caption=caption, use_column_width=True)

def run_problematic_data_fix(problematic_files):
    mydb,mycursor = connect_to_the_DB()
    tagged_df = pd.read_csv(problematic_files[0])

    for i,row in stqdm(tagged_df.iterrows(), total=tagged_df.shape[0]):
        update_record_to_sql(mydb, mycursor, PLAYER_TABLE, 'p_name_transfer',f"'{str(row['p_name_transfer'])}'", ['p_id'],[int(row['p_id'])], only_print_query = False)
        update_record_to_sql(mydb, mycursor, PLAYER_TABLE, 'p_name_instat', f"'{str(row['p_name_instat'])}'", ['p_id'],[int(row['p_id'])], only_print_query = False)
        update_record_to_sql(mydb, mycursor, PLAYER_TABLE, 'p_name_transfer_matching_score', INDICATION_FOR_MANUALLY_ADDED_PLAYER_NAME, ['p_id'],[int(row['p_id'])], only_print_query = False)

    disconnect_from_the_db(mycursor,mydb)


def select_the_t_id(t_id, next_game_tid, prev_game_tid):
    if next_game_tid == prev_game_tid and prev_game_tid != t_id:
        return prev_game_tid
    else:
        return t_id


def other_team_in_the_middle_of_the_same_team(teams_list_by_date):
    if len(teams_list_by_date) >= 3:
        for team_before, team_now, team_after in zip(teams_list_by_date[:-2], teams_list_by_date[1:-1],
                                                     teams_list_by_date[2:]):
            if team_before == team_after and team_now != team_before:
                return True
        return False
    else:
        return len(set(teams_list_by_date)) != 1


def get_candidates_for_team_check(all_players_in_game_df):
    gb_player_transfer_team_list = all_players_in_game_df.sort_values('game_date').groupby(['p_id']).agg(
        {'t_name_transfer': list})
    gb_player_transfer_team_list['candidate_to_team_bug'] = gb_player_transfer_team_list['t_name_transfer'].apply(
        other_team_in_the_middle_of_the_same_team)
    return list(gb_player_transfer_team_list[gb_player_transfer_team_list['candidate_to_team_bug']].index)


def run_team_fix():
    mydb, mycursor = connect_to_the_DB()
    query = """SELECT p.p_id , p.p_name_instat ,p.p_name_transfer ,p.p_name_transfer_matching_score ,t.t_name_transfer , t.t_name_instat,pig.game_date,t.t_id
    FROM player p, player_in_game pig,teams t 
    WHERE
    	pig.player_id = p.p_id
    	and t.t_id =pig.t_id """
    all_pig_df = read_from_table(mycursor, 'f', query)
    bug_pids = get_candidates_for_team_check(all_pig_df)
    pig_df = all_pig_df[all_pig_df['p_id'].isin(bug_pids)].copy()
    tmp_result = pd.DataFrame()
    for p_id in bug_pids[:]:
        sub_df = pig_df[pig_df['p_id'] == p_id].copy()
        sub_df['prev_game_t_id'] = sub_df['t_id'].shift(1)
        sub_df['next_game_t_id'] = sub_df['t_id'].shift(-1)
        sub_df['t_id_new'] = sub_df.apply(
            lambda x: select_the_t_id(x['t_id'], x['next_game_t_id'], x['prev_game_t_id']), axis=1)
        tmp_result = tmp_result.append(sub_df[['p_id', 'game_date', 't_id_new']])
    if len(tmp_result) > 0:
        pig_df_updated_t_id = pig_df.merge(tmp_result, on=['p_id', 'game_date'], how='left')
        pig_to_update = pig_df_updated_t_id[pig_df_updated_t_id['t_id_new'].eq(pig_df_updated_t_id['t_id']) == False]
        if len(pig_to_update) > 0:
            for i,row in stqdm(pig_to_update.iterrows(), total=pig_to_update.shape[0]):
                update_record_to_sql_many_values(mydb,mycursor,PLAYER_IN_GAME_TABLE,['t_id','opponent_id'],[int(row['t_id_new']),int(row['t_id_new'])],['player_id','game_date'],
                                                 [int(row['p_id']), row['game_date']],only_print_query =False )

    disconnect_from_the_db(mycursor,mydb)




# header, player_in_game, update_weights, rank_players = containers()
def app():
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    col1, col2, col3 = st.columns(page_structure)
    with col1:
        pass
    with col2:
        # # ----------NAVBAR---------
        st.markdown(HEADER, unsafe_allow_html=True)

        st.markdown(CONTENT, unsafe_allow_html=True)
        # # -------/NAVBAR/--------------
        header = st.container()
        player_in_game = st.container()
        fix_data_issues = st.container()
        with header:
            show_st_image(img_file_name='opteamize.png', caption='Connecting the dots')
            show_problematic_datasets_for_download()
            # st.title('Hi! Welcome to opTEAMize!')
        with player_in_game:
            st.subheader("Upload new 'player in game' files")
            uploaded_files = st.file_uploader(label="Upload player in game files", accept_multiple_files=True)
            btn_update_teams = st.checkbox("Would you like to update teams and 'team in league' tables?")
            btn_upload_files = st.button("Update DB")
            if btn_upload_files:
                if len(uploaded_files) > 0:
                    run(uploaded_files, mydb, mycursor, update_teams_and_leagues=btn_update_teams, update_db=True)

                else:
                    st.error("You didn't upload player in a game file")
        with fix_data_issues:
            st.subheader("Upload labeled data and fix the data")
            st.write("1. Download the problematic file --> 2. Tag the cases -match instat to transfer player name (by changing transfer name) --> 3. Click 'fix issues'")
            problematic_files = st.file_uploader(label="Upload problematic files", accept_multiple_files=True)
            btn_upload_problematic_files = st.button("Fix issues")
            if btn_upload_problematic_files:
                if len(problematic_files) > 0:
                    run_problematic_data_fix(problematic_files)
                run_team_fix()
                mydb,mycursor = connect_to_the_DB()
                pig_df = read_all_table(mycursor,PLAYER_IN_GAME_TABLE)
                teams_to_tag_manualy = tuple(list(read_all_table(mycursor,TIL_TO_TAG_MANUALLY)['index'].unique()))
                query = f"select t_id from teams where t_name_transfer in {teams_to_tag_manualy}"
                t_ids = read_from_table(mycursor, 'f', query)['t_id'].to_list()
                teams_to_match_league = get_teams_to_match_league(pig_df)

                teams_to_match_league = [i for i in teams_to_match_league if i not in t_ids]
                teams_to_match_league = list(pig_df[pig_df['opponent_id'].isin(teams_to_match_league)]['opponent_id'].value_counts().index)
                print(teams_to_match_league)
                if len(teams_to_match_league) > 0 :
                    tr.run_team_in_league_matching(team_ids=teams_to_match_league)
                disconnect_from_the_db(mycursor,mydb)
                st.success("Done!")



    mydb.close()

    with col3:
        pass
    with col2:

        st.header("Do you want to download files from Instat?")
        open_instat = st.button("Click here to open Instat")
        if open_instat:
            open_instat = not st.button("Close Instat")

    if open_instat:
        components.iframe("https://football.instatscout.com/login", height=700, scrolling=True)

# copy to the terminal:
# 1.export LC_ALL=en_US.UTF-8
# 2.streamlit run update_database_with_player_in_game_file_streamlit.py
# 3.stop execution of process with shortcut CTRL + C
