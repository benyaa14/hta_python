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
import streamlit.components.v1 as components
# # If config works - delete------------------
# ALL_POSITIONS_STR = "All positions"
# SELECTION_ERROR = 'selection_error'
# PATH = 'new_files_to_update'
# # PATH_PLAYERS = '/content/gdrive/MyDrive/HTA project/Position and attributes analysis/files/league_to_update'
# now = dt.datetime.now().date()
# HOST = "hta-project.cf9mllj1rhry.us-east-2.rds.amazonaws.com"
# USER = 'Sagi'
# PASSWORD = "HTAproject2022"
# DB = 'hta_project'
# mydb = mysql.connector.connect(
#     host=HOST, user=USER, password=PASSWORD, database=DB
# )
# mycursor = mydb.cursor()
# TEAM_TABLE_NAME = 'teams'
# LEAGUE_TABLE = 'league'
# PLAYER_IN_GAME_TABLE = 'player_in_game'
# WEIGHTS_TABLE = 'att_to_weight'
# TEAM_IN_LEAGUE_TABLE = 'team_in_league'
# GAME_RANK_COL = 'game_rank'
# PLAYER_TABLE = 'player'
# POSITIONS = ["LD", "LM", "RM", "RD", "DM", "CM", "CD", "F"]
# ---------------------------------------------
now = dt.datetime.now().date()

page_structure = [1, 3, 1]


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
        player_rows = gb_player_position[gb_player_position['p_name'] == name].copy()

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

    sum_games = all_players_df['p_name'].value_counts().to_dict()
    players = all_players_df.groupby(['p_name', 'position'])['position'].size().to_frame().rename({'position': 'count'},
                                                                                                  axis=1).reset_index()
    players['per_pos'] = players.apply(lambda x: x['count'] / sum_games[x['p_name']], axis=1)
    unique_players = pd.DataFrame(players['p_name'].unique(), columns=['p_name'])
    unique_players[['position', 'all_positions']] = unique_players.apply(
        lambda x: get_main_pos_and_positions_count(x['p_name'], players), axis=1, result_type='expand')
    unique_players['wage'] = None
    unique_players['nationality'] = None
    unique_players['market_value'] = None
    unique_players['last_game_date'] = unique_players['p_name'].apply(
        lambda x: all_players_df[all_players_df['p_name'] == x]['game_date'].max())
    unique_players['p_rank'] = None
    unique_players['last_rank_date'] = None
    unique_players['DOB'] = None
    # unique_players.rename({'name':'p_name'},axis=1,inplace=True)
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
    players_to_add = temp_players_df[temp_players_df['p_name'].isin(players_name_to_assign_new_id)].copy()
    players_to_add['p_id'] = new_ids
    return players_to_add


# def append_all_positions_counter_to_old_players(players_table, players_in_game):
#   # players_in_game
#   pass

def get_team_id_from_team_name(team_name, df_teams):
    t_id = df_teams[df_teams['t_name'] == team_name]['t_id']
    if len(t_id) == 0:
        t_id = df_teams[df_teams['transfermarket_team_name'] == team_name]['t_id']
    t_id = t_id.iloc[0]
    return t_id


def get_player_id_from_player_name(player_name, df_player):
    return df_player[df_player['p_name'] == player_name]['p_id'].iloc[0]


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
    new_players_name = set(temp_players_df['p_name'].unique())
    old_players_name = set(players_table['p_name'].unique())

    players_name_to_assign_new_id = list(
        new_players_name - old_players_name)
    existing_name_in_the_table = new_players_name & old_players_name

    # Assign id the the new players
    players_to_add = assign_id_to_new_players(players_table['p_id'].max(), temp_players_df,
                                              players_name_to_assign_new_id)
    new_player_table = pd.concat([players_table, players_to_add])

    # Assign to the players_in_game df the player id instead of its name
    players_in_game['player_id'] = players_in_game['p_name'].apply(
        lambda x: get_player_id_from_player_name(x, new_player_table))
    players_in_game.drop(['p_name'], axis=1, inplace=True)
    return new_player_table, players_in_game, players_to_add['p_id'].to_list()
    # update_sql_table('player',df)


def assign_id_to_new_teams(last_id, temp_df, names_to_assign_id_list, id_col):
    new_ids = np.arange(last_id + 1, last_id + 1 + len(names_to_assign_id_list))
    temp_df[id_col] = new_ids


def create_new_team_table(mycursor, team_table_name, players_in_game):
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

    mycursor.execute(f"SELECT * FROM {team_table_name}")
    team_table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    new_teams_name = set(players_in_game['opponent'].unique())
    old_teams_name = set(list(team_table['t_name'].unique()) + list(team_table['transfermarket_team_name'].unique()))
    new_teams_to_add = list(new_teams_name - old_teams_name)
    temp_teams_df = pd.DataFrame(new_teams_to_add, columns=['t_name'])
    assign_id_to_new_teams(team_table['t_id'].max(), temp_teams_df, new_teams_to_add, 't_id')
    new_teams_table = pd.concat([team_table, temp_teams_df])

    players_in_game['opponent_id'] = players_in_game['opponent'].apply(
        lambda x: get_team_id_from_team_name(x, new_teams_table))
    players_in_game.drop(['opponent'], axis=1, inplace=True)
    return players_in_game, temp_teams_df


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
        new_player_table_updated_positions_count['p_id'].isin(new_players_to_add_ids_list) == False]
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


def create_new_player_in_game_df(mycursor, all_players_in_game_df, player_in_game_table_name='player_in_game'):
    """

    :param all_players_in_game_df: dataframe with player in game data
    :param player_in_game_table_name: player in game table name in the DB
    :return: new_player_in_game: new player in game dataframe that will be in the DB after it will be updated
            old_players_in_game: the old table of players_in_game that is currently in the DB
    """
    old_players_in_game = read_all_table(mycursor, player_in_game_table_name)
    new_player_in_game = pd.concat([old_players_in_game, all_players_in_game_df])
    new_player_in_game = new_player_in_game[
        new_player_in_game.duplicated(subset=['player_id', 'game_date'], keep='first') == False].copy()

    return new_player_in_game, old_players_in_game


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
    all_players_in_game_df = read_many_tables_into_one(players_file_name_list)
    players_df = create_new_players_table(all_players_in_game_df)
    new_player_table, all_players_in_game_df, new_players_to_add_ids_list = append_players_table_with_new_players(
        mycursor, PLAYER_TABLE, players_df, all_players_in_game_df)
    all_players_in_game_df, new_teams_table = create_new_team_table(mycursor, TEAM_TABLE_NAME,
                                                                    all_players_in_game_df)
    new_player_in_game, old_players_in_game = create_new_player_in_game_df(mycursor, all_players_in_game_df)
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

    # Update tables
    if update_db:
        update_sql_table(TEAM_TABLE_NAME, new_teams_table, 't_id')
        update_sql_table(PLAYER_TABLE, new_player_table_updated_positions_count_new_players_to_append, 'p_id')
        update_sql_table(PLAYER_IN_GAME_TABLE, player_in_game_to_insert_sql, ['player_id', 'game_date'])
        update_position_and_all_position_columns(mydb, mycursor, new_player_table_updated_positions_count,
                                                 new_player_table,
                                                 new_players_to_add_ids_list)

        st.success("Updated DB")
        st.subheader('The new updated data:')
        st.write(player_in_game_to_insert_sql)
    if update_teams_and_leagues:
        st.info('Scrapping data from transfermarket')
        tr.run_team_name_matching()
        query_to_find_teams_to_add_to_til_table = "select * from teams where (transfermarket_t_name_matching_score >= 60 or transfer_added_manually = 1 ) " \
                                                  "and t_id  not in (select DISTINCT (t_id) from team_in_league)"
        df_missing_teams_in_til_table = read_from_table(mycursor, TEAM_IN_LEAGUE_TABLE,query_to_find_teams_to_add_to_til_table)
        if len(list(df_missing_teams_in_til_table['t_id'].unique())) > 0:
            tr.run_team_in_league_matching(team_ids=list(df_missing_teams_in_til_table['t_id'].unique()))

            st.success("Done!")


def show_st_image(img_file_name, caption=None):
    image = Image.open(img_file_name)
    st.image(image, caption=caption,use_column_width=True)


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
        with header:
            show_st_image(img_file_name='opteamize.png', caption='Connecting the dots')
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
    mydb.close()

    with col3:
        pass
    with col2:

        st.header("Do you want to download files from Instat?")
        open_instat = st.button("Click here to open Instat")
        if open_instat:
            open_instat = not st.button("Close Instat")

    if open_instat:
        components.iframe("https://football.instatscout.com/login",height=700,scrolling=True)



# copy to the terminal:
# 1.export LC_ALL=en_US.UTF-8
# 2.streamlit run update_database_with_player_in_game_file_streamlit.py
# 3.stop execution of process with shortcut CTRL + C
