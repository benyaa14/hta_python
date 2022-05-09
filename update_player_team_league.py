"""
download player files : (0')======== V ============================================================================================================
    1. read excel files (all players in league) --> download all leagues
    2. add for each file the league name (from file name "09_04_2022_field_players_tournament_England.PremierLeague" --> PremierLeague)
    3. add column: downloded_at
    4. Join the files
    5. Store files in the DB --> instat_player_data

    ========== V ==========================================================================================================================================



match between player and instat_player_data (10')
    1. match between player in our db ('players') instat_player_data V
    2. generate keys between tables V
    3. join the tables V
    4. Update the new columns [Team	Nationality	National team 	Age	Height	Weight	Foot League] V


Trasfermarket.com request (12')
    players:
        1. scrap all players from transfermarket and store the new table in 'players_transfermarket' (run per league)
            columns = [name position team league instat_position market_value]
        2. match between player in 'players' table to 'players_transfermarket'
        3. join the tables
        4. update 'players' table with transfermarket data [market_value tranfermarket_data(dict)]

    teams(optional):
        1. Take from 'instat_player_data' the league for each team in the current season (columns: [team_name league_name seasonm])
        2. Join with team_in_league
        3. filter for discrapencies
        4. updates discrapencies by the instat_player_data

Create the most uptodate player data
DONE

"""
import numpy as np
import mysql.connector
from streamlit_gui import *
from nav_bar import HEADER, CONTENT
from config import *
from main_functions import *
from stqdm import stqdm
from PIL import Image
import transfermarket_request as tr

now = dt.datetime.now()
from fuzzywuzzy import fuzz

page_structure = [1, 3, 1]


def show_st_image(img_file_name, caption=None):
    image = Image.open(img_file_name)
    st.image(image, caption=caption)


def read_excel(file_name):
    try:
        df = pd.read_excel(file_name, engine='openpyxl')
    except:
        raise Exception(f"{file_name} dosen't exist")
    return df


def get_league_name_from_file_name(file_name):
    # "09_04_2022_field_players_tournament_England.PremierLeague.xlsx" --> PremierLeague
    full_name = str(file_name.split('_')[-1].split('.xlsx')[0])  # returns England.PremierLeague
    country, league = full_name.split('.', 1)
    return country, league


def join_uploded_files(uploaded_files):
    for j, file_name in enumerate(uploaded_files):
        country, league = get_league_name_from_file_name(file_name.name)
        if j == 0:  # Read the first df
            all_players_df = read_excel(file_name)
            all_players_df['l_country'] = country
            all_players_df['league'] = league
        else:
            tmp_df = read_excel(file_name)
            tmp_df['l_country'] = country
            tmp_df['league'] = league
            all_players_df = pd.concat([tmp_df, all_players_df])
    all_players_df['date_downloaded'] = now
    return all_players_df


def change_col_names(df):
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    df.rename({'unnamed:_1': 'player_name'}, axis=1, inplace=True)
    df.drop('unnamed:_0', axis=1, inplace=True)
    df.reset_index(inplace=True, drop=True)


def transform_and_upload_files_to_db(uploaded_files, mydb, mycursor):
    st.info('Uploading files to the DB')
    joined_df = join_uploded_files(uploaded_files)
    change_col_names(joined_df)
    update_sql_table(INSTAT_DATA_TABLE, joined_df.reset_index(), 'index', 'append')
    st.info(f"Inserted {len(joined_df)} rows")
    st.success('Uploaded files to the DB')

def download_data_from_transfermarket(mycursor):
    all_leagues = read_all_table(mycursor, LEAGUE_TABLE)['l_name_transfermarket'].to_list()
    df_players_transfermarket = pd.DataFrame(
        columns=['name', 'position', 'market_value', 'nationality', 'team', 'league'])
    for league in stqdm(all_leagues):
        teams_in_league = tr.find_all_teams_in_league(league)
        for team in stqdm(teams_in_league):
            players_in_team = tr.find_all_players_with_pos_in_team(team)
            tmp_df = pd.DataFrame(players_in_team, columns=['name', 'position', 'market_value', 'nationality'])
            tmp_df['team'] = team
            tmp_df['league'] = league
            df_players_transfermarket = pd.concat([df_players_transfermarket, tmp_df])
    df_players_transfermarket['instat_position'] = df_players_transfermarket['position'].apply(
        lambda x: DICT_TRANSFERMARKET_POS_TO_INSTAT_POS.get(x))
    df_players_transfermarket.reset_index(inplace=True)
    df_players_transfermarket.drop_duplicates(inplace=True, subset=['name', 'position', 'team'])
    df_players_transfermarket.drop('index', inplace=True, axis=1)
    df_players_transfermarket['date_downloaded'] = now
    update_sql_table(PLAYERS_TRANSFERMARKET_TABLE, df_players_transfermarket.reset_index(), 'index',
                     if_exists='append')
    st.success('Done')


def run_external_data_enhancement(uploaded_files, mydb, mycursor):
    if len(uploaded_files) > 0:
        transform_and_upload_files_to_db(uploaded_files, mydb, mycursor)
    download_data_from_transfermarket(mycursor)


def read_all_relevant_tables_for_player_table_enhancments(mycursor):
    df_instat = read_all_table(mycursor, INSTAT_DATA_TABLE)
    teams_df = read_all_table(mycursor, TEAM_TABLE_NAME)
    players_trans = read_all_table(mycursor, PLAYERS_TRANSFERMARKET_TABLE)
    players_trans = find_updated_records_in_instat_data(players_trans, gb_cols = ['name','position','team'])
    player_df = read_all_table(mycursor, PLAYER_TABLE)
    pig_df = read_all_table(mycursor, PLAYER_IN_GAME_TABLE)
    return df_instat, teams_df, players_trans, player_df, pig_df


def find_updated_records_in_instat_data(df_instat, gb_cols):
    # The func return the most up-to-date records from instat player df
    updated_instat = df_instat.sort_values('date_downloaded').groupby(gb_cols).agg(
        {i: 'last' for i in df_instat.columns if i not in gb_cols}).reset_index()
    return updated_instat


def merge_players_in_db_to_instat_data(updated_instat, player_df):
    gb = updated_instat.groupby(['player_name_instat'])['player_name_instat'].size()
    dup_players = list(set(gb[gb > 1].index))
    merged_df = player_df.merge(updated_instat, left_on=['p_name'], right_on=['player_name_instat'], how='outer')
    return merged_df, dup_players


def split_dfs_into_3_groups(merged_df, updated_instat, dup_players):
    didnt_match = merged_df[merged_df['player_name_instat'].isnull()].copy()
    duplicate_players_to_split_in_player_in_game_later = updated_instat[
        updated_instat['player_name_instat'].isin(dup_players)].copy()
    matched_players = merged_df[
        (merged_df['player_name_instat'].isnull() == False) & (merged_df['p_name'].isnull() == False) & (
                    merged_df['p_name'].isin(dup_players) == False)].copy()
    matched_players['confidance_instat'] = 1
    return matched_players, duplicate_players_to_split_in_player_in_game_later, didnt_match


def match_best_player_name(row, player_name_list_instat):
    d_scores = dict()
    for name_istat in player_name_list_instat:
        score = fuzz.ratio(row['p_name'], name_istat)
        if score > 50:
            d_scores[name_istat] = score
    return [[k, v] for k, v in sorted(d_scores.items(), key=lambda item: item[1], reverse=True)]


def match_for_other_players_we_didnt_match_and_concat_to_matched_players(updated_instat, didnt_match, matched_players,
                                                                         dup_players_list):
    # best match for players we didn't find perfect match
    player_name_list_instat = updated_instat['player_name_instat'].to_list()
    didnt_match['d_scores'] = didnt_match.apply(lambda x: match_best_player_name(x, player_name_list_instat), axis=1)
    didnt_match['best_score'] = didnt_match['d_scores'].apply(lambda x: x[0][1] if x is not None else None)
    didnt_match['best_match'] = didnt_match['d_scores'].apply(lambda x: x[0][0] if x is not None else None)

    add_to_matched = didnt_match[
        (didnt_match['best_score'] > 85) & (didnt_match['best_match'].isin(dup_players_list) == False)].copy()
    add_to_matched['confidance_instat'] = add_to_matched['best_score'] / 100
    add_to_matched = add_to_matched.merge(updated_instat, left_on='best_match', right_on='player_name_instat',
                                          how='left', suffixes=['_drop', '']).drop(
        ['best_score', 'best_match', 'd_scores'], axis=1)
    add_to_matched.drop([i for i in list(add_to_matched.columns) if '_drop' in i], axis=1, inplace=True)
    if sorted(list(add_to_matched.columns)) != sorted(list(matched_players.columns)):
        raise Exception('There is a problem with the columns of add_to_matched, matched_players')
    matched_players = pd.concat([matched_players, add_to_matched])
    to_add_ids = add_to_matched['p_id'].to_list()
    didnt_match = didnt_match[didnt_match['p_id'].isin(to_add_ids) == False].copy()
    return matched_players, didnt_match


def generate_groups_of_dfs_to_match(pig_df, df_instat, player_df):
    # The function will generate 3 dfs :
    # 1.  didnt_match = players we didnt find matched player in instat data
    # 2.  duplicate_players_to_split_in_player_in_game_later = many player with the same name --> we will solve it manually
    # 3.  matched_players = we found 1:1 match between the db and instat
    updated_instat = find_updated_records_in_instat_data(df_instat,
                                                         gb_cols=['player_name', 'position', 'nationality', 'foot',
                                                                  'height'])
    updated_instat.columns = [str(i) + '_instat' for i in updated_instat.columns]
    merged_df, dup_players = merge_players_in_db_to_instat_data(updated_instat, player_df)
    matched_players, duplicate_players_to_split_in_player_in_game_later, didnt_match = split_dfs_into_3_groups(
        merged_df, updated_instat, dup_players)
    dup_players_list = list(duplicate_players_to_split_in_player_in_game_later['player_name_instat'].unique())
    matched_players, didnt_match = match_for_other_players_we_didnt_match_and_concat_to_matched_players(updated_instat,
                                                                                                        didnt_match,
                                                                                                        matched_players,
                                                                                                        dup_players_list)
    return matched_players, duplicate_players_to_split_in_player_in_game_later, didnt_match


def get_str_matching_dict(origin_name, name_list, treshold_score=50, return_only_top_value=False):
    d_scores = dict()
    for cand_name in name_list:
        score = fuzz.ratio(origin_name, cand_name)
        if score > treshold_score:
            d_scores[cand_name] = score
    top_list = [[k, v] for k, v in sorted(d_scores.items(), key=lambda item: item[1], reverse=True)]
    if return_only_top_value:
        if len(top_list) > 0:
            return top_list[0][0]
        return None
    #   d.keys()
    return [[k, v] for k, v in sorted(d_scores.items(), key=lambda item: item[1], reverse=True)]


def find_matching_in_transfermarket_data(p_name_db, p_name_instat, team_name_transfer, players_trans):
    # res = None
    cand_players_trans = players_trans[players_trans['team'] == team_name_transfer]
    cand_players_trans_name_list = cand_players_trans['name'].to_list()
    if p_name_db in cand_players_trans_name_list or p_name_instat in cand_players_trans_name_list:
        res = cand_players_trans[cand_players_trans['name'].isin([p_name_db, p_name_instat])].iloc[0]
        res['confidance'] = 1
    # string matching
    # if res != None: todo: maybe we should add this conditions because the code is doing the above and below sections. but if there is 100% matching, dont do the below section
    best_match = get_str_matching_dict(p_name_db, cand_players_trans_name_list, return_only_top_value=True)
    if best_match is not None:
        res = cand_players_trans[cand_players_trans['name'] == best_match].iloc[0]
        res['confidance'] = fuzz.ratio(best_match, p_name_db) / 100
    else:  # No match
        return None
    return res['name'], res['position'], res['market_value'], res['nationality'], res['team'], res['league'], res[
        'date_downloaded'], res['confidance']


def add_transfermarket_data(players_trans, df_instat, matched_players):
    teams_trans = list(players_trans['team'].unique())
    teams_instat = list(df_instat['team'].unique())
    d_instat_team_to_trans_team = dict()
    for ins_team in teams_instat:
        d_instat_team_to_trans_team[ins_team] = get_str_matching_dict(ins_team, teams_trans, treshold_score=50,
                                                                      return_only_top_value=True)

    matched_players[
        ['name_transfer', 'position_transfer', 'market_value_transfer', 'nationality_transfer', 'team_transfer',
         'league_transfer', 'date_downloaded_transfer', 'confidance_transfer']] = matched_players.apply(
        lambda x: find_matching_in_transfermarket_data(x['p_name'], x['player_name_instat'],
                                                       d_instat_team_to_trans_team.get(x['team_instat']),
                                                       players_trans), axis=1,
        result_type='expand')  # p_name (in db), player_name (instat), team (instat)
    return d_instat_team_to_trans_team


def decide_which_data_to_update_between_instat_and_transfer(providers_data, providers_col_name,
                                                            d_instat_team_to_trans_team):
    indication_for_null_signs = ('nan', None, 'Null', 'None', np.nan, '', ' ')

    # The logic is:
    # 1. If the data in transfermarket = instat --> implement
    # 2. elif the 2 providers are null --> implement null
    # 3. elif one of the providers is null --> implement other provider
    # 4. else: the 2 providers have different data --> append discrapencies list between instat to transfermarket --> implement instat


    provider = providers_col_name[0].split('_')[-1]

    if len(providers_data) == 1:  # only 1 provider for the data
        data = providers_data[0]

    elif len(providers_data) == 2:
        # one of the providers is null
        if str(providers_data[0]) in indication_for_null_signs:
            data = providers_data[1]
            provider = providers_col_name[1].split('_')[-1]

        elif str(providers_data[1]) in indication_for_null_signs:
            data = providers_data[0]

        elif (str(providers_data[1]) == str(providers_data[0])) or (
                'team' in providers_col_name[0] and fuzz.ratio(providers_data[0], providers_data[1]) > 0.9):
            data = providers_data[0]
            provider = 'all'
        else:  # discrapencies in the data
            data = providers_data[0]
            provider = 'discrapency'
    else:
        raise Exception("Something is wrong with the providers data")
    return data, provider


def get_final_df_to_update_db(row, d_instat_team_to_trans_team):
    columns_to_update_dict = {'nationality': ['nationality_instat', 'nationality_transfer'],
                              'market_value': ['market_value_transfer'],
                              'age': ['age_instat'],
                              'height': ['height_instat'],
                              'weight': ['weight_instat'],
                              'foot': ['foot_instat'],
                              'team': ['team_instat', 'team_transfer']
                              }
    new_col_dict = dict()
    chosen_provider_dict = dict()  # will hold for each column which provider is chosen {column:provider} --> if there are discrapencies between the providers the value of provider  = 'discrepancy'
    for col, providers_col_name in columns_to_update_dict.items():
        providers_data = [str(row[i]).replace('-', ' ') for i in providers_col_name]

        if col == 'nationality':  # ['isral,poland', 'denemark'] --> [['israel','poland'], ['denemark']]
            new_providers_data = []
            for str_nations in providers_data:
                new_provider_data = []
                nations_list = str_nations.split(',')
                for nation in nations_list:
                    nation = nation.replace("'","").strip()
                    new_provider_data.append(nation)
                new_providers_data.append(sorted(new_provider_data))
            providers_data = new_providers_data

            # providers_data = [i.strip() for i in data.split(',') for data in providers_data]
        data, provider = decide_which_data_to_update_between_instat_and_transfer(providers_data, providers_col_name,
                                                                                 d_instat_team_to_trans_team)
        new_col_dict[col] = data
        chosen_provider_dict[col] = provider
    instat_dict = row.loc[[i for i in row.index if 'instat' in i]].to_dict()
    instat_dict['confidance_instat'] = row['confidance_instat']
    instat_dict['date_downloaded_instat'] = str(instat_dict['date_downloaded_instat']).split()[0]

    trans_dict = row.loc[[i for i in row.index if 'transfer' in i]].to_dict()
    trans_dict['confidance_transfer'] = row['confidance_transfer']  # todo
    trans_dict['date_downloaded_transfer'] = str(trans_dict['date_downloaded_transfer']).split()[0]

    return row['p_id'], row['p_name'], row['position'], row['all_positions'], new_col_dict['nationality'], new_col_dict[
        'market_value'], \
           new_col_dict['age'], new_col_dict['height'], new_col_dict['weight'], new_col_dict['foot'], new_col_dict[
               'team'], trans_dict, instat_dict, chosen_provider_dict


def match_team_by_providers_data(transfermarket_data_d, instat_data_d, teams_df, d_instat_team_to_trans_team):
    instat_team = instat_data_d.get('team_instat')
    trans_team = instat_data_d.get('team_transfer')

    instat_match = teams_df[teams_df['t_name'] == instat_team]
    transfermarket_match = teams_df[teams_df['transfermarket_team_name'] == trans_team]
    combined_match = teams_df[teams_df['transfermarket_team_name'] == d_instat_team_to_trans_team.get(instat_team)]
    # first match team name by instat data
    if len(instat_match) == 1:
        return instat_match['t_id'].iloc[0]

    elif len(transfermarket_match) == 1:
        return transfermarket_match['t_id'].iloc[0]

    elif len(combined_match) == 1:
        return combined_match['t_id'].iloc[0]
    else:
        return None


def change_none_to_null_and_tackle_to_nothing(dict_):
    for k, v in dict_.items():
        if str(v) in (None, 'None', '', ' '):
            dict_[k] = 'Null'
        if "'" in str(v):
            dict_[k] = v.replace("'", "")
    if 'transfermarket_data' in list(dict_.keys()):
        del dict_['transfermarket_data']
    return dict_


def run_player_table_enhancments(mydb, mycursor):
    st.info('Runnning..')

    df_instat, teams_df, players_trans, player_df, pig_df = read_all_relevant_tables_for_player_table_enhancments(
        mycursor)

    matched_players, duplicate_players_to_split_in_player_in_game_later, didnt_match = generate_groups_of_dfs_to_match(
        pig_df, df_instat, player_df)
    d_instat_team_to_trans_team = add_transfermarket_data(players_trans, df_instat, matched_players)

    df_to_update = matched_players.apply(lambda x: get_final_df_to_update_db(x, d_instat_team_to_trans_team), axis=1,
                                         result_type='expand')
    df_to_update.columns = ['p_id', 'p_name', 'position', 'all_positions', 'nationality', 'market_value', 'age',
                            'height', 'weight', 'foot', 'team', 'transfermarket_data', 'instat_data',
                            'providers_metadata']


    df_to_update['t_id'] = df_to_update.apply(
        lambda x: match_team_by_providers_data(x['transfermarket_data'], x['instat_data'], teams_df,
                                               d_instat_team_to_trans_team), axis=1)
    df_to_update.drop('team', axis=1, inplace=True)


    df_to_update['transfermarket_data'] = df_to_update['transfermarket_data'].apply(
        change_none_to_null_and_tackle_to_nothing)
    df_to_update['instat_data'] = df_to_update['instat_data'].apply(change_none_to_null_and_tackle_to_nothing)

    today_str = str(dt.datetime.now().date())
    df_to_update['last_rank_date'] = today_str
    cols_to_set_str = ['market_value', 'age', 'height', 'weight', 'last_rank_date']
    cols_to_set_int = ['t_id']
    cols_to_set_dict = ['transfermarket_data', 'instat_data', 'providers_metadata', 'nationality', 'foot']
    all_cols_to_set = cols_to_set_str + cols_to_set_int + cols_to_set_dict
    for i, row in stqdm(df_to_update.iterrows(), total=df_to_update.shape[0]):
        vals_to_update = [f""" "{str(i).replace("'","")}" """ if i not in ('', ' ') else 'Null' for i in
                          row.loc[all_cols_to_set].values]

        _id = str(int(row['p_id']))
        update_record_to_sql_many_values(mydb, mycursor, 'player', cols_name_to_set=all_cols_to_set,
                                         values_to_update=vals_to_update,
                                         index_cols=['p_id'],
                                         index_values=[_id], only_print_query=False)
    st.success('Updated DB')
    return duplicate_players_to_split_in_player_in_game_later,didnt_match

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
        upload = st.container()
        update = st.container()
        with header:
            show_st_image(img_file_name='opteamize.png', caption='Connecting the dots')
            st.title('Update Static Data')

        with upload:
            st.header("A. External Data Enhancement")
            st.markdown("When you will click `Update DB` the system will do the following process:")
            st.markdown(
                """<ol> <li>Read all Instat files and update <code> instat_player_data</code> table </li> <li> Scrape all leagues from <a href = "https://www.transfermarkt.co.uk/" >transfermarket.co.uk </a> and update <code>players_transfermarket</code> table </li></ol>""",
                unsafe_allow_html=True)
            uploaded_files = st.file_uploader(label="Upload player personal data by league (files from Instat)",
                                              accept_multiple_files=True)
            btn_upload_files = st.button("Update DB")
            if btn_upload_files:
                run_external_data_enhancement(uploaded_files, mydb, mycursor)

        with update:
            st.header("B. Fuse The Data ")
            st.markdown("When you will click `RUN!` the system will do the following process:")
            st.markdown("""<ol> <li> Matching between player in <code>players</code> table to <code>instat_player_data</code></li> 
                                <li> Matching between player in <code>players</code> table to <code>players_transfermarket</code>  </li>
                                <li> Static data enhancement: updates the following columns: 
                                <ul>
                                    <li> Nationality   </li> 
                                    <li> Market value   </li>
                                    <li> Name   </li>
                                    <li> foot   </li>
                                    <li> Age   </li>
                                    <li> Height   </li>
                                    <li> Weight   </li>
                                    <li> t_id   </li>
                                    <li> last_rank_date   </li>
                                    <li> transfermarket_data   </li>
                                    <li> instat_data   </li>
                                    <li> providers_metadata   </li>
                                 </ul>
                                 </li> 
                         </ol>""",
                        unsafe_allow_html=True)
            # st.header('B. Update player, league, teams and team_in_league tables')
            run_updates = st.button('RUN!!')
            if run_updates:
                duplicate_players_to_split_in_player_in_game_later,didnt_match = run_player_table_enhancments(mydb, mycursor)
                st.subheader(f"There are {len(didnt_match) } we couldn't match. Would you like to "
                             f"download their csv file and fill it up manually ?")

                @st.cache
                def convert_df(df):
                    return df.to_csv().encode('utf-8')
                csv = convert_df(didnt_match)
                st.download_button(
                    "Press to Download",
                    csv,
                    "unmatched_players.csv",
                    "text/csv",
                    key='download-csv'
                )



        mydb.close()


