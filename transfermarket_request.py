from fuzzywuzzy import fuzz
import requests
from bs4 import BeautifulSoup
import mysql.connector
import numpy as np
from main_functions import *
from stqdm import stqdm
import re
import wikipedia

mydb = mysql.connector.connect(
    host=HOST, user=USER, password=PASSWORD, database=DB
)
mycursor = mydb.cursor()
to_tag_manually = []
til_to_tag_manually = []


def get_transfermarket_team_name(team, to_tag_manually):
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    team = team.replace('-', ' ')
    team_str = team.lower().replace(' ', '+')
    page = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query=" + team_str
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')

    teams = pageSoup.find_all("td", {"class": "hauptlink"})
    idx_to_remove = 0
    for i, team_ in enumerate(teams):
        if "verein" not in str(team_):  # Indicates that this is a team
            idx_to_remove = i + 1
        else:
            break

    teams = teams[idx_to_remove:]
    if len(teams) == 0:
        to_tag_manually.append(team)
        return None, None
    first_teams_a = str(teams[0].find_all("a")[0])
    idx_start = first_teams_a.find('>') + 1
    idx_end = first_teams_a.rfind('<')
    transfer_team = first_teams_a[idx_start:idx_end]
    return transfer_team, fuzz.ratio(transfer_team, team)


def get_leagues_per_team(team):
    if team is None:
        return ('ERROR!!!!!! Null transfer market team name')
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    # team = 'bnei'
    team_str = team.lower().replace(' ', '+')
    page = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query=" + team_str
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    teams = pageSoup.find_all("td", {"class": "hauptlink"})

    if len(teams) > 0 :
        first_teams_a = teams[0].find_all("a")
        if len(first_teams_a) > 0:
            first_teams_a = str(first_teams_a[0])

        idx_start = first_teams_a.find('a href=') + len('a href="')
        team_url = first_teams_a[idx_start:].split(" ")[0][:-1]
        page = "https://www.transfermarkt.co.uk" + team_url
        pageTree = requests.get(page, headers=headers)
        pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
        d_values = pageSoup.find_all("span", {"class": "dataValue"})
        try:
            league_url = d_values[-1].a.get('href')
            page = "https://www.transfermarkt.co.uk" + league_url
            pageTree = requests.get(page, headers=headers)
            pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
            table_leagues = pageSoup.find_all("table", {"class": "items"})
            if len(table_leagues) == 1:
                df_leagues = pd.read_html(str(table_leagues[0]))[0]
                df_leagues['t_name'] = team
                return df_leagues[['Season', 'League.1', 't_name']]
            else:
                return (['ERROR!!!!!!'])
        except:
            return (['Something went wrong'])
    else:
        return (['Something went wrong len(teams) == 0 '])

def get_league_id_from_league_transfermarket_name(l_transfer, leagues_df, generate_new_league_if_null):
    league = leagues_df[leagues_df['l_name_transfermarket'] == l_transfer]['l_id']
    if len(league) == 0:
        if generate_new_league_if_null:
            new_index = leagues_df['l_id'].max() + 1
            l_name = l_name_transfermarket = l_transfer
            correction_to_il = None
            country = None
            df_to_add = pd.DataFrame([[new_index, l_name, country, correction_to_il, l_name_transfermarket]],
                                     columns="l_id,l_name,country,correction_to_il,l_name_transfermarket".split(','))
            update_sql_table(LEAGUE_TABLE, df_to_add, 'l_id')
            print('Updated DB')
            return new_index

            # get_league_id_from_league_transfermarket_name(l_transfer,False)
        else:
            return None
    else:
        return league.iloc[0]


def add_db_columns(til_transfer_df, teams_df, team, leagues_df, leagues_transfer_name_in_the_db, d_transfer_name_to_id):
    # d_transfer_name_to_id = leagues_df[leagues_df['l_name_transfermarket'].isin(leagues_transfer_name_in_the_db)][['l_name_transfermarket','l_id']].set_index('l_name_transfermarket').to_dict()['l_id']
    print(d_transfer_name_to_id)
    til_transfer_df['season_y'] = til_transfer_df['Season'].apply(lambda x: int('20' + x.split('/')[0]))
    til_transfer_df['t_id'] = teams_df[teams_df['t_name_transfer'] == team]['t_id'].iloc[0]
    til_transfer_df['l_id'] = til_transfer_df['League.1'].apply(
        lambda x: d_transfer_name_to_id[x] if x in d_transfer_name_to_id else None)
    return d_transfer_name_to_id


def run_team_name_matching(match_all_teams=False):
    teams_df = read_all_table(mycursor, TEAM_TABLE_NAME)
    teams = teams_df['t_name_transfer'].to_list()
    # Find new teams that do not have transfermarket_team_name
    if not match_all_teams:
        teams = teams_df[teams_df['transfermarket_team_name'].isnull()]['t_name'].to_list()
    # For each team match the transfermarket team name
    for team in stqdm(teams):
        transfer_team, matching_score = get_transfermarket_team_name(team, to_tag_manually)
        if transfer_team != None:
            # Update the DB
            update_record_to_sql(mydb, mycursor, TEAM_TABLE_NAME, 'transfermarket_team_name',
                                 '"' + transfer_team + '"', index_cols=['t_name'], index_values=['"' + team + '"'],
                                 only_print_query=False)
            update_record_to_sql(mydb, mycursor, TEAM_TABLE_NAME, 'transfermarket_t_name_matching_score',
                                 matching_score, index_cols=['t_name'], index_values=['"' + team + '"'],
                                 only_print_query=False)
            update_record_to_sql(mydb, mycursor, TEAM_TABLE_NAME, 'transfer_added_manually',
                                 False, index_cols=['t_name'], index_values=['"' + team + '"'], only_print_query=False)
    # to_tag_manually = Teams that we couldn't succeed with the team matching name
    print(f"Teams to update manually:\n {to_tag_manually}")
    return to_tag_manually

def run_team_in_league_matching(team_ids: list = [] ,years_lookback=3):
    mydb,mycursor = connect_to_the_DB()
    # Read
    teams_df = read_all_table(mycursor, TEAM_TABLE_NAME)
    til_df = read_all_table(mycursor, TEAM_IN_LEAGUE_TABLE)
    leagues_df = read_all_table(mycursor, LEAGUE_TABLE)
    leagues_transfer_name_in_the_db = list(leagues_df['l_name'].unique())
    if len(leagues_df) == 0 :
        last_league_index = 0
    else:
        last_league_index = leagues_df['l_id'].max()
    new_til = pd.DataFrame(columns=['t_id', 'season_y', 'l_id'])
    d_transfer_name_to_id = leagues_df[leagues_df['l_name'].isin(leagues_transfer_name_in_the_db)][
        ['l_name', 'l_id']].set_index('l_name').to_dict()['l_id']
    if len(team_ids) > 0:
        teams = list(teams_df[teams_df['t_id'].isin(team_ids)]['t_name_transfer'].unique())
    else:
        return
    for i, team in stqdm(enumerate(teams),total=len(teams)):

        til_transfer_df = get_leagues_per_team(team)[
                          :years_lookback]  # dataframe in this format: til_transfer_df[['Season','League.1','t_name']] or ['Something went wrong'] or ['ERROR!!!!!!']
        if team is not None:
            print('-------' + team + '-----')
        if type(til_transfer_df) == pd.DataFrame:
            # Find if it is new league to add
            add_db_columns(til_transfer_df, teams_df, team, leagues_df, leagues_transfer_name_in_the_db,
                           d_transfer_name_to_id)
            new_leagues = til_transfer_df[til_transfer_df['l_id'].isnull()][['l_id', 'League.1']].copy()
            if len(new_leagues) > 0:
                # new_leagues = new_leagues['League.1'].value_counts().to_frame()
                new_leagues = new_leagues.groupby('League.1').agg({'League.1': 'first'})
                new_leagues['l_id'] = np.arange(last_league_index + 1, last_league_index + 1 + len(new_leagues))

                new_leagues['l_name'] = new_leagues['League.1']
                new_leagues['country'] = None
                new_leagues['correction_to_il'] = None
                # Add to the league df
                df_to_concat = new_leagues[['l_id', 'l_name', 'country', 'correction_to_il']]
                leagues_df = pd.concat([leagues_df, df_to_concat])
                last_league_index = leagues_df['l_id'].max()
                # Insert new l_ids ids to til_transfer_df
                d_transfer_name_to_id = \
                    leagues_df[['l_name', 'l_id']].set_index('l_name').to_dict()['l_id']
                til_transfer_df['l_id'] = til_transfer_df["League.1"].apply(lambda x: d_transfer_name_to_id[x])

            print('After')

            new_til = pd.concat([new_til, til_transfer_df[['t_id', 'season_y', 'l_id']]])


        else:

            til_to_tag_manually.append(team)

    leagues_df.to_csv('to_check_leagues_df.csv')
    new_til.to_csv('to_check_new_til.csv')

    til_to_tag_manually_df = pd.DataFrame(til_to_tag_manually, columns=['index'])
    til_to_tag_manually_df.to_csv('til_to_tag_manually.csv')

    old_leagues_df = read_all_table(mycursor, LEAGUE_TABLE)
    ids_to_insert = list(set(leagues_df['l_id'].unique()) - set(old_leagues_df['l_id'].unique()))
    leagues_df.to_csv("NEW_league.csv")# todo: delete later
    update_sql_table(LEAGUE_TABLE, leagues_df[leagues_df['l_id'].isin(ids_to_insert)], 'l_id')
    print("leagues_df was updated")

    total_til = pd.concat([til_df, new_til])
    til_df.rename({'l_id': 'old_l_id'}, axis=1, inplace=True)
    til_records_to_update = til_df.merge(new_til, on=['t_id', 'season_y'], how='inner')
    til_records_to_update['same_league'] = til_records_to_update['l_id'].eq(til_records_to_update['old_l_id'])
    til_records_to_update = til_records_to_update[til_records_to_update['same_league'] == False]

    for i, row in til_records_to_update.iterrows():
        update_record_to_sql(mydb, mycursor, table_name=TEAM_IN_LEAGUE_TABLE,
                             col_name_to_set='l_id', value_to_update=row['l_id']
                             , index_cols=['t_id', 'season_y'], index_values=[row['t_id'], row['season_y']],
                             only_print_query=True)
    new_records_til = new_til.merge(til_df, how='left',
                                    on=['t_id', 'season_y'])  # new records will be:  old_l_df == null
    new_records_til = new_records_til[new_records_til['old_l_id'].isnull()][['t_id', 'season_y', 'l_id']]

    print("NEW RECORDS TIL:")
    print(new_records_til)
    new_records_til.to_csv('NEW_til.csv')# todo: delete later
    print(new_records_til)
    update_sql_table(TEAM_IN_LEAGUE_TABLE, new_records_til, ['t_id', 'season_y'])
    print("TEAM_IN_LEAGUE_TABLE was updated")

    print(til_to_tag_manually)
    update_sql_table('til_to_tag_manually', til_to_tag_manually_df, "index")

    print('------------')


def get_players_name_from_team_name(team):
    pattern = '[a-zA-Z0-9,.]+'
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    team_str = team.lower().replace(' ', '+')
    page = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query=" + team_str
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    teams = pageSoup.find_all("td", {"class": "hauptlink"})
    first_teams_a = str(teams[0].find_all("a")[0])
    idx_start = first_teams_a.find('a href=') + len('a href="')
    team_url = first_teams_a[idx_start:].split(" ")[0][:-1]
    page = "https://www.transfermarkt.co.uk" + team_url
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    match = []
    records = pageSoup.find_all("td", {"class": "posrela"})
    records_market_value = pageSoup.find_all("td", {"class": "rechts hauptlink"})
    records_nationality = pageSoup.find_all("td", {"class": "zentriert"})

    for rec, rec_market_value, rec_nations in zip(records, records_market_value, records_nationality):
        dff = pd.read_html(str(rec))[0][1]
        name = dff.values[0]
        # pos = dff.values[1]
        full_name = name.split('.')[0][:-1]


        # market_value_raw = rec_market_value.text
        # str_market_value = "".join(re.findall(pattern, str(market_value_raw)))

        # nationalities = rec_nations.find_all("img", {"class": "flaggenrahmen"})
        # nation_list = []
        # for nation in nationalities:
        #     nation = nation['title']
        #     nation = "".join(re.findall(pattern, str(nation)))
        #     nation_list.append(nation)
        # nations = ",".join(nation_list)
        # match.append([full_name, pos, str_market_value, nations])
        match.append(full_name)



    return match




def get_player_data(player: str):
    pattern = '[a-zA-Z0-9,.]+'
    # connect to the website
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    # find the player
    player = player.replace('.', '')
    player_str = player.lower().replace(' ', '+')
    page = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query=" + player_str
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')

    players = pageSoup.find_all("td", {"class": "hauptlink"})
    if len(players) == 0:
        return None
    selected_player = str(players[0])
    len_href = len("a href=")
    idx_start = selected_player.find('a href=')
    selected_player = selected_player[idx_start + len_href + 1:]
    idx_end = selected_player.find('"')
    player_url = selected_player[:idx_end]

    # Go to the player's page
    page = "https://www.transfermarkt.co.uk" + player_url

    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    # return pageSoup
    table = pageSoup.find_all("div",{"class": "large-6 large-pull-6 small-12 columns spielerdatenundfakten"})
    data_dict = dict()
    if len(table)>0:
        table = table[0]

        data_player = [i.text.strip() for i in table.find_all('span')[1:]]
        for key, value in zip(data_player[:-1], data_player[1:]):
            if ':' in key:
                data_dict[key[:-1]] = value.replace(u'\xa0', u' ')
        market_val = pageSoup.find_all("div", {"class": "data-header__box--small"})
        if len(market_val) > 0:
            market_val = market_val[0].text.strip().split()[0]
            market_val = re.findall(pattern, market_val)[0]
            data_dict['market_value'] = market_val
    return data_dict
    # key = pageSoup.find_all("span", {"class": "info-table__content info-table__content--regular"})
    # value = pageSoup.find_all("span", {"class": "info-table__content info-table__content--bold"})
    # data_dict = dict()
    # # take the data to a dict
    # print(len(key))
    # print(value)
    # print([str(key[i].text).strip()[:-1] for i in range(len(key))])
    # print([str(value[i].text).strip() for i in range(len(value))])
    # for i in range(min(len(key),len(value))):
    #
    #     # print(str(key[i].text).strip()[:-1])
    #     # print(str(value[i].text).strip().replace("'", ''))
    #     # if str(key[i].text).strip() not in ('Name in home country:','Height:','Citizenship:'):#todo:delete this row later
    #     data_dict[str(key[i].text).strip()[:-1]] = " ".join(
    #         re.findall(pattern, str(value[i].text).strip().replace("'", '')))
    # if len(data_dict) == 0:
    #     return None
    # team = pageSoup.find_all("span", {"class": "hauptpunkt"})
    # if len(team) > 0:
    #     data_dict['team'] = team[0].text
    # market_val = pageSoup.find_all("div", {"class": "data-header__box--small"})
    # if len(market_val) > 0:
    #     market_val = market_val[0].text.strip().split()[0]
    #     market_val = re.findall(pattern, market_val)[0]
    #     data_dict['market_value'] = market_val
    # return data_dict


def find_all_teams_in_league(league_name):
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    league_str = league_name.lower().replace(' ', '+')
    page = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query=" + league_str
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    league_url = pageSoup.find('a', {'title': league_name})['href']
    page = "https://www.transfermarkt.co.uk" + league_url
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    df_league = pd.read_html(str(pageSoup.find('table', {'class': 'items'})))[0]
    clubs = df_league['club.1'].to_list()
    if np.nan in clubs:
        clubs.remove(np.nan)
    return clubs


def find_all_players_with_pos_in_team(team):
    pattern = '[a-zA-Z0-9,.]+'
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    team_str = team.lower().replace(' ', '+')
    page = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query=" + team_str
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    teams = pageSoup.find_all("td", {"class": "hauptlink"})
    first_teams_a = str(teams[0].find_all("a")[0])
    idx_start = first_teams_a.find('a href=') + len('a href="')
    team_url = first_teams_a[idx_start:].split(" ")[0][:-1]
    page = "https://www.transfermarkt.co.uk" + team_url
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
    match = []
    records = pageSoup.find_all("td", {"class": "posrela"})
    records_market_value = pageSoup.find_all("td", {"class": "rechts hauptlink"})
    records_nationality = pageSoup.find_all("td", {"class": "zentriert"})

    for rec, rec_market_value, rec_nations in zip(records, records_market_value, records_nationality):
        dff = pd.read_html(str(rec))[0][1]
        name = dff.values[0]
        pos = dff.values[1]
        full_name = name.split('.')[0][:-1]


        market_value_raw = rec_market_value.text
        str_market_value = "".join(re.findall(pattern, str(market_value_raw)))

        nationalities = rec_nations.find_all("img", {"class": "flaggenrahmen"})
        nation_list = []
        for nation in nationalities:
            nation = nation['title']
            nation = "".join(re.findall(pattern, str(nation)))
            nation_list.append(nation)
        nations = ",".join(nation_list)
        match.append([full_name, pos, str_market_value, nations])


    return match





# OLD!!!!!
# def find_all_players_with_pos_in_team(team):
#     headers = {'User-Agent':
#                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
#     team_str = team.lower().replace(' ', '+')
#     page = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query=" + team_str
#     pageTree = requests.get(page, headers=headers)
#     pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
#     teams = pageSoup.find_all("td", {"class": "hauptlink"})
#     first_teams_a = str(teams[0].find_all("a")[0])
#     idx_start = first_teams_a.find('a href=') + len('a href="')
#     team_url = first_teams_a[idx_start:].split(" ")[0][:-1]
#     page = "https://www.transfermarkt.co.uk" + team_url
#     pageTree = requests.get(page, headers=headers)
#     pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
#     match = []
#     records = pageSoup.find_all("td", {"class": "posrela"})
#     for rec in records:
#         dff = pd.read_html(str(rec))[0][1]
#         name = dff.values[0]
#         pos = dff.values[1]
#         full_name = name.split('.')[0][:-1]
#         match.append([full_name, pos])
#     return match

# def wikipedia_scrapping(player_name):
#     wiki_res = wikipedia.search(player_name)
#     if len(wiki_res) > 0:
#         wiki_res = wiki_res[0]
#         player_name = wiki_res
#     else:
#         return None
#     html = wikipedia.page(wiki_res).html()
#     pageSoup = BeautifulSoup(html, 'html.parser')
#     pattern = '[a-zA-Z0-9 ,.-]+'
#     table = pageSoup.find('table', {'class': 'infobox vcard'})
#
#     df_player_data = pd.read_html(str(table))[0].set_index(0)[[1]].rename({1: 'data'}, axis=1)
#     data_of_interest = ['Position(s)', 'Date of birth', 'Place of birth', 'Height', 'Current team']
#     data_dict = dict()
#     for data in data_of_interest:
#         if data in list(df_player_data.index):
#             if data == 'Place of birth':
#                 val = df_player_data.loc[data].iloc[0].split(',')[-1]
#             elif data == 'Height':
#                 val = df_player_data.loc[data].iloc[0].split('(')[0]
#             elif data == 'Date of birth':
#                 val = df_player_data.loc[data].iloc[0].split()[-1]
#                 data = 'age'
#             else:
#                 val = df_player_data.loc[data].iloc[0]
#
#             data_dict[data] = re.findall(pattern, val)[0].strip()
#     data_dict['player_name'] = player_name
#     return data_dict

# def get_player_data(player: str):
#     # connect to the website
#     headers = {'User-Agent':
#                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
#
#     # find the player
#     player = player.replace('.', '')
#     player_str = player.lower().replace(' ', '+')
#     page = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query=" + player_str
#     pageTree = requests.get(page, headers=headers)
#     pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
#
#     players = pageSoup.find_all("td", {"class": "hauptlink"})
#     if len(players) == 0:
#         return None
#     selected_player = str(players[0])
#     len_href = len("a href=")
#     idx_start = selected_player.find('a href=')
#     selected_player = selected_player[idx_start + len_href + 1:]
#     idx_end = selected_player.find('"')
#     player_url = selected_player[:idx_end]
#
#     # Go to the player's page
#     page = "https://www.transfermarkt.co.uk" + player_url
#
#     pageTree = requests.get(page, headers=headers)
#     pageSoup = BeautifulSoup(pageTree.content, 'html.parser')
#     key = pageSoup.find_all("span", {"class": "info-table__content info-table__content--regular"})
#     value = pageSoup.find_all("span", {"class": "info-table__content info-table__content--bold"})
#     data_dict = dict()
#     # take the data to a dict
#     for i in range(len(key)):
#         # if str(key[i].text).strip() not in ('Name in home country:','Height:','Citizenship:'):#todo:delete this row later
#         data_dict[str(key[i].text).strip()[:-1]] = str(value[i].text).strip().replace("'", '')
#     if len(data_dict) == 0:
#         return None
#     team = pageSoup.find_all("span", {"class": "hauptpunkt"})
#     if len(team) > 0:
#         data_dict['team'] = team[0].text
#     market_val = pageSoup.find_all("div", {"class": "dataMarktwert"})
#     if len(market_val) > 0:
#         data_dict['market_value'] = market_val[0].text.strip().split()[0]
#     return data_dict


# sample usage

# data_dict = get_player_data("Dolev Haziza")
# print(data_dict)

# print(get_players_name_from_team_name(MAIN_TEAM))

