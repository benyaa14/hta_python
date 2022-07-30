import mysql.connector
from stqdm import stqdm
from streamlit_gui import *
from main_functions import *
from main import page_structure, HEADER, CONTENT
import transfermarket_request as tr
import json

now = dt.datetime.now()
def run_merge_teams_algo(mydb, mycursor, teams_to_merge_df):
    ids = tuple(teams_to_merge_df['t_id'].unique())
    query = f"""
    SELECT opponent_id as t_id ,COUNT(opponent_id ) as cnt
    from player_in_game pig 
    where opponent_id in {ids}
    GROUP by opponent_id
    ORDER by cnt DESC 
    """
    mycursor.execute(query)
    df_cnt = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    main_team = ids[0]
    if len(df_cnt) > 0:
        main_team = df_cnt['t_id'].iloc[0]
    ids = list(ids)
    if len(ids) > 1:
        ids.remove(main_team)
        for id in ids:
            update_record_to_sql(mydb, mycursor, 'player_in_game', 'opponent_id', main_team, ['opponent_id'], [id],
                                 False)
            print("Updated player_in_game")
            drop_query = f"delete from team_in_league WHERE t_id = {id}"
            mycursor.execute(drop_query)
            mydb.commit()
            print("Updated team_in_league")
            drop_query = f"delete from teams WHERE t_id = {id}"
            mycursor.execute(drop_query)
            mydb.commit()
            print("Updated teams")


def app():
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    cola, colb, colc = st.columns(page_structure)
    # ----------NAVBAR---------
    st.markdown(HEADER, unsafe_allow_html=True)

    st.markdown(CONTENT, unsafe_allow_html=True)
    # -------/NAVBAR/--------------
    with colb:
        header, selection, update_teams, rank_players = containers()
        player_data = st.container()

        with header:
            from PIL import Image

            image = Image.open('opteamize.png')

            st.image(image, caption='Connecting the dots',use_column_width=True)
            st.title('Update manually fields to the DB')

        with selection:
            query = "show tables;"
            mycursor.execute(query)
            tables = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            tables_chbox = st.selectbox("Select table", tables['Tables_in_hta_project']
                                        .apply(lambda x: x.decode("utf-8")))

            query = f"SHOW COLUMNS from {tables_chbox};"
            mycursor.execute(query)
            cols_df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            columns = cols_df['Field'].to_list()
            keys = cols_df[cols_df['Key'].apply(lambda x: x.decode("utf-8")) == 'PRI']
            keys_list = keys['Field'].to_list()
            st.write("Where:")
            st.write("(If one of the keys is string or date nest it with: ' ')")
            keys_values = []
            for key_ in keys_list:
                keys_values.append(st.text_input(key_, 0))

            query_selected_rec = f"SELECT * from {tables_chbox} where "
            i = 0
            for key_, value_ in zip(keys_list, keys_values):
                if i == 0:
                    query_selected_rec += f"{key_} = {value_}"
                else:
                    query_selected_rec += f" and {key_} = {value_}"
                i += 1

            try:
                mycursor.execute(query_selected_rec)
                res_df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
                st.write(res_df)
            except:
                st.error("Problematic query")

            cols_chbox = st.selectbox("Select column to set new value", columns)
            type_ = cols_df[cols_df['Field'] == cols_chbox]['Type'].iloc[0].decode("utf-8")

            # mycursor.execute(f"SELECT * FROM {tables_chbox} where ")
            # table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)

            value_to_set = st.text_input('Your new value to update', 0)
            if type_ == 'int':
                value_to_set = int(value_to_set)
            elif type_ == 'float':
                value_to_set = float(value_to_set)

            cond = query_selected_rec.split('where')[-1]
            btn_update = st.button('Apply')
            if btn_update:
                query = f'UPDATE {tables_chbox} SET {cols_chbox} = "{value_to_set}" WHERE ' + cond
                mycursor.execute(query)
                mydb.commit()

                if tables_chbox == TEAM_TABLE_NAME:  # We want to let us know that we updated this field
                    query = f"UPDATE {tables_chbox} SET transfer_added_manually = 1 WHERE " + cond
                    mycursor.execute(query)
                    mydb.commit()
                st.success("Updated DB")

        with update_teams:
            st.title('Update teams data')
            st.subheader("Find the same teams")
            query_same_teams = """
                SELECT * from teams t2 where transfermarket_team_name in (select  transfermarket_team_name
                from teams t 
                group by transfermarket_team_name 
                HAVING count(transfermarket_team_name)>1)
                ORDER by transfermarket_team_name 
            """
            mycursor.execute(query_same_teams)
            same_teams_df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            if len(same_teams_df) > 0:
                # for i,team in enumerate(list(same_teams_df['transfermarket_team_name'].unique())):
                team = same_teams_df.sample(1)['transfermarket_team_name'].to_list()
                if len(team) > 0:
                    # team = list(same_teams_df['transfermarket_team_name'].unique())[i]
                    team = team[0]
                    st.write(f"Is this the same team? ")
                    st.write(same_teams_df[same_teams_df['transfermarket_team_name'] == team])
                    merge_teams = st.button(f'Merge teams')
                    if merge_teams:
                        run_merge_teams_algo(mydb, mycursor,
                                             same_teams_df[same_teams_df['transfermarket_team_name'] == team])
                    dont_merge_teams = st.button(f'Not the same teams')
                    if dont_merge_teams:
                        i += 1
            else:
                st.info("No teams to match")

            st.subheader("Add additional data to teams")
            all_teams = st.checkbox(
                "Would you like to match all teams? If you won't click this button, it will match only teams that were not matched")
            btn_update_team = st.button('Update team table')
            if btn_update_team:
                teams_to_tag_manually = tr.run_team_name_matching(match_all_teams=all_teams)
                st.write("Teams that you should tag manually:", teams_to_tag_manually)

            query = """
            select * 
            from teams t 
            where t_id not in (SELECT distinct(t_id) from team_in_league til2)
            """
            mycursor.execute(query)
            teams_that_are_not_in_til = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write("These teams do not exist in 'team in league' table", teams_that_are_not_in_til,
                     "would you like to run for only these team? ")
            teams_id = []
            btn_selected_teams = st.checkbox(
                "Yes (If you won't select this check box it will run the process for all teams. this would take a while")

            btn_update_til = st.button('Update "team_in_league" table')
            if btn_update_til:
                if btn_selected_teams:
                    teams_id = list(teams_that_are_not_in_til['t_id'].unique())
                tr.run_team_in_league_matching(teams_id)

        with player_data:
            st.title('Update player static data')
            st.subheader('Transfermarket.com')
            player_df = read_all_table(mycursor, PLAYER_TABLE)
            all_players = list(player_df['p_name'].unique())
            selected_players = st.multiselect(
                'Select players',
                all_players)

            btn_all_players = st.checkbox(
                f"Run for all players in the DB ({len(all_players)} players)")
            if btn_all_players:
                selected_players = all_players
            if len(selected_players) > 0:
                btn_player_data = st.button('Run data scrapping')
                if btn_player_data:
                    for player in stqdm(selected_players):
                        transfermarket_data = tr.get_player_data(player)
                        # print(transfermarket_data)
                        if transfermarket_data is not None and str(transfermarket_data) != 'nan':
                            if 'Name in home country:' in transfermarket_data:
                                del transfermarket_data['Name in home country:']
                            # transfermarket_data = json.dumps(transfermarket_data)# todo: uncomment if you want to save tho whole dict
                        else:
                            transfermarket_data = 'Null'
                        p_id = player_df[player_df['p_name'] == player].iloc[0]['p_id']

                        # todo: we can uncomment this if we want the nationality separatly:

                        # nationality = None
                        # if type(transfermarket_data) == dict:
                        #     nationality = "Other"
                        #     if 'Israel' in transfermarket_data.get('Citizenship'):
                        #         nationality = "Israel"
                        #     elif transfermarket_data.get('Citizenship') is None:
                        #         nationality = None
                        #
                        #     market_value = transfermarket_data.get('market_value')
                        #     team = transfermarket_data.get('Current club')

                        #
                        # update_record_to_sql(mydb, mycursor, 'player', 'nationality', f'"{nationality}"',
                        #                      ['p_id'], [p_id], False)

                        # update_record_to_sql_many_values(mydb, mycursor,PLAYER_TABLE,['nationality','market_value','team_transfermarket'],[nationality,market_value,team],
                        #                                  ['p_id'],[p_id],True)
                        update_record_to_sql(mydb, mycursor, 'player', 'transfermarket_data',
                                             f""" "{transfermarket_data}" """,
                                             ['p_id'], [p_id], False)
                        mycursor.close()

            st.subheader('Scrap to enhance players transfermarket table')
            all_leagues = read_all_table(mycursor, LEAGUE_TABLE)['l_name_transfermarket'].to_list()
            selected_leagues = st.multiselect(
                'Select leagues',
                ['Select', 'All leagues'] + all_leagues)
            if selected_leagues == 'All leagues':
                selected_leagues = all_leagues
            if len(selected_leagues) > 0:
                btn_all_players_from_league_data = st.button('Run data scrapping to all players in league(s)')
                if btn_all_players_from_league_data:
                    df_players_transfermarket = pd.DataFrame(columns=['name', 'position','market_value','nationality', 'team', 'league'])
                    # old_players_transfermarket_df = read_all_table(mycursor, PLAYERS_TRANSFERMARKET_TABLE)
                    for league in stqdm(selected_leagues):
                        teams_in_league = tr.find_all_teams_in_league(league)
                        for team in stqdm(teams_in_league):
                            players_in_team = tr.find_all_players_with_pos_in_team(team)

                            tmp_df = pd.DataFrame(players_in_team, columns=['name', 'position','market_value','nationality'])
                            tmp_df['team'] = team
                            tmp_df['league'] = league
                            df_players_transfermarket = pd.concat([df_players_transfermarket, tmp_df])
                    df_players_transfermarket['instat_position'] = df_players_transfermarket['position'].apply(
                                lambda x: DICT_TRANSFERMARKET_POS_TO_INSTAT_POS.get(x))
                    df_players_transfermarket.reset_index(inplace=True)
                    df_players_transfermarket.drop_duplicates(inplace=True,subset=['name','position','team'])
                    df_players_transfermarket.drop('index', inplace=True, axis=1)
                    # new_players_transfermarket_df = pd.concat(
                    #             [old_players_transfermarket_df, df_players_transfermarket])
                    # to_insert_players_transfermarket_df = new_players_transfermarket_df.drop('index', axis=1).drop_duplicates()
                    df_players_transfermarket['date_downloaded'] = now
                    update_sql_table(PLAYERS_TRANSFERMARKET_TABLE, df_players_transfermarket.reset_index(),'index',if_exists='append')
                    st.success('Done')
                    mydb.close()





