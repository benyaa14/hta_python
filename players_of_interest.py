from streamlit_gui import *
from nav_bar import HEADER, CONTENT
from main import page_structure
from main_functions import *
from stqdm import stqdm
import transfermarket_request as tr
from main_new import get_season_from_date
from players_comparison_app import return_new_rank
from statsmodels.tsa.api import SimpleExpSmoothing
now = dt.datetime.now()


MIN_GAMES_TO_RANK_A_PLAYER = 5
def get_player_df():
    if 'player_df' not in st.session_state:
        mydb,mycursor = connect_to_the_DB()
        player_df = read_all_table(mycursor, PLAYER_TABLE)
        st.session_state['player_df'] = player_df
    else:
        player_df = st.session_state['player_df']
    return player_df

def update_poi_column(selected_ids,players_df):
    players_df['poi'] = players_df['p_id'].apply(lambda x: x in selected_ids)

def find_players_to_update_poi(pos_player_df_new,pos_player_df_old):
    merged_player_df = pos_player_df_new.rename({'poi':'poi_new'},axis=1).merge(pos_player_df_old[['p_id','poi']].rename({'poi':'poi_old'},axis=1),how='left',on='p_id')
    return merged_player_df[merged_player_df['poi_new'].eq(merged_player_df['poi_old'])==False][['p_id','poi_new']].rename({'poi_new':'poi'},axis=1)

def update_column_poi_in_the_db(players_to_update_poi):
    mydb, mycursor = connect_to_the_DB()
    for i, row in stqdm(players_to_update_poi.iterrows(), total=players_to_update_poi.shape[0]):
        update_record_to_sql(mydb, mycursor, PLAYER_TABLE, 'poi', row['poi'], ['p_id'], [row['p_id']],
                             only_print_query=False)
    st.success("Updated DB")
    del st.session_state['player_df']

def show_players_selection(form,all_players,selected_players_in_the_past):
    selected_players = form.multiselect(
        'Select players',
        all_players, default=(selected_players_in_the_past['p_id'].astype(str) + '_' + selected_players_in_the_past[
            'p_name_transfer']).to_list())
    return selected_players

def get_all_player_ids_of_interest():
    mydb,mycursor = connect_to_the_DB()
    list_of_player_name_main_team_in_transfermarket = tuple(tr.get_players_name_from_team_name(MAIN_TEAM))
    df= read_from_table(mycursor,'player',f"select p_id,p_name_transfer from player where poi  = 1 OR p_name_transfer in {list_of_player_name_main_team_in_transfermarket}")
    disconnect_from_the_db(mycursor,mydb)
    return df['p_id'].to_list(),df['p_name_transfer'].to_list()

def get_players_dict_list(p_ids,p_names):
    d_p_id_to_p_name = dict()
    for p_id,p_name in stqdm(zip(p_ids,p_names),total=len(p_ids)):
        player_data = tr.get_player_data(p_name)
        d_p_id_to_p_name[p_id] = player_data
    return d_p_id_to_p_name

def calculate_player_rank_in_game(p_ids):
    mydb, mycursor = connect_to_the_DB()
    pig_df = read_from_table(mycursor, PLAYER_IN_GAME_TABLE,f"select * from {PLAYER_IN_GAME_TABLE} where player_id  in {tuple(p_ids)}")
    til_table = read_all_table(mycursor, TEAM_IN_LEAGUE_TABLE)
    league_df = read_all_table(mycursor, LEAGUE_TABLE)
    pig_df['season'] = pig_df['game_date'].apply(get_season_from_date)
    joined_df = pig_df.merge(til_table, left_on=['t_id', 'season'], right_on=['t_id', 'season_y'], how='left')
    joined_df[POSTERIOR_RANK] = joined_df.apply(lambda x: return_new_rank(x, league_df, POSTERIOR_RANK), axis=1)
    disconnect_from_the_db(mycursor,mydb)
    return joined_df

def calculate_final_player_score(pig_df, method,smoothing_level = 0.2):
    #todo: add instat index score too
    players_to_rank = list(pig_df['player_id'].unique())
    d_id_to_rank = dict()
    if method == 'exp_smoothing':
        for player_id in players_to_rank:
            data_dates_ranks = pig_df[pig_df['player_id'] == player_id].set_index('game_date')[POSTERIOR_RANK]
            data_dates_ranks.index = [i for i in range(len(data_dates_ranks))]
            if len(data_dates_ranks) >= MIN_GAMES_TO_RANK_A_PLAYER:
                fit = SimpleExpSmoothing(data_dates_ranks).fit(smoothing_level=smoothing_level, optimized=False)
                rank = fit.forecast(1).values[0]#.rename(alpha_str+str(smoothing_level))
            else:
                rank = None
            d_id_to_rank[player_id] = rank
    return d_id_to_rank

def add_p_ranke_to_data_dict(data_dict_list,p_ids_to_rank_dict):
    for player_id,rank in p_ids_to_rank_dict.items():
        rank_to_add = {'p_rank':rank}
        data_dict_list[player_id] = {**data_dict_list[player_id], **rank_to_add}
    return data_dict_list

def get_market_val(market_value_str):

    if 'm' in market_value_str:
      market_value = float(market_value_str.replace('m','')) * 1000000
    elif 'Th' in market_value_str:
      market_value = float(market_value_str.replace('Th','')) * 1000
    else:
      market_value = None
    return market_value

def append_lists_to_update_db(cols_to_update,values_to_update,col_name,val_name):
    cols_to_update.append(col_name)
    values_to_update.append(val_name)




def update_player_table_with_data_dict_list(data_dict_list):
    """
    "1":{
"Name in home country":"عبدالله جابر"
"Date of birth":"Feb 17, 1993"
"Place of birth":"Tayyibe"
"Age":"29"
"Height":"1,75 m"
"Citizenship":"Palästina Israel"
"Position":"Defender - Left-Back"
"Foot":"left"
"Current club":"Ihud Bnei Sakhnin"
"Joined":"Jul 1, 2021"
"Contract expires":"Jun 30, 2023"
"market_value":"225Th."
"p_rank":-0.3572788267003726
    :param data_dict_list:
    :return:
    """
    mydb,mycursor = connect_to_the_DB()
    DOB_dt = market_value = height_float = None
    st.info("Updating DB")
    for p_id, data_dict in stqdm(data_dict_list.items(),total=len(data_dict_list)):
        cols_to_update = []
        values_to_update = []
        DOB = data_dict.get("Date of birth")
        if DOB is not None:# or len(DOB) > 0 :
            if 'happy birthday' in DOB.lower():
                DOB_dt = dt.datetime.now().date()
            else:
                try:
                    DOB_dt = f"'{dt.datetime.strptime(DOB, '%b %d, %Y').date()}'"
                except:
                    DOB_dt = dt.datetime(1970,1,1)
            # cols_to_update.append('DOB')
            # values_to_update.append(DOB_dt)
            append_lists_to_update_db(cols_to_update,values_to_update,'DOB',DOB_dt)
        height_str = data_dict.get("Height")
        if height_str is not None:# or len(height_str) > 0:
            try:
                height_float = eval(height_str.replace(',','.').replace(' m',''))
            except:
                height_float = 999
            append_lists_to_update_db(cols_to_update,values_to_update,'height',height_float)

        foot = f"'{data_dict.get('Foot')}'"
        append_lists_to_update_db(cols_to_update, values_to_update, 'foot', foot)
        p_rank = data_dict.get("p_rank")
        if p_rank is not None and str(p_rank) !='nan':
            append_lists_to_update_db(cols_to_update, values_to_update, 'p_rank', p_rank)

        nationality = str(data_dict.get('Citizenship')).replace("'", "")
        nationality = f"'{nationality}'"
        append_lists_to_update_db(cols_to_update, values_to_update, 'nationality', nationality)

        market_value_str = data_dict.get("market_value")
        if market_value_str is not None:# or len(market_value_str)>0:
            market_value = get_market_val(market_value_str)
            append_lists_to_update_db(cols_to_update, values_to_update, 'market_value', market_value)

        rank_date = f"'{now.date()}'"
        append_lists_to_update_db(cols_to_update, values_to_update, 'last_rank_date', rank_date)
        try:
            update_record_to_sql_many_values(mydb,mycursor,PLAYER_TABLE,cols_to_update,
                                         values_to_update,['p_id'],[p_id],False)
        except:
            update_record_to_sql_many_values(mydb, mycursor, PLAYER_TABLE, cols_to_update,
                                             values_to_update, ['p_id'], [p_id], True)
            st.error(f"Dindn't update player_id = {p_id}")
    st.success('Updated DB')
    disconnect_from_the_db(mycursor,mydb)




def app():

    # ----------NAVBAR---------
    st.markdown(HEADER, unsafe_allow_html=True)
    st.markdown(CONTENT, unsafe_allow_html=True)
    col_a, col_b, col_c = st.columns(page_structure)
    player_df = get_player_df()
    with col_b:
        st.header('Select your POI')
        position = st.selectbox("Filter by position position", ['Select position', 'All positions'] + POSITIONS)
        if position != ALL_POSITIONS_STR:
            pos_player_df = player_df[player_df['position'] == position].copy()
        else:
            pos_player_df = player_df.copy()
        all_players = (pos_player_df['p_id'].astype(str) + '_' + pos_player_df['p_name_transfer']).to_list()
        form = st.form(key='poi')
        selected_players = show_players_selection(form,all_players,pos_player_df[pos_player_df['poi']==True].copy())
        submitted = form.form_submit_button()
        if submitted:
            if len(selected_players)> 0:
                update_poi_column([int(i.split('_')[0]) for i in  selected_players], pos_player_df)
            else:
                update_poi_column([], pos_player_df)
            update_column_poi_in_the_db(find_players_to_update_poi(pos_player_df,player_df[player_df['position'] == position].copy()))
        if st.button("Get model csv"):
            p_ids,p_names = get_all_player_ids_of_interest()
            data_dict_list = get_players_dict_list(p_ids,p_names)
            pig_updated_posterior_by_league = calculate_player_rank_in_game(p_ids)
            p_ids_to_rank_dict = calculate_final_player_score(pig_updated_posterior_by_league,method = 'exp_smoothing')
            data_dict_list = add_p_ranke_to_data_dict(data_dict_list,p_ids_to_rank_dict)
            # st.write(data_dict_list)
            update_player_table_with_data_dict_list(data_dict_list)
            df_to_download = download_players_csv()

            @st.cache
            def convert_df(df):
                return df.to_csv().encode('utf-8')

            csv = convert_df(df_to_download)
            st.download_button(
                "Click to Download The Candidates CSV",
                csv,
                f"Cands_to_optimize.csv",
                "text/csv",
                key='download-csv'
            )



def download_players_csv():
    mydb,mycursor = connect_to_the_DB()
    list_of_player_name_main_team_in_transfermarket = tuple(tr.get_players_name_from_team_name(MAIN_TEAM))
    df= read_from_table(mycursor,'player',f"select * from player where poi  = 1 OR p_name_transfer in {list_of_player_name_main_team_in_transfermarket}")
    # todo: arrange the dataframe: first non hapoel players and then hapoel
    df['is_foreign'] = df['nationality'].apply(lambda x: MAIN_NATIONALITY.lower() not in x.lower() if x is not None else None)
    pig_df = read_from_table(mycursor, PLAYER_IN_GAME_TABLE,
                             f"select player_id,game_date,t_id from {PLAYER_IN_GAME_TABLE} where player_id  in {tuple(df['p_id'].to_list())}")

    df = df.merge(pig_df.sort_values('game_date').groupby('player_id').agg({'t_id':'last'}).reset_index(),
             left_on='p_id',right_on='player_id',how='left')
    return df


