from streamlit_gui import *
from nav_bar import HEADER, CONTENT
from main import page_structure
from main_functions import *
from stqdm import stqdm

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





