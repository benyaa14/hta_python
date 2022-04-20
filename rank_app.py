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
from main import page_structure, show_st_image
from config import *

now = dt.datetime.now().date()


def app():
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    # ----------NAVBAR---------
    st.markdown(HEADER, unsafe_allow_html=True)

    st.markdown(CONTENT, unsafe_allow_html=True)
    # -------/NAVBAR/--------------
    rank_players = st.container()
    col1_1, col2_1, col3_1 = st.columns(page_structure)
    with col2_1:
        rank_players = st.container()
        with rank_players:
            st.header('Rank new players')
            show_st_image(img_file_name='rank.jpeg')
            cnt_games_with_no_rank = read_from_table(mycursor, PLAYER_IN_GAME_TABLE,
                                                     f"""SELECT count(*) as cnt from player_in_game pig where {GAME_RANK_COL} 
                                                     is NULL or {LIKELIHOOD_RANK} is NULL or {POSTERIOR_RANK} is Null""")[
                'cnt'].iloc[0]

            st.write(f'The algorithm will rank players without any rank. ({cnt_games_with_no_rank} games)')
            btn_rank_all_players = st.checkbox(
                'Would you like to rank all players to the DB? (this action could take few moments)')
            btn_rank = st.button('Run ranking algorithm')
            if btn_rank:
                num_of_records_updated = rp.run_rate_players_app(btn_rank_all_players)
                st.success(f"{num_of_records_updated} records were updated")
    mydb.close()


