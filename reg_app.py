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


def return_league_names_list(df):
    return df['l_name'].to_list()


def get_leagues_without_correction(df):
    return df[df['correction_to_il'].isnull()]['l_name'].to_list()


def app():
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    df_league = read_all_table(mycursor, LEAGUE_TABLE)
    # ----------NAVBAR---------
    st.markdown(HEADER, unsafe_allow_html=True)

    st.markdown(CONTENT, unsafe_allow_html=True)
    # -------/NAVBAR/--------------

    re_col1, re_col2, re_col3 = st.columns(page_structure)
    with re_col2:
        league_reg = st.container()
    btn_reg = False
    with league_reg:
        correction_to_il = l_id = None
        st.header('League rate adjustment')
        show_st_image(img_file_name='reg.jpeg', caption="Let the machine do the work")

        st.subheader(
            "Select the position and league you would like to adjust\n")
        st.info("Leagues without any adjustment:" + str(get_leagues_without_correction(df_league)))
        league = st.selectbox("Select league", ['Select league'] + return_league_names_list(df_league))
        if league != 'Select league':
            missing_pos = lr.find_missing_corrected_positions_in_the_league(mycursor, league)
            if len(missing_pos) > 0:
                st.write(f"Missing positions for the {league} = {missing_pos}")
            else:
                st.write(f"All positions were updated to the {league}")
            position = st.selectbox("Select position", ['Select position', 'All positions'] + POSITIONS)
            btn_res = st.checkbox('Update DB')
            btn_reg = st.button('Run regression algorithm')
    if btn_reg:
        correction_to_il, l_id = lr.run_regression_app_test(mydb, mycursor, league, position, btn_res)
        if position != ALL_POSITIONS_STR and correction_to_il is not False:
            st.success(f"Correction = {round(correction_to_il, 3)} || League = '{league}' || Position = {position}")
        elif position == ALL_POSITIONS_STR:
            str_update = f"League = '{league}'  \n"
            for correction_to_il_, position in zip(correction_to_il, POSITIONS):
                str_update += f"Position = {position}, Correction = {correction_to_il_} || \n"
            st.success(str_update)
    mydb.close()
