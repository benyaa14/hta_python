import mysql.connector
from main_functions import *
import streamlit as st
from nav_bar import *
from main import page_structure

MAX_BUDGET = 10.0
MAX_PLAYER_IN_POS = 10
MIN_PLAYER_IN_POS = 1
MIN_PLAYERS_IN_TEAM = 18
MAX_PLAYERS_IN_TEAM = 40


def input_budget():
    budget = st.slider('What is the transfers budget (M$)?', 0.0, MAX_BUDGET, 1.0, step=0.01)
    st.write("The budget is", budget, 'M$')
    return budget


def input_num(min_max: str, position: str, max_value, min_value, default_val):
    val = st.number_input(f"{position}: {min_max} players", min_value, max_value, default_val)
    return val


def app():
    cola, colb, colc = st.columns(page_structure)
    # ----------NAVBAR---------
    st.markdown(HEADER, unsafe_allow_html=True)

    st.markdown(CONTENT, unsafe_allow_html=True)
    # -------/NAVBAR/--------------
    with colb:
        header = st.container()
        with header:
            st.header('Input your data')
            st.subheader('You must enter the following data')
            budget = input_budget()
            d_min_max_pos = dict()
            st.subheader('Number of players in team')
            for type_ in ['Max', 'Min']:
                val = input_num(min_max=type_, position="All players in the team", max_value=MAX_PLAYERS_IN_TEAM,
                                min_value=MIN_PLAYERS_IN_TEAM,
                                default_val=23)
                d_min_max_pos[("team", type_)] = val

            for pos in POSITIONS:
                st.subheader(pos)
                for type_ in ['Max', 'Min']:
                    val = input_num(min_max=type_, position=pos, max_value=MAX_PLAYER_IN_POS,
                                    min_value=MIN_PLAYER_IN_POS, default_val=3)
                    d_min_max_pos[(pos, type_)] = val


app()
