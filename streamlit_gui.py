# tutorial https://www.youtube.com/watch?v=-IM3531b1XU&list=PLM8lYG2MzHmTATqBUZCQW9w816ndU0xHc&index=1

import streamlit as st


def show_sidebar():
    sideb = st.sidebar
    upload_players = sideb.button("Upload New Data")
    opt_model = sideb.button("Optimization Model")
    change_weights = sideb.button("Change Weights")
    rate_players = sideb.button("Rate Players")
    league_rate_modification = sideb.button("League Rate Modification")
    screens = [upload_players, opt_model, change_weights, rate_players, league_rate_modification]
    return screens


def containers():
    """Generate 5 containers"""
    header = st.container()
    player_in_game = st.container()
    update_weights = st.container()
    rank_players = st.container()
    # league_reg = st.container()
    return header, player_in_game, update_weights, rank_players  # ,league_reg

# copy to the terminal:
# 1.export LC_ALL=en_US.UTF-8
# 2.streamlit run streamlit_gui.py
# 3.stop execution of process with shortcut CTRL + C
