from streamlit_gui import *
from nav_bar import HEADER, CONTENT
from main import page_structure, show_st_image
from main_functions import *
import mysql.connector
import random
import plotly.figure_factory as ff
import numpy as np
import json
import ast
import plotly.express as px
import plotly.graph_objects as go

MIN_GAMES_TO_VIS = 10


def generate_random_color():
    color = ["#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])
             for i in range(1)]
    return color[0]

def plot_lines(df, players_name: list, col_to_vis, league_df,colors):
    df = df[df['p_name'].isin(players_name)].copy()
    for rank in [LIKELIHOOD_RANK, POSTERIOR_RANK]:
        df[rank] = df.apply(lambda x: return_new_rank(x, league_df, rank), axis=1)
    fig = px.line(df, x='game_date', y=col_to_vis, color='p_name',template="simple_white",color_discrete_sequence=colors)
    fig.update_layout(
        title_text="Time series ranking"
    )
    fig.update_xaxes(
        dtick="M1",
        tickformat="%b\n%Y",
        ticklabelmode="period")
    # Add range slider
    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label="1m",
                         step="month",
                         stepmode="backward"),
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    )

    return fig

def plot_dists(df, players_name: list, col_to_vis, league_df):
    df = df[df['p_name'].isin(players_name)].copy()
    for rank in [LIKELIHOOD_RANK, POSTERIOR_RANK]:
        df[rank] = df.apply(lambda x: return_new_rank(x, league_df, rank), axis=1)
    hist_data = []
    group_labels = []
    colors = []
    for player_name in players_name:
        hist_data.append(df[df['p_name'] == player_name][col_to_vis].to_list())
        group_labels.append(player_name)
        colors.append(generate_random_color())

    # Create distplot with curve_type set to 'normal'
    fig = ff.create_distplot(hist_data, group_labels, colors=colors,
                             show_rug=True)

    # Add title
    fig.update_layout(title_text='Players rating dist')
    table_data = df[df['p_name'].isin(players_name)].groupby(['position', 'p_name']).agg({col_to_vis: ['mean', 'std', 'size']}).round(decimals=2).reset_index()
    table_data.columns = ['Position','Name','Mean','Std','#Games']
    cols = table_data.columns
    fig_table = ff.create_table(table_data)
    st.plotly_chart(fig_table)
    return fig,colors


def return_new_rank(row, league_df, rank_col):
    if str(row['l_id']) == 'nan':
        return row[rank_col]
    league_row = league_df[league_df['l_id'] == row['l_id']]
    correction_dict = league_row['correction_to_il'].iloc[0]
    if correction_dict is None:
        return row[rank_col]
    else:
        return ast.literal_eval(correction_dict)[row['position']] + row[rank_col]


def app():
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    selected_players = []
    btn_plot = False
    # ----------NAVBAR---------
    st.markdown(HEADER, unsafe_allow_html=True)

    st.markdown(CONTENT, unsafe_allow_html=True)
    # -------/NAVBAR/--------------
    pig_df = read_all_table(mycursor, PLAYER_IN_GAME_TABLE)
    player_df = read_all_table(mycursor, PLAYER_TABLE)
    pig_df['season'] = pig_df['game_date'].apply(
        lambda x: x.year if 8 <= x.month <= 12 else x.year - 1)
    til_table = read_all_table(mycursor, TEAM_IN_LEAGUE_TABLE)
    league_df = read_all_table(mycursor, LEAGUE_TABLE)
    joined_df = pig_df.merge(player_df[['p_id', 'p_name']], how='left', left_on='player_id', right_on='p_id').merge(
        til_table, left_on=['opponent_id', 'season'], right_on=['t_id', 'season_y'], how='left')
    col1_1, col2_1, col3_1 = st.columns(page_structure)
    with col2_1:
        players_comp = st.container()
        with players_comp:
            st.header('Players comparison')
            st.write('Please select players to compare their rating dist')
            position = st.selectbox("Filter by position position", ['Select position', 'All positions'] + POSITIONS)
            if position != ALL_POSITIONS_STR:
                joined_df = joined_df[joined_df['position'] == position]
            all_players = joined_df['p_name'].value_counts()
            all_players = list(all_players[all_players > MIN_GAMES_TO_VIS].index)
            selected_players = st.multiselect(
                'Select players',
                all_players)

            col_to_vis = st.selectbox(
                'Select column to visualize',
                [POSTERIOR_RANK] + list(joined_df.columns))
            if len(selected_players) > 0:
                btn_plot = st.button('Click to plot')
                if btn_plot:
                    fig_dist,colors = plot_dists(joined_df, selected_players, col_to_vis, league_df)
                    st.plotly_chart(fig_dist)
                    fig_line = plot_lines(joined_df, selected_players, col_to_vis, league_df,colors)
                    st.plotly_chart(fig_line)


    mydb.close()