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
import matplotlib.pyplot as plt
from mplsoccer import Radar

MIN_GAMES_TO_VIS = 10

mydb = mysql.connector.connect(
    host=HOST, user=USER, password=PASSWORD, database=DB
)
mycursor = mydb.cursor()

pig_df = read_all_table(mycursor, PLAYER_IN_GAME_TABLE)
player_df = read_all_table(mycursor, PLAYER_TABLE)
df_weights = read_all_table(mycursor,WEIGHTS_TABLE)
til_table = read_all_table(mycursor, TEAM_IN_LEAGUE_TABLE)
league_df = read_all_table(mycursor, LEAGUE_TABLE)
mydb.close()
colors = ['#01c49d','#d80499','#c42a04','#89c401']


def generate_random_color():
    color = ["#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])
             for i in range(1)]
    return color[0]

def plot_lines(df, players_name: list, col_to_vis, league_df):#,colors):
    df = df[df['p_name'].isin(players_name)].copy()
    for rank in [LIKELIHOOD_RANK, POSTERIOR_RANK]:
        df[rank] = df.apply(lambda x: return_new_rank(x, league_df, rank), axis=1)
    df.sort_values('game_date', inplace=True)
    df['moving_avg_' + col_to_vis] = df.groupby('player_id')[col_to_vis].transform(
        lambda x: x.rolling(10,5).mean())

    fig = px.line(df, x='game_date', y='moving_avg_' + col_to_vis, color='p_name',template="simple_white",color_discrete_sequence=colors
                  ,color_discrete_map = {players_name[i]: colors[i] for i in range(len(players_name))})
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
    # colors = []
    for player_name in players_name:
        hist_data.append(df[df['p_name'] == player_name][col_to_vis].to_list())
        group_labels.append(player_name)
        # colors.append(generate_random_color())

    # Create distplot with curve_type set to 'normal'
    fig = ff.create_distplot(hist_data, group_labels, colors=colors,
                             show_rug=True,show_hist=False,histnorm = 'probability density')
    fig['layout'].update(xaxis=dict(title=col_to_vis))
    fig['layout'].update(yaxis=dict(title='Probability Density'))
    # Add title
    fig.update_layout(title_text='Players rating distplot',
                      legend=dict(
                          orientation="h",
                          yanchor="bottom",
                          y=1.02,
                          xanchor="right",
                          x=1
                      )
                      )
    table_data = df[df['p_name'].isin(players_name)].groupby(['position', 'p_name']).agg({col_to_vis: ['mean', 'std', 'size']}).round(decimals=2).reset_index()
    table_data.columns = ['Position','Name','Mean','Std','#Games']
    cols = table_data.columns
    fig_table = ff.create_table(table_data)
    # st.plotly_chart(fig_table)
    return fig,fig_table


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
    # mydb = mysql.connector.connect(
    #     host=HOST, user=USER, password=PASSWORD, database=DB
    # )
    # mycursor = mydb.cursor()
    selected_players = []
    btn_plot = False
    # ----------NAVBAR---------
    st.markdown(HEADER, unsafe_allow_html=True)

    st.markdown(CONTENT, unsafe_allow_html=True)
    # -------/NAVBAR/--------------
    # pig_df = read_all_table(mycursor, PLAYER_IN_GAME_TABLE)
    # player_df = read_all_table(mycursor, PLAYER_TABLE)
    # df_weights = read_all_table(mycursor,WEIGHTS_TABLE)

    pig_df['season'] = pig_df['game_date'].apply(
        lambda x: x.year if 8 <= x.month <= 12 else x.year - 1)
    # til_table = read_all_table(mycursor, TEAM_IN_LEAGUE_TABLE)
    # league_df = read_all_table(mycursor, LEAGUE_TABLE)
    joined_df = pig_df.merge(player_df[['p_id', 'p_name']], how='left', left_on='player_id', right_on='p_id').merge(
        til_table, left_on=['opponent_id', 'season'], right_on=['t_id', 'season_y'], how='left')
    col1_1, col2_1, col3_1 = st.columns(page_structure)
    btn_plot = False
    with col2_1:
        players_comp = st.container()
        with players_comp:
            st.header('Players ranking comparison')
            st.write('Please select players to compare their performance')
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
                btn_plot = st.button('Plot')
    vis_container = st.container()
    with vis_container:

        if btn_plot:
            fig_line = plot_lines(joined_df, selected_players, col_to_vis, league_df)  # ,colors)

            fig_dist,fig_table = plot_dists(joined_df, selected_players, col_to_vis, league_df)
            with col2_1:
                st.plotly_chart(fig_table)
            col_1, col_2 = st.columns(2)
            with col_1:
                st.plotly_chart(fig_dist)
            with col_2:
                st.plotly_chart(fig_line)
            radar_container = st.container()
            with radar_container:
                col1_1, col2_1, col3_1 = st.columns(page_structure)
                with col2_1:
                    plot_radar(joined_df, selected_players, position, df_weights)





def max_attributes(df_weights,pos, k):
    df = df_weights.sort_values(by=pos, ascending=False).head(k).copy()
    return df[['attribute', pos]]

def get_player_stat_to_radar(joined_df,selected_player_name,params):
    return joined_df[joined_df['p_name']==selected_player_name][params].mean().to_list()

def get_low_high_values_lists(first_player,second_player):
    low = []
    high = []

    for f, s in zip(first_player, second_player):
            if min(f,s)>10:
                low.append(min(f,s)-5)
            else:
                low.append(min(f,s)-0.2)
            high.append(max(f,s)+0.2)
    return low,high


def radar_mosaic(radar_height=0.915, title_height=0.06, figheight=14):
    """ Create a Radar chart flanked by a title and endnote axes.

    Parameters
    ----------
    radar_height: float, default 0.915
        The height of the radar axes in fractions of the figure height (default 91.5%).
    title_height: float, default 0.06
        The height of the title axes in fractions of the figure height (default 6%).
    figheight: float, default 14
        The figure height in inches.

    Returns
    -------
    fig : matplotlib.figure.Figure
    axs : dict[label, Axes]
    """
    if title_height + radar_height > 1:
        error_msg = 'Reduce one of the radar_height or title_height so the total is ≤ 1.'
        raise ValueError(error_msg)
    endnote_height = 1 - title_height - radar_height
    figwidth = figheight * radar_height
    figure, axes = plt.subplot_mosaic([['title'], ['radar'], ['endnote']],
                                      gridspec_kw={'height_ratios': [title_height, radar_height,
                                                                     endnote_height],
                                                   # the grid takes up the whole of the figure 0-1
                                                   'bottom': 0, 'left': 0, 'top': 1,
                                                   'right': 1, 'hspace': 0},
                                      figsize=(figwidth, figheight))
    axes['title'].axis('off')
    axes['endnote'].axis('off')
    return figure, axes

def radar_setups(radar,first_player_data, second_player_data,first_player_name,second_player_name):
    # creating the figure using the function defined above:
    fig, axs = radar_mosaic(radar_height=0.915, title_height=0.06, figheight=8)

    # plot radar
    radar.setup_axis(ax=axs['radar'])  # format axis as a radar
    rings_inner = radar.draw_circles(ax=axs['radar'], facecolor='#ffb2b2', edgecolor='#fc5f5f')
    radar_output = radar.draw_radar_compare(first_player_data, second_player_data, ax=axs['radar'],
                                            kwargs_radar={'facecolor': '#00f2c1', 'alpha': 0.6},
                                            kwargs_compare={'facecolor': '#d80499', 'alpha': 0.6})
    radar_poly, radar_poly2, vertices1, vertices2 = radar_output
    range_labels = radar.draw_range_labels(ax=axs['radar'], fontsize=13)
    param_labels = radar.draw_param_labels(ax=axs['radar'], fontsize=13)
    axs['radar'].scatter(vertices1[:, 0], vertices1[:, 1],
                         c='#00f2c1', edgecolors='#6d6c6d', marker='o', s=125, zorder=2)
    axs['radar'].scatter(vertices2[:, 0], vertices2[:, 1],
                         c='#d80499', edgecolors='#6d6c6d', marker='o', s=125, zorder=2)

    # adding the endnote and title text (these axes range from 0-1, i.e. 0, 0 is the bottom left)
    # Note we are slightly offsetting the text from the edges by 0.01 (1%, e.g. 0.99)
    # endnote_text = axs['endnote'].text(0.99, 0.5, 'Inspired By: StatsBomb / Rami Moghadam', fontsize=15,
    #                                   fontproperties=robotto_thin.prop, ha='right', va='center')
    title1_text = axs['title'].text(0.01, 0.65, first_player_name, fontsize=25, color='#01c49d', ha='left', va='center')
    # title2_text = axs['title'].text(0.01, 0.25,team_first_player_name, fontsize=20,ha='left', va='center', color='#01c49d')
    title3_text = axs['title'].text(0.99, 0.65, second_player_name, fontsize=25, ha='right', va='center',
                                    color='#d80499')
    st.pyplot(fig)
    # title4_text = axs['title'].text(0.99, 0.25, team_second_player_name, fontsize=20,ha='right', va='center', color='#d80499')

def plot_radar(joined_df,selected_players,position,df_weights,k=10):
    if len(selected_players) < 2 or position ==ALL_POSITIONS_STR:
        st.info('To plot radar plot you must select 2 players in the same position')
        return None
    elif len(selected_players) >= 2:
        if len(selected_players) > 2:
            st.info('Radar plot takes the first 2 players in the list you selected')
        selected_players = selected_players[:2]

    df_top_atts =  max_attributes(df_weights,position,k)
    params = df_top_atts['attribute'].to_list()

    first_player_data = get_player_stat_to_radar(joined_df,selected_players[0],params)
    second_player_data = get_player_stat_to_radar(joined_df,selected_players[1],params)

    low, high = get_low_high_values_lists(first_player_data,second_player_data)

    radar = Radar(params, low, high,
                  # whether to round any of the labels to integers instead of decimal places
                  round_int=[False]*k,
                  num_rings=4,  # the number of concentric circles (excluding center circle)
                  # if the ring_width is more than the center_circle_radius then
                  # the center circle radius will be wider than the width of the concentric circles
                  ring_width=0.7, center_circle_radius=0.7)

    radar_setups(radar,first_player_data,second_player_data,selected_players[0],selected_players[1])
