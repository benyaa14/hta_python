import mysql.connector
import json
import plotly.express as px
import ast
from sympy.solvers import solve
from sympy import Symbol
import streamlit as st
from main_functions import *

# # If config works - delete------------------
# INSTAT_INDEX = 'instat_index'
# SELECTION_ERROR = 'selection_error'
# MAIN_LEAGUE = "Ligat ha'Al"
# POSITIONS = ["LD", "LM", "RM", "RD", "DM", "CM", "CD", "F"]
# LEAGUE_TABLE = 'league'
# PLAYER_IN_GAME_TABLE = 'player_in_game'
# WEIGHTS_TABLE = 'att_to_weight'
# TEAM_IN_LEAGUE_TABLE = 'team_in_league'
# GAME_RANK_COL = 'game_rank'
# POSTERIOR_RANK = 'game_rank_posterior'
# LIKELIHOOD_RANK = 'game_rank_likelihood'
# PRIMARY_KEYS_PLAYER_IN_GAME = ["player_id","game_date"]
# POSITION_COL = 'position'
# REDUCE_RATING_COLUMNS = ['red_cards']
# LEAGUE_ID_COL = 'l_id'
# ALL_POSITIONS_STR ="All positions"
# ---------------------------------------------


now = dt.datetime.now().date()
HOST = "hta-project.cf9mllj1rhry.us-east-2.rds.amazonaws.com"
USER = 'Sagi'
PASSWORD = "HTAproject2022"
DB = 'hta_project'
# mydb = mysql.connector.connect(
#     host=HOST, user=USER, password=PASSWORD, database=DB
# )
# mycursor = mydb.cursor()
NIN_UNIQUE_PLAYERS = 3
MIN_GAMES = 30


# def read_all_table(mycursor, table_name):
#     mycursor.execute(f"SELECT * FROM {table_name}")
#     table = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
#     return table
#
#
# def update_record_to_sql(mydb, mycursor, table_name, col_name_to_set, value_to_update, index_cols: list,
#                          index_values: list, only_print_query=False):
#     query = f"UPDATE {table_name} set {col_name_to_set} = {value_to_update} WHERE "
#     i = 0
#     for index_col, index_value in zip(index_cols, index_values):
#         if i > 0:
#             query += " AND "
#         if type(index_value) == dt.date:
#             query += f"{index_col} = '{index_value}'"
#         else:
#             query += f"{index_col} = {index_value}"
#         i += 1
#     if only_print_query:
#         print(query)
#     else:
#         mycursor.execute(query)
#         mydb.commit()


def find_diff_x_axis(slop_il_league, const_il_league, const_new_league):
    x = Symbol('x')
    sol1 = solve(slop_il_league * x + const_il_league, x)[0]
    sol2 = solve(slop_il_league * x + const_new_league, x)[0]
    diff_x = float(sol1 - sol2)
    return diff_x


def show_regression(all_games_df, league_id, position_id, plot_to_ui, ipl_league_id=0, ligat_haal_name="Ligat ha'Al"):
    position_df = all_games_df[
        (all_games_df['position'] == position_id) & (all_games_df['l_id'].isin([league_id, ipl_league_id]))].copy()

    print(len(position_df[position_df['l_id'] == league_id]))
    if len(position_df[position_df['l_id'] == league_id]) < MIN_GAMES:
        st.error('There are not enough games in this league for this position in order to make regression')
        return False
    if position_df[position_df['l_id'] == league_id]['player_id'].nunique() < NIN_UNIQUE_PLAYERS:
        st.error('There are not enough unique players in this league for this position in order to make regression')
        return False

    col1_1, col1_2 = st.columns(2)
    fig_before = px.scatter(position_df, x=LIKELIHOOD_RANK, y=INSTAT_INDEX, color="l_name", trendline="ols")
    # with col1_2:
    #     st.plotly_chart(fig_before, use_container_width=True)
    results_before = px.get_trendline_results(fig_before)
    ligat_haal_index = results_before[results_before['l_name'] == ligat_haal_name].index

    const_il_league, slop_il_league = list(results_before.iloc[ligat_haal_index]['px_fit_results'].iloc[0].params)

    new_league_idx = 1
    if ligat_haal_index == 1:
        new_league_idx = 0
    # print(results.iloc[new_league_idx].values[1])
    const_new_league, slop_new_league = list(results_before.iloc[new_league_idx].values[1].params)
    diff_x = find_diff_x_axis(slop_il_league, const_il_league, const_new_league)
    position_df['new_rank'] = position_df.apply(
        lambda x: x[LIKELIHOOD_RANK] if x['l_id'] == ipl_league_id else x[LIKELIHOOD_RANK] + diff_x, axis=1)

    fig_together_before = px.scatter(position_df, x=LIKELIHOOD_RANK, y=INSTAT_INDEX, trendline="ols")
    results_together_before = px.get_trendline_results(fig_together_before)

    fig_after = px.scatter(position_df, x="new_rank", y=INSTAT_INDEX, color="l_name", trendline="ols")
    col2_1, col2_2 = st.columns(2)

    fig_together_after = px.scatter(position_df, x="new_rank", y=INSTAT_INDEX, trendline="ols")

    results_together_after = px.get_trendline_results(fig_together_after)

    if plot_to_ui:
        with col1_1:
            st.subheader('Before')
            st.plotly_chart(fig_before, use_container_width=True)
        with col1_2:
            st.subheader('After')
            st.plotly_chart(fig_after, use_container_width=True)
        with col2_1:
            st.write(results_together_before.px_fit_results.iloc[0].summary())
        with col2_2:
            st.write(results_together_after.px_fit_results.iloc[0].summary())

    return diff_x


def run_regression_correction_to_il(league, position, leagues: pd.DataFrame, all_games: pd.DataFrame,
                                    til_table: pd.DataFrame, plot_to_ui):
    l_id = leagues[leagues['l_name'] == league][LEAGUE_ID_COL].iloc[0]
    all_games['season'] = all_games['game_date'].apply(
        lambda x: x.year if 8 <= x.month <= 12 else x.year - 1)  # Create year column to merge with team_in_league table
    all_games_with_league = all_games.merge(til_table, left_on=['opponent_id', 'season'], right_on=['t_id', 'season_y'],
                                            how='left').merge(leagues[[LEAGUE_ID_COL, 'l_name']], how='left', on='l_id')

    correction_to_il = show_regression(all_games_with_league, l_id, position, plot_to_ui=plot_to_ui)

    return correction_to_il, l_id


def apply_regression_to_db(mydb, mycursor, correction_to_il, l_id, position):
    if correction_to_il is not False:
        leagues = read_all_table(mycursor, LEAGUE_TABLE)
        league_to_implement = leagues.set_index('l_id').loc[l_id]
        old_correction_to_il = league_to_implement.loc['correction_to_il']
        if old_correction_to_il is None:
            old_correction_to_il = dict()
        else:
            old_correction_to_il = ast.literal_eval(old_correction_to_il)
        old_correction_to_il[position] = correction_to_il
        new_correction_to_il = json.dumps(old_correction_to_il)
        update_record_to_sql(mydb, mycursor, table_name='league', col_name_to_set='correction_to_il',
                             value_to_update=json.dumps(new_correction_to_il), index_cols=['l_id'],
                             index_values=[l_id], only_print_query=False)
        print(f"Updated '{position}' position to {LEAGUE_TABLE} table")
    else:
        print(f"'{position}' position: There is not correction to il")


def run_regression_app_test(mydb, mycursor, league, position, update_db=False):
    if league == MAIN_LEAGUE:
        st.error("Please change the league")
        return False, False
    leagues = read_all_table(mycursor, LEAGUE_TABLE)
    all_games = read_all_table(mycursor, PLAYER_IN_GAME_TABLE)
    weights = read_all_table(mycursor, WEIGHTS_TABLE)
    til_table = read_all_table(mycursor, TEAM_IN_LEAGUE_TABLE)
    weights.replace('Ñ\x81hances_created', 'сhances_created', inplace=True)
    if 'Select' in league or 'Select' in position:
        st.error('You didnt select position/league')
        return False, False
    elif position == ALL_POSITIONS_STR:
        st.success('All positions section')
        correction_to_il = []
        for pos in POSITIONS:
            correction_to_il_, l_id = run_regression_correction_to_il(league, pos, leagues, all_games, til_table,
                                                                      plot_to_ui=False)
            if update_db:
                apply_regression_to_db(mydb, mycursor, correction_to_il_, l_id, pos)

            correction_to_il.append(correction_to_il_)
        return correction_to_il, l_id
    else:  # selected specific position
        h1, h2, h3 = st.columns(3)
        with h2:
            st.subheader('Results')
        correction_to_il, l_id = run_regression_correction_to_il(league, position, leagues, all_games, til_table,
                                                                 plot_to_ui=True)
    if correction_to_il is False:
        return correction_to_il, l_id
    st.write()
    if update_db:
        apply_regression_to_db(mydb, mycursor, correction_to_il, l_id, position)
        st.success(f"{league}'s correction was updated to the DB")
    else:
        st.error(f"{league}'s correction wasn't updated to the DB because you didn't click update DB.")
    return correction_to_il, l_id


def find_missing_corrected_positions_in_the_league(mycursor, league):
    leagues = read_all_table(mycursor, LEAGUE_TABLE)
    positions_corrected = leagues[leagues['l_name'] == league]['correction_to_il'].iloc[0]
    if positions_corrected is not None:
        positions = set(json.loads(positions_corrected).keys())
    else:
        positions = set()
    missing_positions = list(set(POSITIONS) - positions)
    return ", ".join(missing_positions)
