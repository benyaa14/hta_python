import mysql.connector
from main_functions import *
import streamlit as st
from nav_bar import *
from main import page_structure
import plotly.figure_factory as ff
import gurobipy as gp
from gurobipy import GRB
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from mplsoccer.pitch import Pitch
import matplotlib.pyplot as plt

MAX_BUDGET = 200.0 # M$
MAX_BUDGET_SALARIES = 1000.0 # Th. $
MAX_PLAYER_IN_POS = 10
MIN_PLAYER_IN_POS = 1
MIN_PLAYERS_IN_TEAM = 9
MAX_PLAYERS_IN_TEAM = 40


def input_budget(form):
    budget = form.slider('What is the transfers budget (M$)?', 0.0, MAX_BUDGET, 1.0, step=0.1)
    # form.write("The transfers budget is", budget, 'M$')
    budget *= 1000000
    return budget

def salary_budget(form):
    sal_budget = form.slider('What is the salary budget (Th$)?', 0.0, MAX_BUDGET_SALARIES,50.0, step=50.0)
    # form.write("The budget for salaries is", sal_budget, 'Th$')
    sal_budget *= 1000
    return sal_budget


def input_num(form,min_max: str, position: str, max_value, min_value, default_val):
    val = form.number_input(f"{position}: {min_max} players", min_value, max_value, default_val)
    return val

def store_optimization_solutions(m1,combinations,x,PTF,NOT_IN_TEAM):
    try:
        m1.optimize()
        m1.setParam(GRB.Param.OutputFlag, 1)

        status = m1.Status
        if status in (GRB.INF_OR_UNBD, GRB.INFEASIBLE, GRB.UNBOUNDED):
            st.error("The model cannot be solved because it is infeasible or unbounded")
            return None
        if status != GRB.OPTIMAL:
            st.info("Optimization was stopped with status" + str(status))
            return None
        solutions_players_dict = dict()
        solutions_scores_dict = dict()
        expenses_dict = dict()
        solutions_players_dict[0] = []
        for v in m1.getVars():
            if v.x > .9:
                start_name_index = v.varName.find('[') + 1
                solutions_players_dict[0].append(v.varName[start_name_index:-1])
        nSolutions = m1.SolCount
        for e in range(nSolutions):
            m1.setParam(GRB.Param.SolutionNumber, e)
            solutions_scores_dict[e] = m1.PoolObjVal

        min_score = min(list(solutions_scores_dict.values()))
        if min_score < 0:
            solutions_scores_dict = {k: v + abs(min_score) for k, v in solutions_scores_dict.items()}
        if nSolutions >= 2:
            for i in range(nSolutions):
                expences = 0
                solutions_players_dict[i] = []
                m1.setParam(GRB.Param.SolutionNumber, i)
                # print(f"Selected elements in {i+1}-th best solution:")
                for player in combinations:
                    if x[(player)].Xn > .9:
                        solutions_players_dict[i].append(player)
                        if player in NOT_IN_TEAM:
                            expences += PTF[(player)]
                expenses_dict[i] = expences

            df_optimal_teams = pd.DataFrame()
            for n_opt_team, players in solutions_players_dict.items():
                df_optimal_teams.loc[n_opt_team, players] = 1 * solutions_scores_dict[n_opt_team]
                df_optimal_teams.loc[n_opt_team, 'Score'] = solutions_scores_dict[n_opt_team]
                df_optimal_teams.loc[n_opt_team, 'Expenses'] = expenses_dict[n_opt_team]
            df_optimal_teams.fillna(0, inplace=True)
            return df_optimal_teams
    except gp.GurobiError as e:
        print('Gurobi error ' + str(e.errno) + ": " + str(e.message))
        return None
    except AttributeError as e:
        print("AttributeError: " + str(e))
        return None
def run_optimization(candidates_df,salary,VETO_PLAYERS,BUDGET,lambda_,
                     teta,MAX_PLAYERS_IN_TEAM,MIN_PLAYERS_IN_TEAM,SALARY_BUDGET, IS_IN_TEAM,NOT_IN_TEAM,ALL_POSITIONS,position_to_apply_height,H,extra_payments=1):
    candidates_df['is_foreign'] = candidates_df['is_foreign'].apply(lambda x: 1 if x == True else 0)
    to_multidict = dict()
    for i, row in candidates_df.iterrows():
        to_insert = [row['p_rank'], row['market_value'], row['is_foreign'], row['position'], salary, extra_payments,
                     row['height']]
        to_multidict[(row['p_name'])] = to_insert
    combinations, fi, PTF, is_foreign, positions, salary, extra_payments, height = gp.multidict(to_multidict)

    # -----------
    # Variables
    # -----------
    m1 = gp.Model('opteamize')
    x = m1.addVars(combinations,vtype = GRB.BINARY, name="player")
    #-----------
    #constrains
    #-----------


    # 1) Budget
    budget = m1.addConstr(
        gp.quicksum((PTF[(player)] * x[(player)] for player in NOT_IN_TEAM)) - gp.quicksum(
            (PTF[(player)] * (1 - x[(player)]) for player in IS_IN_TEAM)) <=
        BUDGET, 'Budget')

    # 2) Num of players in positions
    for pos in ALL_POSITIONS:
        players_in_constraint = []
        for player, value in positions.items():
            if pos == value:
                players_in_constraint.append(x[player])
        m1.addConstr(gp.quicksum(players_in_constraint) >= lambda_[pos], name=f"num of players [{pos}]")

    # 3) Num of players in team
    m1.addConstr(x.sum('*') <= MAX_PLAYERS_IN_TEAM, name=f"max players in team")
    m1.addConstr(x.sum('*') >= MIN_PLAYERS_IN_TEAM, name=f"min players in team")

    # 4) Num of foreign_players
    foreign_players = m1.addConstr((x.prod(is_foreign) <= teta), name='Foreign players')

    # 5) Salary budget
    salary_budget = m1.addConstr(
        gp.quicksum((salary[(player)] * x[(player)] for player in NOT_IN_TEAM)) <=
        SALARY_BUDGET - gp.quicksum((salary[(player)] * (x[(player)]) for player in IS_IN_TEAM)), 'SalaryBudget')



    # 6) Players who must be on the team
    veto = m1.addConstr(gp.quicksum((x[(player)] for player in VETO_PLAYERS)) == len(VETO_PLAYERS), 'vetoPlayers')

    # # 7) Height players
    if H is not None:
        y = m1.addVars([combination for combination in combinations if positions[combination] == position_to_apply_height]
                       , vtype=GRB.BINARY, name="height_var_" + position_to_apply_height)


        for player, h_i in height.items():
            if positions[(player)] == position_to_apply_height:
                m1.addConstr(x[(player)] * h_i >= H * y[(player)], 'Height_' + player)
        sum_y = m1.addConstr(gp.quicksum(y) == 1, 'sum_y')

    #-----------
    # Objective function
    #-----------

    objective_func = m1.setObjective(x.prod(fi), GRB.MAXIMIZE)

    # -----------
    # collect solutions
    # -----------

    m1.setParam(GRB.Param.PoolSolutions, 100)
    m1.setParam(GRB.Param.PoolGap, 20)
    m1.setParam(GRB.Param.PoolSearchMode, 2)


    # m1.write('opteamize.lp')

    # -----------
    # Results
    # -----------
    df_optimal_teams = store_optimization_solutions(m1,combinations,x,PTF,NOT_IN_TEAM)
    if df_optimal_teams is not None:
        players_to_check = (df_optimal_teams.sum() / df_optimal_teams['Score'].sum()).sort_values(ascending=False)
        players_to_check.drop(['Score', 'Expenses'], inplace=True)
    else:
        return None,None,None,None
    # m1.optimize()

    opt_players = []
    cnt_solutions = m1.getAttr("solCount")
    if cnt_solutions > 0:
        # Display optimal values of decision variables
        for v in m1.getVars():
            if v.x > 1e-6:
                # st.write(v.varName, v.x)
                start_name_index = v.varName.find('[') + 1
                opt_players.append(v.varName[start_name_index:-1])

                # Display optimal total matching score
        # st.info(f"Total matching score: {m1.objVal}")
    else:
        st.error("**************NO OPTIMAL SOLUTION***********")
    return m1,opt_players,df_optimal_teams,players_to_check

def visualize_top_teams_and_expenses(df_optimal_teams,height_plot):
    y_expences, y_scores = df_optimal_teams['Expenses'].head(10).to_list(), df_optimal_teams['Score'].head(10).to_list()
    x = list(df_optimal_teams.index * -1)
    fig = make_subplots(rows=1, cols=2, specs=[[{}, {}]], shared_xaxes=True,
                        shared_yaxes=False, vertical_spacing=0.001)

    fig.append_trace(go.Bar(
        x=y_scores,
        y=x,
        marker=dict(
            color='rgba(50, 171, 96, 0.6)',
            line=dict(
                color='rgba(50, 171, 96, 1.0)',
                width=1),
        ),
        name='Team total scores',
        orientation='h',
    ), 1, 1)

    fig.append_trace(go.Scatter(
        x=y_expences, y=x,
        mode='lines+markers',
        line_color='rgb(128, 0, 128)',
        name='Expenses, Million USD',
    ), 1, 2)

    fig.update_layout(height=height_plot,
        title='Top Optimal Teams',
        yaxis=dict(
            showgrid=False,
            showline=False,
            showticklabels=False,
            domain=[0, 0.85],
        ),
        yaxis2=dict(
            showgrid=False,
            showline=True,
            showticklabels=False,
            linecolor='rgba(102, 102, 102, 0.8)',
            linewidth=2,
            domain=[0, 0.85],
        ),
        xaxis=dict(
            zeroline=False,
            showline=False,
            showticklabels=True,
            showgrid=True,
            domain=[0, 0.42], range=[min(y_scores) - np.array(y_scores).std() / 2, max(y_scores)]
        ),
        xaxis2=dict(
            zeroline=False,
            showline=False,
            showticklabels=True,
            showgrid=True,
            domain=[0.47, 1],
            side='top',
            # dtick=25000,
        ),
        legend=dict(x=0.029, y=1.038, font_size=10),
        margin=dict(l=100, r=20, t=70, b=70),
        paper_bgcolor='rgb(248, 248, 255)',
        plot_bgcolor='rgb(248, 248, 255)',
    )

    annotations = []

    y_s = np.round(y_scores, decimals=2)
    y_nw = np.rint(y_expences)

    # Adding labels
    for ydn, yd, xd in zip(y_nw, y_s, x):
        # labeling the scatter savings
        annotations.append(dict(xref='x2', yref='y2',
                                y=xd, x=ydn + 20000,  # ,x= ydn
                                text='{:,}'.format(ydn / 1000000) + 'M [USD]',
                                font=dict(family='Arial', size=12,
                                          color='rgb(128, 0, 128)'),
                                showarrow=False))
        # labeling the bar net worth
        annotations.append(dict(xref='x1', yref='y1',
                                y=xd, x=yd + 3,
                                text=str(yd) + '%',
                                font=dict(family='Arial', size=12,
                                          color='rgb(50, 171, 96)'),
                                showarrow=False))

    fig.update_layout(annotations=annotations)
    return fig

def visualize_top_players_to_check(players_to_check):
    players_to_check_df = players_to_check.to_frame().reset_index()
    height_plot =  len(players_to_check_df) * 30
    players_to_check_df.columns = ['Name', 'Percent Matching']
    fig = px.bar(players_to_check_df, x="Percent Matching", y="Name", orientation='h',
                 height=height_plot,
                 title='Top Players: for video analysis')
    fig.update_layout(yaxis=dict(autorange="reversed"),
      legend=dict(x=0.029, y=1.038, font_size=10),
        margin=dict(l=100, r=20, t=70, b=70),
        paper_bgcolor='rgb(248, 248, 255)',
        plot_bgcolor='rgb(248, 248, 255)',)
    return fig,height_plot

def visualize_results(candidates_df,opt_players,budget,m1,df_optimal_teams,players_to_check,your_team_id):
    st.info(f"Total matching score: {m1.objVal}")
        # selected_team = st.selectbox("Select ranked team", list(df_optimal_teams.head(10).index + 1), 0) - 1
        # selected_team_ser = df_optimal_teams.iloc[selected_team]
        # opt_players = list(selected_team_ser[selected_team_ser != 0].index)
    col1,col2 = st.columns(2)
    candidates_df.sort_values('position',inplace=True)
    candidates_df = candidates_df.round(3)
    candidates_df['in_optimal_solution'] = candidates_df['p_name'].apply(lambda x: x in opt_players)
    players_to_buy_df = candidates_df[
        (candidates_df['in_optimal_solution'] == 1) & (candidates_df['t_id'] != 12)].copy()
    players_to_sell_df = candidates_df[
        (candidates_df['in_optimal_solution'] == 0) & (candidates_df['t_id'] == 12)].copy()
    cols_to_vis = ['p_name','position','market_value','p_rank','age','is_foreign']

    expenses = players_to_buy_df['market_value'].sum()
    revenues = players_to_sell_df['market_value'].sum()
    model_overview_container, selected_team_container = st.container(),st.container()
    with model_overview_container:

        with col1:

            fig_top_players, height_plot = visualize_top_players_to_check(players_to_check)
            st.plotly_chart(fig_top_players,use_container_width=True)


        with col2:
            fig_top_teams = visualize_top_teams_and_expenses(df_optimal_teams,height_plot)
            st.plotly_chart(fig_top_teams,use_container_width=True)


    with selected_team_container:
        with col1:

            st.subheader("Optimal team")
            st.plotly_chart(
                ff.create_table(
                    candidates_df[candidates_df['in_optimal_solution']][cols_to_vis].sort_values('position')),use_container_width=True)
        with col2:

            st.subheader("Players to buy")
            st.plotly_chart(ff.create_table(players_to_buy_df[cols_to_vis]))
            st.warning(f"Expenses:  {round(expenses /1000000,3) } M$ ")

            st.subheader("Players to sell")
            st.plotly_chart(ff.create_table(players_to_sell_df[cols_to_vis]))
            st.warning(f"Revenues:  {round(revenues /1000000,3) } M$ ")

            st.subheader("Budget summary")
            buffer = budget + revenues - expenses
            st.success(f"Budget + Revenues - Expenses = Buffer = {buffer}$")
    with col1:
        # todo:just for 4-4-2
        IS_IN_TEAM = candidates_df[candidates_df['t_id'] == your_team_id]['p_name'].to_list()
        NOT_IN_TEAM = candidates_df[candidates_df['p_name'].isin(IS_IN_TEAM) == False]['p_name'].to_list()
        candidates_df['new_player'] = candidates_df['p_name'].apply(lambda x: x in NOT_IN_TEAM)
        plot_pitch(candidates_df[candidates_df['in_optimal_solution']].copy())



        @st.cache
        def convert_df(df):
            return df.to_csv().encode('utf-8')

        csv = convert_df(df_optimal_teams)
        st.download_button(
            "Click to Download The Optimal Teams Results",
            csv,
            f"Optimal_Teams.csv",
            "text/csv",
            key='download-csv'
        )

def add_x_y_to_df(df,dict_line_up):
    pos_x=[]
    pos_y=[]
    for index, row in df.iterrows():
      pos_x.append(int(dict_line_up[row['our_position']][0]))
      pos_y.append(int(dict_line_up[row['our_position']][1]))
    df['x_lineup']=pos_x
    df['y_lineup']=pos_y
    return df


def plot_pitch(df):
    value_counts_pos = df['position'].value_counts()
    if value_counts_pos.loc['F'] != 2 or value_counts_pos.loc['CD']!=2 or value_counts_pos.sum()!=10: # this is *not* 4-4-2
        st.info("The field plot is just for 4-4-2 in the meantime")
        return 
    df.sort_values('position', inplace=True)
    df['s_position'] = df.sort_values('position')['position'].shift()
    df['our_position'] = df.apply(
        lambda x: str(x['position']) + '1' if x['position'] != x['s_position'] else str(x['position']) + '2', axis=1)
    dict_f_f_2 = {'RD1': [40, 7], 'LD1': [40, 73], 'CD1': [25, 30], 'CD2': [25, 50], 'DM1': [54, 40],
                  'CM1': [80, 40], 'RM1': [67, 15], 'LM1': [67, 65], 'F1': [100, 28], 'F2': [100, 52]}
    df = add_x_y_to_df(df, dict_f_f_2)
    pitch = Pitch(pitch_color='#aabb97', line_color='white',
                  stripe_color='#c2d59d', stripe=True)  # optional stripes
    fig, ax = pitch.draw(figsize=(9, 5), constrained_layout=True, tight_layout=False)
    fig.gca().invert_yaxis()
    Candidate = plt.scatter(25, 30, color='lightsteelblue')
    Existing = plt.scatter(25, 30, color='firebrick')
    for index, row in df.iterrows():
        if row['new_player'] == 1:
            lineup_color = 'lightsteelblue'
        else:
            lineup_color = 'firebrick'
        pitch.scatter(row['x_lineup'], row['y_lineup'], s=600, color=lineup_color, edgecolors='black', ax=ax)
        pitch.annotate(text=row['p_name'], xytext=(row['x_lineup'] - 2, row['y_lineup'] + 6), xy=(80, 80), ha='center',
                       fontweight='bold', style='italic', va='top', ax=ax)
    plt.legend((Candidate, Existing),
               ('Candidate player', 'Existing player'),
               scatterpoints=1,
               loc='lower left',
               ncol=3,
               fontsize=8)
    # title = ax.set_title('The Optimal Team', fontsize = 20)
    st.pyplot(fig)


# def app(your_team_id = 12):
#     cola, colb, colc = st.columns(page_structure)
#     # ----------NAVBAR---------
#     st.markdown(HEADER, unsafe_allow_html=True)
#
#     st.markdown(CONTENT, unsafe_allow_html=True)
#     # -------/NAVBAR/--------------
#     with colb:
#         header = st.container()
#         with header:
#             st.header('Input your data')
#             st.subheader('You must enter the following data')
#             uploaded_file = st.file_uploader(label="Upload your candidates players file")
#             VETO_PLAYERS = []
#             if uploaded_file:
#                 candidates_df = pd.read_csv(uploaded_file)
#                 #todo: add logic to find if the file is candidates file
#                 IS_IN_TEAM = candidates_df[candidates_df['t_id']==your_team_id]['p_name'].to_list()
#                 NOT_IN_TEAM = candidates_df[candidates_df['p_name'].isin(IS_IN_TEAM) == False]['p_name'].to_list()
#                 VETO_PLAYERS = st.multiselect('Pick up your "veto" players',IS_IN_TEAM)
#             budget = input_budget()
#             sal_budget = salary_budget()
#             teta = st.number_input("Maximum number of foreign players in the team",0,11,2)
#
#             d_min_max_pos = dict()
#             st.subheader('Number of players in team')
#             for type_ in ['Max', 'Min']:
#                 val = input_num(min_max=type_, position="All players in the team", max_value=MAX_PLAYERS_IN_TEAM,
#                                 min_value=MIN_PLAYERS_IN_TEAM,
#                                 default_val=10)
#                 d_min_max_pos[("team", type_)] = val
#             st.subheader('Minimum height for at least one player in a position')
#             HEIGHT = None
#             height_pos = st.selectbox('Select position',['Select position'] + POSITIONS)
#             if height_pos != 'Select position':
#                 HEIGHT = st.number_input('Select minimum height in CM',0,300,0,1)
#
#
#
#
#
#     cols = st.columns(len(POSITIONS))
#     lambda_ = dict()
#     # for pos in POSITIONS:
#     for cl,pos in zip(cols,POSITIONS):
#         for type_ in ['Min']:#['Max', 'Min']:
#             with cl:
#                 val = input_num(min_max=type_, position=pos, max_value=MAX_PLAYER_IN_POS,
#                                 min_value=MIN_PLAYER_IN_POS, default_val=1)
#                 d_min_max_pos[(pos, type_)] = val
#                 lambda_[pos] = val
#     cont_button = st.container()
#     model =  opt_players = None
#     a,b,c = st.columns(3)
#     with cont_button:
#         with b:
#             st.write('And now....')
#             if st.button('opTEAMize'):
#                 if uploaded_file is None :
#                     st.error('First upload the candidates file')
#                 else:
#                     model,opt_players,df_optimal_teams,players_to_check = run_optimization(candidates_df,1,VETO_PLAYERS,budget,lambda_,teta,
#                                      d_min_max_pos[('team','Max')],d_min_max_pos[('team','Min')],sal_budget,IS_IN_TEAM,NOT_IN_TEAM,
#                                      POSITIONS,height_pos,HEIGHT)
#
#     if model is not None:
#         with cont_button:
#             visualize_results(candidates_df,opt_players,budget,model,df_optimal_teams,players_to_check,your_team_id)

def app(your_team_id = 12):
    cola, colb, colc = st.columns(page_structure)
    # ----------NAVBAR---------
    st.markdown(HEADER, unsafe_allow_html=True)

    st.markdown(CONTENT, unsafe_allow_html=True)
    # -------/NAVBAR/--------------
    st.header('Input your data')
    st.subheader('You must enter the following data')
    uploaded_file = st.file_uploader(label="Upload your candidates players file")
    VETO_PLAYERS = []
    if uploaded_file:
        candidates_df = pd.read_csv(uploaded_file)
        # todo: add logic to find if the file is candidates file
        IS_IN_TEAM = candidates_df[candidates_df['t_id'] == your_team_id]['p_name'].to_list()
        NOT_IN_TEAM = candidates_df[candidates_df['p_name'].isin(IS_IN_TEAM) == False]['p_name'].to_list()
        VETO_PLAYERS = st.multiselect('Pick up your "veto" players', IS_IN_TEAM)
    form = st.form("optimize_form_inputs")
    with colb:
        header = form.container()
        with header:




            budget = input_budget(form)
            sal_budget = salary_budget(form)
            teta = form.number_input("Maximum number of foreign players in the team",0,11,2)


            form.subheader('Minimum height for at least one player in a position')
            HEIGHT = None
            height_pos = form.selectbox('Select position',['Select position'] + POSITIONS)
            if height_pos != 'Select position':
                HEIGHT = form.number_input('Select minimum height in CM',0,300,0,1)

            d_min_max_pos = dict()
            form.subheader('Number of players in team')
            for type_ in ['Max', 'Min']:
                val = input_num(form,min_max=type_, position="All players in the team", max_value=MAX_PLAYERS_IN_TEAM,
                                min_value=MIN_PLAYERS_IN_TEAM,
                                default_val=10)
                d_min_max_pos[("team", type_)] = val



    cols = st.columns(len(POSITIONS))
    lambda_ = dict()
    # for pos in POSITIONS:
    for cl,pos in zip(cols,POSITIONS):
        for type_ in ['Min']:#['Max', 'Min']:
            with cl:
                val = input_num(form, min_max=type_, position=pos, max_value=MAX_PLAYER_IN_POS,
                                min_value=MIN_PLAYER_IN_POS, default_val=1)
                d_min_max_pos[(pos, type_)] = val
                lambda_[pos] = val
    cont_button = st.container()
    model =  opt_players = None
    a,b,c = st.columns(3)
    submitted = form.form_submit_button("opTEAMize")
    with cont_button:
        with b:
            # st.write('And now....')
            if submitted:
                if uploaded_file is None :
                    st.error('First upload the candidates file')
                else:
                    model,opt_players,df_optimal_teams,players_to_check = run_optimization(candidates_df,1,VETO_PLAYERS,budget,lambda_,teta,
                                     d_min_max_pos[('team','Max')],d_min_max_pos[('team','Min')],sal_budget,IS_IN_TEAM,NOT_IN_TEAM,
                                     POSITIONS,height_pos,HEIGHT)

    if model is not None:
        with cont_button:
            visualize_results(candidates_df,opt_players,budget,model,df_optimal_teams,players_to_check,your_team_id)


# app()
