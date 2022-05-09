import mysql.connector
from main_functions import *
import streamlit as st

d_std_rate_to_divide_number = {1: 1, 2: 4, 3: 9, 4: 14, 5: 25, 6: 70}


# mydb = mysql.connector.connect(
#     host=HOST, user=USER, password=PASSWORD, database=DB
# )
#
# mycursor = mydb.cursor()

def add_the_new_mean_and_std_to_prior(df_prior, likelihood_df):
    ratings = likelihood_df.sort_values(['mean_l'], ascending=False)['mean_l'].to_list()
    stds_d = likelihood_df[['attribute', 'std_l']].set_index('attribute').to_dict()['std_l']
    if len(ratings) < len(df_prior):
        for i in range(len(df_prior) - len(ratings)):
            ratings.append(0)
    elif len(ratings) > len(df_prior):
        ratings = ratings[:len(df_prior)]

    max_std = likelihood_df['std_l'].max()
    df_prior['mean_p'] = ratings

    df_prior['std_p'] = df_prior.apply(
        lambda x: stds_d.get(x['attribute'], max_std) / d_std_rate_to_divide_number[x['std_p']], axis=1)


def read_dfs(position_to_analyze, mycursor, df_prior, player_in_game_table_name='player_in_game'):
    att_zscores_file = "zscores_" + position_to_analyze
    mycursor.execute(f"SELECT * FROM {att_zscores_file}")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names).set_index('row_names')
    likelihood_df = generate_likelihood_df(df)

    if len(list(df_prior.columns)) != 3 or list(df_prior.columns) != ["attribute", "mean_p", "std_p"]:
        return ['csv prior error']
    add_the_new_mean_and_std_to_prior(df_prior, likelihood_df)
    mycursor.execute(f"SELECT * FROM {player_in_game_table_name}")
    all_players_df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    pos_df = all_players_df[all_players_df['position'] == position_to_analyze].copy()
    mycursor.execute(f"SELECT * FROM att_to_weight")
    df_weights = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    mycursor.execute(f"SELECT * FROM att_to_weight_likelihood")
    df_weights_likelihood = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)

    return df, pos_df, df_prior, likelihood_df, df_weights, df_weights_likelihood


def generate_likelihood_df(df, shadow_atts_list=['shadowMax', 'shadowMean', 'shadowMin','player_id']):
    """ Gets dataframe: zscores for each iteration for all attributes. index: iterations, columns: attributes
    return: likelihhod df- index: counter, columns: attribute,mean_l,	std_l,	n_l.
    The rejected columns are droped"""

    mean_atts = df.T.mean(axis=1).to_frame()
    std_atts = df.T.std(axis=1).to_frame()
    count_atts = df.count().to_frame()
    df_likelihood = pd.concat([mean_atts, std_atts, count_atts], axis=1).reset_index()
    df_likelihood.columns = ['attribute', 'mean_l', 'std_l', 'n_l']
    df_likelihood = df_likelihood[df_likelihood['attribute'].isin(shadow_atts_list) == False]
    rejected_atts = list(df_likelihood[df_likelihood['std_l'].isnull()].index)  # TODO: check if we can clear thi row
    df_likelihood.drop(rejected_atts, inplace=True)
    return df_likelihood


def generate_weights_for_relevant_attributes(df, relevant_atts_for_pos: list):
    """Gets df of posterior: columns- [attribute,post_mean,	post_std]
     return: dictionary : {attribute:weight} """

    df_relevant_atts_to_weight = df[df['attribute'].isin(relevant_atts_for_pos)].copy()
    df_relevant_atts_to_weight['normalized_z'] = df_relevant_atts_to_weight['post_mean'] # this is an old version: / df_relevant_atts_to_weight['post_std']
    sum_z = df_relevant_atts_to_weight['normalized_z'].sum()
    df_relevant_atts_to_weight['weight'] = df_relevant_atts_to_weight['normalized_z'] / sum_z
    d_att_to_weight = df_relevant_atts_to_weight.set_index('attribute')['weight'].to_dict()
    return d_att_to_weight


def calculate_pos_std(row, std_pri_col, std_like_col, count_like_col):
    down = (1 / (row[std_pri_col] ** 2)) + ((row[count_like_col]) / (row[std_like_col] ** 2))
    return 1 / down


def calculate_pos_mean(row, mean_pri_col, std_pri_col, mean_like_col, count_like_col, std_like_col, post_std):
    prior = row[mean_pri_col] * (row[post_std] / row[std_pri_col] ** 2)
    lik = (row[count_like_col] * row[post_std] / row[std_like_col] ** 2) * row[mean_like_col]
    return prior + lik


def generate_prior_likelihood_posterior_df_normal2_model(df_prior, likelihood_df):
    """Args:
    1. df_prior with the columns [attribute, mean_p, std_p]
    2. likelihood_df with the columns :[mean_l	std_l	n_l]
    returns:
    3. df with the corresponding columns for: prior, likelihood, posterior for normal-normal model
        the returned df is without nulls --> the likelihood and prior should be with the same colums
        because we assume that if the BORUTA says that an attribute is not important--> it's the strongest indicator
        :param likelihood_df:
        :param df_prior: """

    if 'attribute' not in list(df_prior.columns) or 'attribute' not in list(
            likelihood_df.columns):  # Check that 'attribute is in the columns
        raise Exception("The column 'attribute' must be in the df_prior and likelihood_df columns")
    df_posterior = pd.concat([df_prior.set_index('attribute'), likelihood_df.set_index('attribute')], axis=1)

    for col in df_posterior.columns:  # Check that all columns are part of the data frame
        if col not in ('mean_p', 'std_p', 'mean_l', 'std_l', 'n_l'):
            raise Exception(
                col + " is wronge column name!! must be one of the following columns to calculate the posterior: ('mean_p','std_p','mean_l','std_l','n_l')")

    df_posterior['post_std'] = df_posterior.apply(lambda x: calculate_pos_std(x, 'std_p', 'std_l', 'n_l'), axis=1)
    df_posterior['post_mean'] = df_posterior.apply(
        lambda x: calculate_pos_mean(x, 'mean_p', 'std_p', 'mean_l', 'n_l', 'std_l', 'post_std'), axis=1)

    like_nulls_list = list(df_posterior[df_posterior['mean_l'].isnull()].index)
    prior_nulls_list = list(df_posterior[df_posterior['mean_p'].isnull()].index)
    like_nulls_list = list(set(like_nulls_list) - {'shadowMax', 'shadowMean', 'shadowMin'})
    if len(like_nulls_list) > 0:
        st.info(f"The following attributes were not rated in the likelihood (boruta) process\n: {like_nulls_list}")
    if len(prior_nulls_list) > 0:
        st.info(f"The following attributes were not rated in the prior (team's) process\n {prior_nulls_list}")
    df_posterior.dropna(inplace=True)
    df_posterior.reset_index(inplace=True)
    df_posterior.rename({'index': 'attribute'}, axis=1, inplace=True)
    return df_posterior


def update_atts_to_weight_file(att_to_weight_dict, df_weights, position_to_analyze):
    df_weights[position_to_analyze] = df_weights['attribute'].apply(
        lambda x: att_to_weight_dict[x] if x in att_to_weight_dict else 0)
    new_columns_to_weight = list(
        set(att_to_weight_dict) - set(df_weights['attribute']))  # check if there are new attributes to add
    if len(new_columns_to_weight) > 0:  # There are new attributes in att_to_weight_dict
        print("new attributes will be added")
        for col in new_columns_to_weight:
            df_weights.at[len(df_weights), 'attribute'] = col
            df_weights.at[len(df_weights) - 1, position_to_analyze] = att_to_weight_dict[col]
            df_weights.fillna(0, inplace=True)
    return df_weights, new_columns_to_weight


# position_selection
# position_to_analyze = 'CD'# position_to_analyze = position_selection.value
def run(mydb, mycursor, position_to_analyze, prior_df, update_db=False):
    if position_to_analyze in POSITIONS:
        dfs = read_dfs(position_to_analyze, mycursor, prior_df)
        if len(dfs) == 1:  # There is problem with the prior df
            raise Exception(f"'{dfs[0]}'")
        df, pos_df, df_prior, likelihood_df, df_weights, df_likelihood_weights = dfs
        df_posterior = generate_prior_likelihood_posterior_df_normal2_model(df_prior, likelihood_df)
        att_to_weight_dict = generate_weights_for_relevant_attributes(df_posterior, df_posterior['attribute'].to_list())
        likelihood_att_to_weight_dict = generate_weights_for_relevant_attributes(
            likelihood_df.rename({'mean_l': 'post_mean', 'std_l': 'post_std'}, axis=1),
            likelihood_df['attribute'].to_list())
        new_df_weights, new_atts = update_atts_to_weight_file(att_to_weight_dict, df_weights, position_to_analyze)
        new_df_likelihood_weights, new_atts_like = update_atts_to_weight_file(likelihood_att_to_weight_dict,
                                                                              df_likelihood_weights,
                                                                              position_to_analyze)

        print("Algorithm is done")
        if update_db:
            mycursor.execute("drop table att_to_weight")
            mydb.commit()
            print('dropped att_to_weight')
            print('to_sql att_to_weight:')
            new_df_weights = new_df_weights.reset_index().set_index('index')
            new_df_weights_cols = new_df_weights.columns[1:]
            new_df_weights = new_df_weights[new_df_weights_cols]
            engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}")
            new_df_weights.to_sql(name='att_to_weight', con=engine)
            print('updated att_to_weight')

            mycursor.execute("drop table att_to_weight_likelihood")
            mydb.commit()
            print('dropped att_to_weight_likelihood')
            print('to_sql att_to_weight_likelihood:')
            new_df_likelihood_weights = new_df_likelihood_weights.reset_index().set_index('index')
            new_df_likelihood_weights_cols = new_df_likelihood_weights.columns[1:]
            new_df_likelihood_weights = new_df_likelihood_weights[new_df_likelihood_weights_cols]

            engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}")
            new_df_likelihood_weights.to_sql(name='att_to_weight_likelihood', con=engine)
            print('updated att_to_weight_likelihood')

        return new_df_weights, new_df_likelihood_weights
    else:
        raise Exception(f"'{position_to_analyze}' is not a position")

# run(mycursor, 'LD', update_db=True)
