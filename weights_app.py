import numpy as np
import mysql.connector
import update_weights as uw
from streamlit_gui import *

from nav_bar import HEADER, CONTENT
from main_functions import *
import plotly.figure_factory as ff
import plotly.graph_objects as go
from streamlit_lottie import st_lottie
import requests
from update_weights import generate_likelihood_df

def load_lottieurl(url: str):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()

def comparison_bar_plots(x1, x2, y1, y2, name1, name2):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=x1,
        y=y1,
        name=name1,
        marker_color='indianred'
    ))
    fig.add_trace(go.Bar(
        x=x2,
        y=y2,
        name=name2,
        marker_color='lightsalmon'
    ))

    fig.update_layout(barmode='group', xaxis_tickangle=-45, title_text="Weights comparison")
    return fig


def show_position_df(mycursor,position):
    weights_df = read_all_table(mycursor, LIKELIHOOD_WEIGHTS_TABLE)
    mycursor.close()
    return weights_df[['attribute',position]].sort_values(position,ascending=False)

def app():

    st.markdown(HEADER, unsafe_allow_html=True)
    st.markdown(CONTENT, unsafe_allow_html=True)
    cola, colb, colc = st.columns([3,1,2])

    with cola:
        update_weights = st.container()
        with update_weights:
            st.header('Change weights')
            position = st.selectbox("Select position", POSITIONS)
    with colc:
        if position in POSITIONS:
            st.subheader("Current weights")
            tables = get_tables_name()
            if LIKELIHOOD_WEIGHTS_TABLE not in tables:
                st.write(f"no {LIKELIHOOD_WEIGHTS_TABLE}")
            else:
                mydb,mycursor = connect_to_the_DB()
                df_pos = show_position_df(mycursor, position)
                download_weights(df_pos, position)
                show_pos_df(df_pos)
                disconnect_from_the_db(mycursor,mydb)
    with cola:
        with update_weights:
            # show_st_image(img_file_name='weights.jpeg')
            downloaded_url = load_lottieurl("https://assets4.lottiefiles.com/packages/lf20_oi0plzot.json")
            st_lottie(downloaded_url, key="success",width=300)
            num_atts_to_set = st.number_input("Select the number of attributes to set up ",min_value=1,max_value=10,value=5,step=1)
            form = st.form("atts_rank")
            d_rank_to_att = dict()
            atts = list(df_pos['Attribute'].to_list())
            with form:
                for i in range(1,int(num_atts_to_set+1)):
                    att = form.selectbox(f"Attrubute #{i}",atts)
                    std = form.number_input(f"Attrubute #{i} - Select the confidence of your prior weights [1-6 scale]",min_value=1,max_value=6,value=3,step=1)
                    if i < num_atts_to_set:
                        st.write("_"*20)
                    rank = i - 1
                    d_rank_to_att[rank] = {'attribute':att, 'std' : std}

                chk_update_weights_db = st.checkbox('Would you like to updates weights table?')

            submitted = form.form_submit_button("Submit")
            if submitted:
                unique_counts = len(set([data_dict['attribute'] for data_dict in d_rank_to_att.values()]))
                if unique_counts == num_atts_to_set:
                    prior_df = get_prior_df(d_rank_to_att,df_pos)
                    mydb, mycursor = connect_to_the_DB()
                    new_weights_df, new_likelihood_df = uw.run(mydb, mycursor, position, prior_df,
                                                                       update_db=chk_update_weights_db)
                    st.success('Done')
                    st.subheader('New weights')
                    st.write(new_weights_df[['attribute', position]])
                    new_weights_df.sort_values(position, ascending=False, inplace=True)
                    fig = comparison_bar_plots(x1=new_weights_df['attribute'].to_list(),
                                               x2=new_likelihood_df['attribute'].to_list(),
                                               y1=new_weights_df[position].to_list(),
                                               y2=new_likelihood_df[position].to_list(),
                                               name1='Your New Weights',
                                               name2='Old weights (from the data)')
                    st.plotly_chart(fig)

                else:
                    st.error("You have selected 2 attributes of the same kind")


    mydb.close()

def get_prior_df(d_rank_to_att,df_pos):
    df_weights = df_pos.copy()
    df_weights.index = np.arange(len(d_rank_to_att), len(d_rank_to_att) + len(df_weights)) # generate new index starting from the number of rated attribute until the number of attributes
    weights_list = df_weights['Weight'].to_list()
    df_weights['std_p'] = 3
    # add the top attributes to the head of the df
    for rank, data_dict in d_rank_to_att.items():
        df_weights.loc[int(rank), ['Attribute', 'std_p']] = [data_dict['attribute'], data_dict['std']]
    df_weights.sort_index(inplace=True)
    df_weights.drop_duplicates(keep='first', subset=['Attribute'], inplace=True)
    df_weights.columns = ['attribute','mean_p','std_p']
    return df_weights
def show_pos_df(df_pos):
    df_pos.columns = ['Attribute', 'Weight']
    df_pos['Weight'] = df_pos['Weight'].apply(lambda x: round(float(x),3))
    fig_table = ff.create_table(df_pos)
    fig_table.layout.width = 450
    st.plotly_chart(fig_table,use_container_width=True)

def download_weights(df_pos,position):
    df_download = df_pos.copy()
    df_download.columns = ['attribute', 'mean_p']
    df_download['std_p'] = None
    df_download.set_index('attribute',inplace=True)
    @st.cache
    def convert_df(df):
        return df.to_csv().encode('utf-8')

    csv = convert_df(df_download)
    st.download_button(
        "Click to Download Table",
        csv,
        f"{position}_weights.csv",
        "text/csv",
        key='download-csv'
    )
