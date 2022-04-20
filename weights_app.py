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
import plotly.figure_factory as ff
import plotly.graph_objects as go


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
    weights_df = read_all_table(mycursor, WEIGHTS_TABLE)
    mycursor.close()
    return weights_df[['attribute',position]].sort_values(position,ascending=False)

def app():
    mydb = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DB
    )
    mycursor = mydb.cursor()
    st.markdown(HEADER, unsafe_allow_html=True)
    st.markdown(CONTENT, unsafe_allow_html=True)
    cola, colb, colc = st.columns(page_structure)

    with colb:
        update_weights = st.container()
        with update_weights:
            # show_st_image(img_file_name='weights.jpeg')
            st.header('Change weights')
            st.write("Please upload your weights' csv file to here")
            st.write("Your file should look like the following example:")
            show_st_image(img_file_name='prior_example.png', caption='Example of the csv file')
            position = st.selectbox("Select position", POSITIONS)

            uploaded_file = st.file_uploader(label="Upload your weights to the position's attributes")
            if uploaded_file:
                prior_df = pd.read_csv(uploaded_file)
            chk_update_weights_db = st.checkbox('Would you like to updates weights db?')
            btn_update_weights = st.button('Update weights')
            if btn_update_weights:
                if uploaded_file is not None:
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
                    st.error("You didn't upload weights file")

    with colc:
        if position in POSITIONS:
            with st.sidebar:
                #todo:
                df_pos = show_position_df(mycursor, position)
                download_weights(df_pos,position)
                show_pos_df(df_pos)


    mydb.close()

def show_pos_df(df_pos):
    df_pos.columns = ['Attribute', 'Weight']
    df_pos['Weight'] = df_pos['Weight'].apply(lambda x: round(float(x),3))
    fig_table = ff.create_table(df_pos)
    fig_table.layout.width = 450
    st.plotly_chart(fig_table)

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
