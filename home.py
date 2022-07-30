import streamlit as st
from main import show_st_image
from main_functions import *
page_structure = [1, 3, 1]

def generate_hi_header_aligned(i,string_to_print):
     st.markdown(f"""<h{i} style="text-align:center"> {string_to_print} </h{i}>""",unsafe_allow_html=True)


def generate_hi_header(i, string_to_print):
    st.markdown(f"""<h{i}> {string_to_print} </h{i}>""", unsafe_allow_html=True)

def generate_ordered_list(list_):
    html = """<ul>"""
    for row in list_:
        html += f"<li>{row}</li>"
    html += '</ul>'
    st.markdown(html,unsafe_allow_html=True)

def first_stage_html():
    generate_ordered_list(list_=[
        'Download from Instat all of the performances of the player of interest during the last two years per game as Excel files'])
    show_st_image(img_file_name='instat_player_page.png', caption='Player matches')
    generate_ordered_list(["Make sure you download only league games"])
    show_st_image(img_file_name='selection_matches.png', caption='Matches selection')
    generate_ordered_list([
                              "Click 'Upload player in game files' and upload the Excel you just downloaded. The system will push the new data to the DB",
                              "Don't worry, once you upload the new data, it will be saved and you won't have to download it again"])

def sec_stage_html():
    generate_ordered_list(list_=[
        'Select the position you would like to adjust',
        'You will see in the left sidebar the weights for each attribute according to our analytics',
        'Download the file and rank the attributes by the position you selected'])
    show_st_image(img_file_name='weights.png')

    generate_ordered_list(['Upload the file', 'Run the algorithm'])
def third_stage_html():
    generate_ordered_list(['As simple as it sounds', 'Click <code> Run ranking algorithm</code>',
                           'Each player will get a new rank for each '
                           'game according to your adjusted weights'])

def fourth_stage_html():
    generate_ordered_list([
                              "Probably you're already aware that there are differences between leagues, one can excel at defence play while the other can excel at offence play"
                              , "Furthermore, there are leagues that are superior to the others in every aspect",
                              'opTEAMize developed a tool that will find the level difference '
                              'for each league in each position and give you the opportunity to analyze it too',
                              'You can decide based on the output of the regression if you would like to give the other league extra/minus points for their ratings'])
    show_st_image(img_file_name='regression_al_england.png', caption='First step')

def app():

    header,steps = st.container(),st.container()

    with header:
        col1, col2, col3 = st.columns(page_structure)
        with col2:

            generate_hi_header_aligned("2","Hi! Welcome to opTEAMize!")
            generate_hi_header_aligned("3","opTEAMize is an end-to-end solution for football analysts, to make their scouting process optimal for the next season")
            show_st_image(img_file_name='opteamize.png', caption='Connecting the dots')
            generate_hi_header("8", "We believe that the coaching staff knows which characteristics of a player they seek for each position. "
                                            "Therefore, we developed a flexible system that allows the analyst to adjust the importance of each event of the game to each position,"
                                            " and by doing so, our algorithm will rank the players based on your particular requirements.")
            generate_hi_header("8", "In the second stage, you select your candidates for the next season, as well as your current players. Our optimization model will take into account:")
            generate_ordered_list(list_ = ["Players' customized rank" , "Wage", "Additional payments","Team's budget", "Many other constraints and needs"])
            generate_hi_header("8", "And it will help the coaching staff make one of the most complex decisions a team can make:<code>  Which players to acquire? Which players to sell? </code> ")

            generate_hi_header_aligned("1" ,"In one word: opTEAMize")


            generate_hi_header_aligned("4","How does it works ?")
    with steps:

        col1, col2, col3 = st.columns(page_structure)

        with col2:
            size = '5'
            generate_hi_header(size,"1. Upload the data from your Instat account")
            info_stage_1 = st.button('For more information',key='1')
            if info_stage_1:
                first_stage_html()
            generate_hi_header(size, "2. Adjust your weights")
            info_stage_2 = st.button('For more information',key='2')
            if info_stage_2:
                sec_stage_html()
            generate_hi_header(size,"3. Rank all players")
            info_stage_3 = st.button('For more information', key='3')
            if info_stage_3:
                third_stage_html()
            generate_hi_header(size, "4. League regression")
            info_stage_4 = st.button('For more information', key='4')
            if info_stage_4:
                fourth_stage_html()
            generate_hi_header(size, "5. Visualize player ranking")
            info_stage_5 = st.button('For more information', key='5')
            if info_stage_5:
                show_st_image(img_file_name='time_series_davida_firmino.png')

                generate_ordered_list(['Before running the optimization model you can compare players ratings and events in the game',
                                       'Select the position you would like to visualize',
                                       'Select multiple players to compare',
                                       'Test the results'])

                show_st_image(img_file_name='dist_davida_firmino.png')

        with col3:
            if info_stage_1:
                show_st_image(img_file_name='upload_data.png', caption='First step')
            if info_stage_2:
                show_st_image(img_file_name='update_weights.png', caption='Second step')
            if info_stage_3:
                show_st_image(img_file_name='rank.png', caption='Third step')
            if info_stage_4:
                show_st_image(img_file_name='regression.png', caption='Fourth step')
            if info_stage_5:
                show_st_image(img_file_name='comparison.png', caption='Fifth step')
            pass

