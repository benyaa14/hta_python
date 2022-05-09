"""Frameworks for running multiple Streamlit applications as a single app.
"""
import streamlit as st

st.set_page_config(layout="wide")
# import main as home
import home as h
import main as upload_player_in_game
import weights_app as wa
import rank_app as ra
import reg_app as rea
import db_manual_updates as dbmu
import players_comparison_app as pca
import update_player_team_league as uptl
import optimization_inputs as oi
from streamlit_option_menu import option_menu


class MultiApp:

    def __init__(self):
        self.apps = []
        self.apps_name = []

    def add_app(self, title, func):
        """Adds a new application.
        Parameters
        ----------
        func:
            the python function to render this app.
        title:
            title of the app. Appears in the dropdown in the sidebar.
        """
        self.apps.append({
            "title": title,
            "function": func
        })
        self.apps_name.append(title)

    def run(self):
        # app = st.sidebar.radio(
        # app = st.sidebar.selectbox(
        #     'App pages',
        #     self.apps,
        #     format_func=lambda app: app['title'])
        #
        # app['function']()
        with st.sidebar:
            app_title = option_menu("Main Menu", self.apps_name,
                                    icons=['house-fill','cloud-upload', 'sliders','diagram-3-fill','bezier2','bar-chart-line','server','file-person','boxes'], menu_icon="cast")

        for app_dict in self.apps:
            if app_dict['title'] == app_title:
                app_dict['function']()

app = MultiApp()

app.add_app("Home",h.app)
app.add_app("Upload player in game files", upload_player_in_game.app)
app.add_app("Update weights", wa.app)
app.add_app("Rank players", ra.app)
app.add_app("Run regression", rea.app)
app.add_app("Players ranking comparison", pca.app)
app.add_app("Update DB manually", dbmu.app)
app.add_app("Update player table", uptl.app)
app.add_app("Optimization model", oi.app)



if __name__ == '__main__':
    try:
        app.run()
    except Exception as e:
        st.exception(e)

