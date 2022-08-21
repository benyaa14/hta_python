ALL_POSITIONS_STR = "All positions"
SELECTION_ERROR = 'selection_error'
PATH = 'new_files_to_update'
# PATH_PLAYERS = '/content/gdrive/MyDrive/HTA project/Position and attributes analysis/files/league_to_update'

# DB auth
# HOST = "hta-project.cf9mllj1rhry.us-east-2.rds.amazonaws.com"
HOST = "hta-project3.c4rdsjz7fetg.eu-west-2.rds.amazonaws.com"
USER = 'Sagi'
PASSWORD = "HTAproject2022"
# DB = 'hta_project'
DB = 'hta_project3'




# Tables
TEAM_TABLE_NAME = 'teams'
LEAGUE_TABLE = 'league'
PLAYER_IN_GAME_TABLE = 'player_in_game'
WEIGHTS_TABLE = 'att_to_weight'
TEAM_IN_LEAGUE_TABLE = 'team_in_league'
LIKELIHOOD_WEIGHTS_TABLE = 'att_to_weight_likelihood'
PLAYER_TABLE = 'player'
PLAYERS_TRANSFERMARKET_TABLE = 'players_transfermarket'
INSTAT_DATA_TABLE = 'instat_player_data'
GAME_ON_DATE = 'game_on_date'
SQUAD_IN_SEASON = 'squad_in_season_transfer'
PLAYER_IN_GAME_RAW_TABLE = 'player_in_game_raw'


# Collumns
GAME_RANK_COL = 'game_rank'
INSTAT_INDEX = 'instat_index'
POSTERIOR_RANK = 'game_rank_posterior'
LIKELIHOOD_RANK = 'game_rank_likelihood'
PRIMARY_KEYS_PLAYER_IN_GAME = ["player_id", "game_date"]
POSITION_COL = 'position'
REDUCE_RATING_COLUMNS = ['red_cards']
LEAGUE_ID_COL = 'l_id'

POSITIONS = ["LD", "LM", "RM", "RD", "DM", "CM", "CD", "F"]

MAIN_LEAGUE = "Ligat ha'Al"

DICT_TRANSFERMARKET_POS_TO_INSTAT_POS = {'Goalkeeper':'GK','Centre-Back':'CD','Left-Back':'LD','Right-Back':'RD','Defensive Midfield':'DM','Central Midfield':'CM',
                         'Attacking Midfield':'CM','Left Winger':'LM','Right Winger':'RM','Centre-Forward':'F','midfield':'CM','Left Midfield':'LM',
                         'attack':'F','Right Midfield':'RM', 'Defender':'CD','Second Striker':'F'}