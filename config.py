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
TIL_TO_TAG_MANUALLY = 'til_to_tag_manually'
TMP_PLAYER_IN_GAME = 'tmp_player_in_game'


# Collumns
GAME_RANK_COL = 'game_rank'
INSTAT_INDEX = 'instat_index'
POSTERIOR_RANK = 'game_rank_posterior'
LIKELIHOOD_RANK = 'game_rank_likelihood'
PRIMARY_KEYS_PLAYER_IN_GAME = ["player_id", "game_date"]
POSITION_COL = 'position'
REDUCE_RATING_COLUMNS = ['red_cards','offsides', 'lost_balls', 'lost_balls_in_own_half','shots_wide','opponents_xg_with_a_player_on']
LEAGUE_ID_COL = 'l_id'

POSITIONS = ["LD", "LM", "RM", "RD", "DM", "CM", "CD", "F"]

MAIN_LEAGUE = "Ligat ha'Al"

DICT_TRANSFERMARKET_POS_TO_INSTAT_POS = {'Goalkeeper':'GK','Centre-Back':'CD','Left-Back':'LD','Right-Back':'RD','Defensive Midfield':'DM','Central Midfield':'CM',
                         'Attacking Midfield':'CM','Left Winger':'LM','Right Winger':'RM','Centre-Forward':'F','midfield':'CM','Left Midfield':'LM',
                         'attack':'F','Right Midfield':'RM', 'Defender':'CD','Second Striker':'F'}


COLS_TO_NORM=['instat_index','minutes_played','goals', 'assists', 'chances',
       'chances_successful', 'chances_per_of_conversion', 'сhances_created',
       'fouls', 'fouls_suffered', 'yellow_cards', 'red_cards', 'offsides',
       'total_actions', 'successful_actions', 'successful_actions_per',
       'shots', 'shots_on_target', 'shots_on_target_per', 'shots_wide',
       'blocked_shots', 'shots_on_post__bar', 'passes', 'accurate_passes',
       'accurate_passes_per', 'key_passes', 'key_passes_accurate', 'crosses',
       'crosses_accurate', 'accurate_crosses_per', 'challenges',
       'challenges_won', 'challenges_won_per', 'defensive_challenges',
       'defensive_challenges_won', 'challenges_in_defence_won_per',
       'attacking_challenges', 'attacking_challenges_won',
       'challenges_in_attack_won_per', 'air_challenges', 'air_challenges_won',
       'air_challenges_won_per', 'dribbles', 'dribbles_successful',
       'successful_dribbles_per', 'tackles', 'tackles_successful',
       'tackles_won_per', 'ball_interceptions', 'free_ball_pick_ups',
       'lost_balls', 'lost_balls_in_own_half', 'ball_recoveries',
       'ball_recoveries_in_opponents_half', 'xg_expected_goals',
       'expected_assists', 'xg_per_shot', 'xg_per_goal', 'xg_conversion',
       'xg_with_a_player_on', 'opponents_xg_with_a_player_on',
       'net_xg_xg_player_on__opp_teams_xg',
       'defensive_xg_xg_of_shots_made_by_guarded_player',
       'defensive_xg_per_shot', 'gf', 'ga']


COLS_TO_MINUTES_STANDARDIZATION=['goals', 'assists', 'chances','сhances_created',
       'fouls', 'fouls_suffered', 'yellow_cards', 'red_cards', 'offsides',
       'total_actions', 'successful_actions','shots', 'shots_on_target','shots_wide',
       'blocked_shots', 'shots_on_post__bar', 'passes', 'accurate_passes','key_passes',
        'key_passes_accurate', 'crosses','crosses_accurate','challenges',
       'challenges_won','defensive_challenges',
       'defensive_challenges_won','attacking_challenges', 'attacking_challenges_won','air_challenges', 'air_challenges_won',
       'dribbles', 'dribbles_successful',
        'tackles', 'tackles_successful','ball_interceptions', 'free_ball_pick_ups',
       'lost_balls', 'lost_balls_in_own_half', 'ball_recoveries',
       'ball_recoveries_in_opponents_half']