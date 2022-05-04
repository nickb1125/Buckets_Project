## by running this file, you also run all the neccecary other files to a full data pull and clean. This can take an hour or so. 
## Data organization in this step is aloso extreamly comuptationally intensive, and will take around 3 hours to run.
## Expect this script to take up to 4 or 5 hours to run.
import nba_api
import pandas as pd
import numpy as np
import requests
import time
import datetime as dt
from nba_api.stats.static import players
import math
from datetime import datetime, timedelta
from nba_api.stats.library.parameters import SeasonAllNullable
from tqdm import tqdm
from scipy.stats import norm
from nba_api.stats import endpoints
from nba_api.stats.static import teams
from nba_api.stats.library.parameters import SeasonType
from nba_api.stats.library.parameters import SeasonTypePlayoffs
from nba_api.stats.library.parameters import  SeasonNullable
from itertools import compress

import roster_injury_pull
import shots_pull

def get_player_games_before(num_games, date, player_id, all_boxes):
    date = pd.to_datetime(date)
    player_box = all_boxes.loc[all_boxes['PLAYER_ID'] == player_id] 
    player_box_before = player_box.loc[player_box['GAME_DATE'] < date].sort_values('GAME_DATE', 
                                                                               ascending = False).reset_index(drop = 1).iloc[:num_games]
    return(player_box_before)

def get_player_playoff_min(date, player_id, all_boxes, NBAgamesC):
    date = pd.to_datetime(date)
    all_playoff_games = NBAgamesC.loc[NBAgamesC['PLAYOFF_H'] == 1]
    all_playoff_games_unique = all_playoff_games['GAME_ID_H'].unique()
    all_player_games = all_boxes.loc[all_boxes['PLAYER_ID'] == player_id]
    all_player_games = all_player_games.loc[all_player_games['GAME_DATE'] < date]
    all_player_playoff_games = all_player_games.loc[all_player_games['GAME_ID'].isin(all_playoff_games_unique)]
    all_playoff_min = all_player_playoff_games['MIN'].sum()
    return(all_playoff_min)


def get_team_playoff_games_past_5_years(teamabb, date, NBAgamesC, current_season):
    team_playoff_games = NBAgamesC.loc[(NBAgamesC['PLAYOFF_H'] == 1)]
    team_playoff_games = team_playoff_games.loc[(team_playoff_games['TEAM_ABBREVIATION_H'] == teamabb) | (team_playoff_games['TEAM_ABBREVIATION_A'] == teamabb)]
    date = pd.to_datetime(date)
    season = int(NBAgamesC.loc[NBAgamesC['GAME_DATE_H'] <= date].reset_index(drop = 1).iloc[0,]['SEASON_ID_H'])
    if NBAgamesC[NBAgamesC['GAME_DATE_H'] > date].empty:
                 season = current_season
    past_season = int(season) - 1
    past_5_seasons = [past_season, past_season - 1, past_season - 2, past_season -3, past_season -4]
    team_playoff_games_past_5 = team_playoff_games.loc[team_playoff_games['SEASON_ID_H'].isin(past_5_seasons)]
    return(len(team_playoff_games_past_5))

def get_player_avgs_over_last_x_games(num_games, date, player_id, all_boxes):
    date = pd.to_datetime(date)
    player_box_before = get_player_games_before(num_games = num_games, date = date, player_id = player_id, all_boxes = all_boxes)
    try:
        player_name = player_box_before['PLAYER_NAME'].unique()[0]
    except:
        final = pd.DataFrame({'player': np.repeat(0, 14), 'var': ['FG_PCT', 'FGA', 'FG3_PCT', 'FG3A', 'FT_PCT', 
                      'FTA', 'OREB', 'DREB', 'TOV', 'AST', 'STL', 'BLK', 'PTS', 'MIN']}).T.reset_index(drop = 1)
        final.columns = final.iloc[1,]
        final = final.drop(1, axis = 0)
        final['PLAYER_NAME'] = 'G-League'
        return(final)
    player_box_before = player_box_before[['FG_PCT', 'FGA', 'FG3_PCT', 'FG3A', 'FT_PCT', 
                      'FTA', 'OREB', 'DREB', 'TOV', 'AST', 'STL', 'BLK', 'PTS', 'MIN']]
    player_box_avgs = list(player_box_before.mean(axis = 0))
    player_box_avgs_cols = list(player_box_before.columns)
    final = pd.DataFrame({'player': player_box_avgs, 'var': player_box_avgs_cols}).T.reset_index(drop = 1)
    final.columns = final.iloc[1,]
    final = final.drop(1, axis = 0)
    final['PLAYER_NAME'] = player_name
    
    return(final)

def get_team_avgs_over_last_x_games(team_abb, date, num_games_for_avg, folded_roster, NBAgamesC,
                                    all_boxes, nba_team, current_season): 
    teamname = nba_team.loc[nba_team['abbreviation'] == team_abb]['full_name'].values[0]
    teamid = nba_team.loc[nba_team['abbreviation'] == team_abb]['id'].values[0]
    date = pd.to_datetime(date)
    season = int(NBAgamesC.loc[NBAgamesC['GAME_DATE_H'] <= date].reset_index(drop = 1).iloc[0,]['SEASON_ID_H'])
    if NBAgamesC.loc[NBAgamesC['GAME_DATE_H'] == date].empty:
                 season = current_season
    roster = folded_roster.get(int(season)).get(teamid)
    
    team_stats = pd.DataFrame()
    for playerid in roster['PLAYER_ID']:
        playerstat = get_player_avgs_over_last_x_games(num_games = num_games_for_avg, date = date, 
                                                       player_id = playerid, all_boxes = all_boxes)
        playerstat['ALL_TIME_PLAYOFF_MIN'] = get_player_playoff_min(date, playerid, all_boxes, NBAgamesC)
        team_stats = pd.concat([team_stats, playerstat])
    team_stats = team_stats.fillna(0)
    return(team_stats)

def get_out_players_for_game(team_id, game_id, folded_out, NBAgamesC):
    game = NBAgamesC.loc[NBAgamesC['GAME_ID_H'] == game_id]
    season = int(game['SEASON_ID_H'])
    out = folded_out.get(int(season)).get(team_id)
    try:
        out_game = str(out[out['game_id'] == int(game_id)].reset_index(drop = 1)['out'][0])
    except:
        out_game = str(out[out['game_id'] == str(game_id)].reset_index(drop = 1)['out'][0])
    
    return(eval(out_game))

def create_team_predictions(team_abb, date, num_games_for_avg, ELO_track, all_boxes, nba_team, NBAgamesC, folded_out, folded_roster, 
                            current_season, upcoming):
    
    team_id = nba_team.loc[nba_team['abbreviation'] == team_abb]['id'].reset_index(drop =1)[0]
    team_stats = get_team_avgs_over_last_x_games(team_abb, date = date, num_games_for_avg = num_games_for_avg, 
                                folded_roster = folded_roster, NBAgamesC = NBAgamesC, 
                                                 all_boxes = all_boxes, nba_team = nba_team,
                                                current_season = current_season)
    
    ## account for injuries
    if upcoming == False:
        date = pd.to_datetime(date)
        games_before_date = NBAgamesC.loc[NBAgamesC['GAME_DATE_H'] <= date]
        game_before_date_team = games_before_date.loc[(games_before_date['TEAM_ABBREVIATION_H'] == team_abb) | 
                                                   (games_before_date['TEAM_ABBREVIATION_A'] == team_abb)].iloc[0,]
        game_id = game_before_date_team['GAME_ID_H']
        out = get_out_players_for_game(team_id = team_id, game_id = game_id, folded_out =  folded_out, 
                                       NBAgamesC = NBAgamesC)
    else:
        games_before_date = NBAgamesC.loc[NBAgamesC['GAME_DATE_H'] <= date]
        game_before_date_team = games_before_date.loc[(games_before_date['TEAM_ABBREVIATION_H'] == team_abb) | 
                                                   (games_before_date['TEAM_ABBREVIATION_A'] == team_abb)].iloc[0,]
        season = current_season
        out_all = get_curr_injuries()
        team_roster = list(folded_roster.get(int(season)).get(team_id)['PLAYER'].reset_index(drop = 1))
        out = list(set.intersection(set(out_all), set(team_roster)))
    
    team_stats = team_stats.loc[~(team_stats['PLAYER_NAME'].isin(out))] # update team stats to dislude injured players

    ## need to account for players stepping up when players are out . NOTE: percentages probs wont change
    
    min_sum = team_stats['MIN'].sum()
    min_in_game = 48*5
    min_diff = min_in_game - min_sum
    fix_time_amount = min_diff / len(team_stats)
    new_min_proj = team_stats['MIN'] + fix_time_amount
    proportion_all_player_time = new_min_proj / min_in_game
    proportion_orig_time = list(new_min_proj / team_stats['MIN'])
    stays_same = team_stats[['FG_PCT', 'FG3_PCT', 'FT_PCT', 'PLAYER_NAME']]
    
    playoff_min = team_stats[['ALL_TIME_PLAYOFF_MIN']].mul(proportion_all_player_time, axis=0)
    
    team_stats = team_stats.drop(['PLAYER_NAME', 'FG_PCT', 
                                  'FG3_PCT', 'FT_PCT', 'ALL_TIME_PLAYOFF_MIN'], 
                                 axis = 1).mul(proportion_orig_time, axis=0)
    
    team_stats = pd.concat([team_stats, stays_same, playoff_min], axis=1)
    
    ## take weighted averages
    fga = team_stats['FGA'].sum()
    fg3a = team_stats['FG3A'].sum()
    fta = team_stats['FTA'].sum()
    ast = team_stats['AST'].sum()
    stl = team_stats['STL'].sum()
    blk = team_stats['BLK'].sum()
    oreb = team_stats['OREB'].sum()
    dreb = team_stats['DREB'].sum()
    tov = team_stats['TOV'].sum()
    fg_p = (team_stats['FG_PCT'] * team_stats['FGA']).sum() / team_stats['FGA'].sum()
    fg_p3 = (team_stats['FG3_PCT'] * team_stats['FG3A']).sum() / team_stats['FG3A'].sum()
    ft_p = (team_stats['FT_PCT'] * team_stats['FTA']).sum() / team_stats['FTA'].sum()
    tplayoff_min = team_stats['ALL_TIME_PLAYOFF_MIN'].sum()
    elo = get_pre_elo(team_abb, date, NBAgamesC = NBAgamesC, ELO_track = ELO_track)
    pts = team_stats['PTS'].sum()
    
    ## calculate p_win over last games, avg_margin over last games, and avg_points_allowed over last games
    games_before = NBAgamesC.loc[NBAgamesC['GAME_DATE_H'] < date]
    games_before = games_before.loc[(games_before['TEAM_ABBREVIATION_H'] == team_abb) | 
                                                   (games_before['TEAM_ABBREVIATION_A'] == team_abb)][0:num_games_for_avg]
    wins = 0
    margin_of_victory = []
    points_allowed = []
    for row in range(len(games_before)):
        game = games_before.iloc[row,]
        if game['TEAM_ABBREVIATION_H'] == team_abb:
            wins += game['WL_H']
            margin_of_victory.append(game['WINMARGIN_H'])
            points_allowed.append(game['PTS_A'])
        else:
            if game['WL_H'] == 0:
                wins += 1  
            margin_of_victory.append(-game['WINMARGIN_H'])
            points_allowed.append(game['PTS_H'])
    p_win = wins / num_games_for_avg
    margin_of_victory = sum(margin_of_victory) / num_games_for_avg
    avg_points_allowed = sum(points_allowed) / num_games_for_avg
    back_to_back = int(games_before.iloc[0,]['GAME_DATE_H'] == pd.to_datetime(date) - timedelta(1))
    schedule = pd.concat([games_before['TEAM_ABBREVIATION_H'], games_before['TEAM_ABBREVIATION_A']])
    schedule = schedule.loc[schedule != 'BOS']
    scheudle_elos = []
    for team in schedule:
        elo_opp = get_pre_elo(team, '2021-09-15', NBAgamesC = NBAgamesC, ELO_track = ELO_track)
        scheudle_elos.append(elo_opp)
    elo_rigor = sum(scheudle_elos) / len(scheudle_elos)
    
    predict_row = pd.DataFrame({'var' : ['FGA', 'FG3A', 'FTA', 'OREB', 'DREB', 'TOV', 'AST', 'STL', 'BLK', 
                                         'FG_P', 'FG_P3', 'FT_P', 'ELO', 'PTS', 'AVG_PLAYOFF_MIN', 'P_WIN', 'AVG_MARGIN', 'AVG_PTS_ALLOWED', 'BTB', 'RIGOR_SCHEDULE'],
                               'value' : [fga, fg3a, fta, oreb, dreb, tov, ast, stl, blk, fg_p, 
                                          fg_p3, ft_p, elo, pts, tplayoff_min, p_win, margin_of_victory, avg_points_allowed, back_to_back, elo_rigor]}).T.reset_index(drop =1)
    
    predict_row.columns = predict_row.iloc[0,]
    predict_row= predict_row.drop(0, axis = 0)
    

    
    ### NEED TO account for teams playing less hard/more hard at end of season based on playoff needs
    
    
    
    return(predict_row)


def pregame_stat_2(team_h_abb, team_a_abb, gamedate, NBAgamesC, NBAgames, nba_team, 
                                  ELO_track, all_boxes, folded_out, folded_roster, 
                   num_games_for_avg, current_season, upcoming, playoff = 0):
    if playoff == 1: ## if playoff game, use last 35 games instead since players play more consistantly
        home_pred = create_team_predictions(team_h_abb, gamedate, num_games_for_avg = num_games_for_avg, folded_roster = folded_roster, 
                        all_boxes = all_boxes, folded_out = folded_out, NBAgamesC = NBAgamesC, nba_team = nba_team, 
                                            ELO_track = ELO_track, upcoming = upcoming, current_season = current_season)
        away_pred = create_team_predictions(team_a_abb, gamedate, num_games_for_avg = num_games_for_avg,  NBAgamesC = NBAgamesC,
                                        folded_roster = folded_roster, folded_out = folded_out, all_boxes = all_boxes, nba_team = nba_team, 
                                        ELO_track = ELO_track, upcoming = upcoming, current_season = current_season)
    else:
        home_pred = create_team_predictions(team_h_abb, gamedate, num_games_for_avg = num_games_for_avg, folded_roster = folded_roster, 
                        all_boxes = all_boxes, folded_out = folded_out, NBAgamesC = NBAgamesC, 
                                            nba_team = nba_team, ELO_track = ELO_track, upcoming = upcoming, current_season = current_season)
        away_pred = create_team_predictions(team_a_abb, gamedate, num_games_for_avg = num_games_for_avg,  NBAgamesC = NBAgamesC,
                                        folded_roster = folded_roster, folded_out = folded_out, all_boxes = all_boxes, nba_team = nba_team, 
                                        ELO_track = ELO_track, upcoming = upcoming, current_season = current_season)
        
    home_pred.columns = [str(col) + '_H' for col in home_pred.columns]
    
    away_pred.columns = [str(col) + '_A' for col in away_pred.columns]
    
    ## calculate away win percentages (highly correlated for playoff interaction)
    
    season = int(NBAgamesC.loc[NBAgamesC['GAME_DATE_H'] <= gamedate].reset_index(drop = 1).iloc[0,]['SEASON_ID_H'])
    if NBAgamesC.loc[NBAgamesC['GAME_DATE_H'] > gamedate].empty:
                 season = current_season
    a_win_p_A = NBAgamesC.loc[(NBAgamesC['GAME_DATE_H'] < gamedate) & (NBAgamesC['TEAM_ABBREVIATION_A'] == team_a_abb) & (NBAgamesC['SEASON_ID_H'] == str(season))]
    if len(a_win_p_A) != 0:
        a_win_p_A = (len(a_win_p_A) - a_win_p_A['WL_H'].sum()) / len(a_win_p_A)
    else:
        a_win_p_A = np.nan

    away_pred['ROAD_WIN_P_A'] = a_win_p_A
    
    
    combined = pd.DataFrame(pd.concat([home_pred.reset_index(drop=True), away_pred.reset_index(drop=True)], axis = 1))
    
    ## add binary playoff variable
    if playoff == 1:
        combined['PLAYOFF'] = 1
    else:
        combined['PLAYOFF'] = 0
        
    ## add number of playoff games played by both teams in past 5
    combined['TEAM_PLAYOFF_GAMES_P5_H'] = get_team_playoff_games_past_5_years(team_h_abb, gamedate, NBAgamesC ,current_season)
    combined['TEAM_PLAYOFF_GAMES_P5_A'] = get_team_playoff_games_past_5_years(team_a_abb, gamedate, NBAgamesC,current_season)
    
    

    return(combined)
    

def create_predictdf_2(sample, NBAgamesC, NBAgames, nba_team, ELO_track, num_games_for_avg, 
                       all_boxes, folded_roster, folded_out, current_season, upcoming = False): ## sample is subset of NBAgamesC dataframe
    predictdf = pd.DataFrame()
    sample = sample.reset_index(drop = 1)
    
    for index in tqdm(range(len(sample))):
        
        team_h_abb = sample.iloc[index,:][['MATCHUP_H']][0][0:3]
        team_a_abb = sample.iloc[index,:][['MATCHUP_H']][0][-3:]
        gamedate = sample.iloc[index,:][['GAME_DATE_H']][0]
        playoff = sample.iloc[index,:][['PLAYOFF_H']][0]
    
        predictrow = pregame_stat_2(team_h_abb, team_a_abb, gamedate, NBAgamesC = NBAgamesC, NBAgames = NBAgames, nba_team = nba_team, 
                                  ELO_track = ELO_track, all_boxes = all_boxes, num_games_for_avg = num_games_for_avg, 
                                    folded_roster = folded_roster, folded_out = folded_out, playoff = playoff, current_season = current_season, 
                                    upcoming = upcoming)
    
        predictdf = pd.concat([predictdf, predictrow])
     
    return predictdf

import warnings
warnings.simplefilter("ignore", UserWarning)

## create massive training set that includes all games with complete averages possibily computed after 2010

training_data = NBAgamesC[NBAgamesC['SEASON_ID_H'].map(int) > 2009]

predictdf_10 = create_predictdf_2(sample = training_data, NBAgames = NBAgames, 
NBAgamesC = NBAgamesC, nba_team = nba_team, ELO_track = ELO_track, 
all_boxes = all_boxes, num_games_for_avg = 10, folded_roster = folded_roster, folded_out = folded_out, 
current_season = 2021)

pd.DataFrame.to_csv(predictdf_10, 'data/predictdf_10')
pd.DataFrame.to_csv(NBAgamesC, 'data/NBAgamesC')
pd.DataFrame.to_csv(ELO_track, 'data/ELO_track')
pd.DataFrame.to_csv(NBAgames, 'data/NBAgames')
pd.DataFrame.to_csv(unfolded_rosters, 'data/unfolded_rosters')
pd.DataFrame.to_csv(unfolded_out, 'data/unfolded_out')
pd.DataFrame.to_csv(nba_team, 'data/nba_team')
pd.DataFrame.to_csv(all_shots, 'data/all_shots')



