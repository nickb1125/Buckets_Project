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

nba_team = pd.DataFrame(teams.get_teams())

allseasons = ['2009-10','2010-11', '2011-12','2012-13','2013-14','2014-15',
              '2015-16','2016-17','2017-18','2018-19','2019-20','2020-21']

games = pd.DataFrame()
for season in tqdm(allseasons):
    gameappend_regular = endpoints.leaguegamefinder.LeagueGameFinder(season_type_nullable = SeasonType.regular, season_nullable = season).get_data_frames()[0]
    gameappend_playoff = endpoints.leaguegamefinder.LeagueGameFinder(season_type_nullable = SeasonTypePlayoffs.playoffs, 
                                              season_nullable = season).get_data_frames()[0]
    gameappend_regular['PLAYOFF'] = 0 
    gameappend_playoff['PLAYOFF'] = 1
    games = pd.concat([games, pd.DataFrame(gameappend_regular)])
    games = pd.concat([games, pd.DataFrame(gameappend_playoff)]) 
    time.sleep(1)
    

games['WL'] = games['WL'].eq('W').mul(1)
NBA_codes = nba_team['id']
NBAgames = games[games['TEAM_ID'].isin(np.array(NBA_codes))]

## ALSO: Order by time, format times correctly

NBAgames = NBAgames.sort_values(by=['GAME_DATE'], ascending = 0).reset_index(drop = 1)
NBAgames['GAME_DATE'] = pd.to_datetime(NBAgames['GAME_DATE'])

NBAgames['TEAM_ID'] = NBAgames['TEAM_ID'].astype(str)
NBAgames = NBAgames[NBAgames['GAME_DATE'] != datetime.today().strftime('%Y-%m-%d')]

### Now need to combine the same games listed twice for the home and away team
## Must filter to unique games by combinding home and away games (vs or @ in mathchup)

NBAgamesC = pd.DataFrame()
for game_id in tqdm(np.unique(NBAgames['GAME_ID'])):
    game_match = pd.DataFrame(NBAgames[NBAgames['GAME_ID'] == game_id])
    home_game = pd.DataFrame(game_match[game_match['MATCHUP'].str.contains('vs')])
    away_game = pd.DataFrame(game_match[game_match['MATCHUP'].str.contains('@')]).drop(['SEASON_ID',
    'GAME_DATE',
    'GAME_ID'], axis =1)
    home_game.columns = [str(col) + '_H' for col in home_game.columns]
    away_game.columns = [str(col) + '_A' for col in away_game.columns]
    combined = pd.DataFrame(pd.concat([home_game.reset_index(drop=True), away_game.reset_index(drop=True)], axis = 1))
    NBAgamesC = pd.concat([NBAgamesC, combined])


NBAgamesC['GAME_DATE_H'] = pd.to_datetime(NBAgamesC['GAME_DATE_H'])

# Find Team swithes and change to new team name

NBAgamesC = NBAgamesC.drop(['PLUS_MINUS_A','PLUS_MINUS_H'], axis = 1).replace({'NOH': 'NOP',
'NJN': 'BKN',
'SEA': 'OKC',
'VAN': 'MEM',
'KCK': 'SAC',
'CHH': 'CHA',
'SDC': 'LAC',
'UTH': 'UTA',
'SAN': 'SAS',
'GOS': 'GSW',
'PHL': 'PHI',
'NOH': 'NOP',
'New Orleans Hornets': 'New Orleans Pelicans',
'NOK': 'NOP',
'New Orleans/Oklahoma City Hornets': 'New Orleans Pelicans',
'New Orleans Hornets': 'New Orleans Pelicans',
'New Jersey Nets': 'Brooklyn Nets',
'Seattle SuperSonics': 'Oklahoma City Thunder',
'Vancouver Grizzlies': 'Memphis Grizzlies',
'Kansas City Kings': 'Sacramento Kings',
'San Diego Clippers': 'Los Angeles Clippers'}, regex=True).sort_values(by=['GAME_DATE_H'], ascending = 0).reset_index(drop = 1)
NBAgamesC['SEASON_ID_H'] = NBAgamesC['SEASON_ID_H'].str[1:]

# Find games with a team that is not in the NBA and remove

nonNBA = []
for index in tqdm(range(len(np.unique(NBAgamesC['MATCHUP_H'].astype(str))))):
    teams = np.unique(NBAgamesC['MATCHUP_H'].astype(str))[index]
    t1 = teams[0:3]
    t2 = teams[-3:]
    if not t1 in np.array(nba_team['abbreviation']): 
        nonNBA = np.append(nonNBA, t1)
        continue
    if not t2 in np.array(nba_team['abbreviation']):
        nonNBA = np.append(nonNBA, t2)
        
remove = pd.DataFrame()
for team in nonNBA:
    removeappend = NBAgamesC[NBAgamesC['MATCHUP_H'].str.contains(team).fillna(False)]
    removeappend2 = NBAgamesC[NBAgamesC['MATCHUP_A'].str.contains(team).fillna(False)]
    remove = remove.append(removeappend)
    remove = remove.append(removeappend2)

if not remove.empty:
    NBAgamesC = pd.merge(NBAgamesC,remove, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)

## Remove low amounts of missing data (ok with large sample size), create win margin feature
NBAgamesC = NBAgamesC[~NBAgamesC.isna().any(axis=1)] # remove rows with NA (VERY FEW) shouldnt cause bias
NBAgamesC['WINMARGIN_H'] = NBAgamesC['PTS_H'] - NBAgamesC['PTS_A']
NBAgamesC = NBAgamesC.reset_index(drop=1)

NBAgamesC['TEAM_ID_H'] = NBAgamesC['TEAM_ID_H'].astype(int)
NBAgamesC['TEAM_ID_A'] = NBAgamesC['TEAM_ID_A'].astype(int)

NBAgamesC.drop(columns = 'PLAYOFF_A')

NBAgamesC['SEASON_ID_H'] == NBAgamesC['SEASON_ID_H'].str[1:]

NBAgamesC ## FINAL DATA