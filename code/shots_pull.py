from json import JSONDecodeError
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
from nba_api.stats.endpoints import shotchartdetail, shotchartleaguewide, shotchartlineupdetail
from json import JSONDecodeError

def get_all_player_shots(seasons = ['2009', '2010', '2011', '2012', '2013', '2014',
                                    '2015', '2016', '2017', '2018', '2019', '2020'], nba_team = nba_team):
    all_shots = pd.DataFrame()
    for season in seasons:
        season = '{}-{}'.format(int(season), str(int(season) + 1)[2:])
        print(season)
        shots_season = shotchartdetail.ShotChartDetail(team_id='00',
                                player_id='00',
                                season_nullable= season,
                                season_type_all_star=['Regular Season'],
                               context_measure_simple = 'FGA',
    headers={'Accept': 'application/json, text/plain, */*',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'en-US,en;q=0.9',
'Connection': 'keep-alive',
'Host': 'stats.nba.com',
'Origin': 'https://www.nba.com',
'Referer': 'https://www.nba.com/',
'sec-ch-ua': '"Google Chrome";v="87", "\"Not;A\\Brand";v="99", "Chromium";v="87"',
'sec-ch-ua-mobile': '?1',
'Sec-Fetch-Dest': 'empty',
'Sec-Fetch-Mode': 'cors',
'Sec-Fetch-Site': 'same-site',
'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36',
'x-nba-stats-origin': 'stats',
'x-nba-stats-token': 'true'}).get_data_frames()[0]
        shots_playoff = shotchartdetail.ShotChartDetail(team_id=0,
                                player_id=0,
                                season_nullable= season,
                                season_type_all_star=['Playoffs'],
                               context_measure_simple = 'FGA',
    headers={'Accept': 'application/json, text/plain, */*',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'en-US,en;q=0.9',
'Connection': 'keep-alive',
'Host': 'stats.nba.com',
'Origin': 'https://www.nba.com',
'Referer': 'https://www.nba.com/',
'sec-ch-ua': '"Google Chrome";v="87", "\"Not;A\\Brand";v="99", "Chromium";v="87"',
'sec-ch-ua-mobile': '?1',
'Sec-Fetch-Dest': 'empty',
'Sec-Fetch-Mode': 'cors',
'Sec-Fetch-Site': 'same-site',
'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36',
'x-nba-stats-origin': 'stats',
'x-nba-stats-token': 'true'}).get_data_frames()[0]
        all_shots = pd.concat([all_shots, shots_season, shots_playoff])
        time.sleep(1)
    all_shots['GAME_DATE'] = pd.to_datetime(all_shots['GAME_DATE'])
    all_shots = all_shots.rename(columns={"TEAM_NAME": "full_name"})
    all_shots = pd.merge(left=all_shots, right=nba_team[['full_name', 'abbreviation']], left_on='full_name', right_on='full_name')

    return(all_shots)

def get_all_shots(years = ['2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']):
    all_shots = pd.DataFrame()
    for year in years:
        found = False
        while found == False:
            try:
                next_year = get_all_player_shots(seasons = [year])
                found = True
            except JSONDecodeError:
                print('Timeout. Trying again.')
                time.sleep(15)
        all_shots = pd.concat([all_shots, next_year])
    all_shots['Shot_Type_Final'] = all_shots['SHOT_ZONE_AREA'] + all_shots['SHOT_ZONE_RANGE']
    all_shots = all_shots[['GAME_ID', 'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'abbreviation', 'PERIOD', 'SHOT_MADE_FLAG', 
    'LOC_X', 'LOC_Y', 'GAME_DATE', 'Shot_Type_Final', 'SHOT_ZONE_BASIC']]

    return(all_shots)
    

        
all_shots = get_all_shots()