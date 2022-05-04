import ELO_track

def get_all_player_box(seasons = ['2009', '2010', '2011', '2012', '2013', '2014',
                                '2015', '2016', '2017', '2018', '2019', '2020']):
    all_boxes = pd.DataFrame()
    for season in tqdm(seasons):
        season = '{}-{}'.format(int(season), str(int(season) + 1)[2:])
        regular_boxes = endpoints.PlayerGameLogs(season_type_nullable = SeasonType.regular, 
                                              season_nullable = season).get_data_frames()[0]
        
        post_boxes = endpoints.PlayerGameLogs(season_type_nullable = SeasonTypePlayoffs.playoffs, 
                                              season_nullable = season).get_data_frames()[0]


        post_boxes['post'] = 1
        regular_boxes['post'] = 0
        boxes_year = pd.concat([regular_boxes, post_boxes])
        all_boxes = pd.concat([all_boxes, boxes_year])
    all_boxes['GAME_DATE'] = pd.to_datetime(all_boxes['GAME_DATE'].str[:10])
    all_boxes['GAME_ID'] = all_boxes['GAME_ID'].astype(int)
    all_boxes = all_boxes.loc[all_boxes['MIN'] != 0] 
    
    all_boxes = all_boxes[['GAME_DATE', 'SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION',
                           'GAME_ID', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 
                           'FTM', 'FTA', 'FT_PCT', 'REB', 'AST', 'STL', 'BLK', 'PTS', 'OREB', 'DREB', 'TOV']]
    return(all_boxes)
        
        
all_boxes = get_all_player_box()

def min_to_int(minutes_w_seconds):
    minutes = int(minutes_w_seconds.partition(':')[0])
    seconds = int(minutes_w_seconds.partition(':')[2])/60
    time_total = round((minutes + seconds), 2)
    return(time_total)

def all_box_score_fixer(all_boxes, NBAgamesC): ## for some games, the nba_api skips box scores (play in games, etc) 
    unique_boxes_games = ['00' + str(x) for x in list(all_boxes['GAME_ID'].unique())]
    unique_NBAgamesC_games = list(NBAgamesC['GAME_ID_H'].unique())
    
    missing = list(set(unique_NBAgamesC_games) - set(unique_boxes_games))
    missing = list(map(str, missing))
    missing = [('0' * (10 - len(x))) + x for x in missing]
    
    missing_boxes = pd.DataFrame()
    for code in tqdm(missing):
        add = endpoints.BoxScoreTraditionalV2(game_id = code).get_data_frames()[0]
        time.sleep(0.2)
        season = NBAgamesC[NBAgamesC['GAME_ID_H'] == code]['SEASON_ID_H'].iloc[0]
        season_id = '{}-{}'.format(season, (int(season)+1) - 2000)
        add = add[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION',
                           'GAME_ID', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 
                           'FTM', 'FTA', 'FT_PCT', 'REB', 'AST', 'STL', 'BLK', 'PTS']]
        add['SEASON_YEAR'] = season_id
        add = add[~add['MIN'].isna()]
        add['MIN'] = [min_to_int(sub) for sub in add['MIN']]
        
        
        missing_boxes = pd.concat([missing_boxes, add])
    all_boxes_fixed = pd.concat([missing_boxes, all_boxes])
    all_boxes_fixed = all_boxes_fixed[all_boxes_fixed['GAME_ID'].isin([int(x) for x in unique_NBAgamesC_games])].reset_index(drop = 1)
    return(all_boxes_fixed)

all_boxes = all_box_score_fixer(all_boxes, NBAgamesC).reset_index(drop = 1)

player_dict = players.get_players()

def get_curr_injuries():
    all_injuries = []
    recent_injuries = pd.read_html('https://www.espn.com/nba/injuries')
    for team in range(len(recent_injuries)):
        all_injuries.extend(list(recent_injuries[team]['NAME']))
    return all_injuries

def get_player_games(player1, all_boxes):
    
    player_gamelog = all_boxes[all_boxes['PLAYER_NAME'] == player1].reset_index(drop = 1)['GAME_ID']
    
    return(player_gamelog)

def make_rosters(seasonid, nba_team = nba_team): ## takes in te
    d = {}
    for teamid in tqdm(nba_team['id']):
        roster = endpoints.commonteamroster.CommonTeamRoster(team_id = teamid, season = seasonid).get_data_frames()[0]
        d.update({teamid : roster})
        time.sleep(.5)
    return d

rosters_2020 = make_rosters(2020)
rosters_2019 = make_rosters(2019)
rosters_2018 = make_rosters(2018)
rosters_2017 = make_rosters(2017)
rosters_2016 = make_rosters(2016)
rosters_2015 = make_rosters(2015)
rosters_2014 = make_rosters(2014)
rosters_2013 = make_rosters(2013)
rosters_2012 = make_rosters(2012)
rosters_2011 = make_rosters(2011)
rosters_2010 = make_rosters(2010)
rosters_2009 = make_rosters(2009)

folded_roster = {2020: rosters_2020, 2019: rosters_2019,
                  2018: rosters_2018,2017: rosters_2017,
                  2016: rosters_2016, 2015: rosters_2015,
                 2014: rosters_2014, 2013: rosters_2013,
                 2012: rosters_2012, 2011: rosters_2011,
                 2010: rosters_2010,
                 2009: rosters_2009}

def create_team_injury_matrix(team_id, year, nested_rosters = nested_rosters, NBAgamesC = NBAgamesC, all_boxes = NBAgamesC):
    rosters = nested_rosters.get(year)
    players = rosters.get(team_id)['PLAYER']
    played = {}
    season_games = NBAgamesC.loc[NBAgamesC['SEASON_ID_H'] == str(year)]
    team_games = season_games[(season_games['TEAM_ID_H'] == team_id) | (season_games['TEAM_ID_A'] == team_id)]
    cols = ['game']
    cols.extend(team_games['GAME_ID_H'].unique())
    played_df = pd.DataFrame(columns = cols)
    for player in players:
        played = [player]
        try:
            all_played = [ int(x) for x in list(get_player_games(player, all_boxes))]
        except:
            played.extend(list(np.repeat(False, len(played_df.columns) - 1)))
            played = pd.DataFrame(played).T
            played.columns = cols
            played_df = pd.concat([played_df, played], ignore_index=True)
            continue
        played = [player]
        for game in list(team_games['GAME_ID_H'].unique()):
            game_check = int(game)
            played.append((game_check in all_played))
        played = pd.DataFrame(played).T
        played.columns = cols
        played_df = pd.concat([played_df, played])
    played_df = played_df.T
    played_df.columns = played_df.iloc[0]
    played_df = played_df.iloc[1:,]
    
    out = []
    
    for row in range(len(played_df.index)):
        out_add = list(compress(list(played_df.iloc[row,].index), list(played_df.iloc[row,] == False)))
        out.append(out_add)
        
    played_df['out'] = out
    
    return(played_df[['out']])


def injuries_by_team_dict(year, nba_team, nested_rosters, NBAgamesC, all_boxes):
    d = {}
    for teamid in tqdm(nba_team['id']):
        out_track = create_team_injury_matrix(teamid, year, nested_rosters, NBAgamesC, all_boxes)
        d.update({teamid : out_track})
    return d

out_2020 = injuries_by_team_dict(2020, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2019 = injuries_by_team_dict(2019, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2018 = injuries_by_team_dict(2018, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2017 = injuries_by_team_dict(2017, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2016 = injuries_by_team_dict(2016, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2015 = injuries_by_team_dict(2015, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2014 = injuries_by_team_dict(2014, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2013 = injuries_by_team_dict(2013, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2012 = injuries_by_team_dict(2012, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2011 = injuries_by_team_dict(2011, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2010 = injuries_by_team_dict(2010, nba_team, nested_rosters, NBAgamesC, all_boxes)
out_2009 = injuries_by_team_dict(2009, nba_team, nested_rosters, NBAgamesC, all_boxes)

folded_out = {2020: out_2020, 2019: out_2019,
                  2018: out_2018,2017: out_2017,
                  2016: out_2016, 2015: out_2015,
              2014: out_2014, 2013: out_2013,
                  2012: out_2012, 2011: out_2011,
             2010: out_2010, 2009: out_2009}

def nested_dict_unfolder(nested, nba_team): ## formats nested df so it can be saved
    unfolded = pd.DataFrame()
    for year in tqdm([2009, 2010, 2011, 2012, 2013, 2014,
                                  2015, 2016, 2017, 2018, 2019, 2020]):
        nested_y = nested.get(year)
        for teamid in nba_team['id']:
            team_df = pd.DataFrame(nested_y.get(teamid))
            unfolded = pd.concat([unfolded, team_df])
    return(unfolded)

unfolded_rosters = nested_dict_unfolder(folded_roster, nba_team).reset_index(drop = 1)
unfolded_out = nested_dict_unfolder(folded_out, nba_team).reset_index(drop = 1)

def nested_dict_folder(unnested, nba_team): ## unfolds 
    folded = {}
    for year in tqdm([2009, 2010, 2011, 2012, 2013, 2014,
                                  2015, 2016, 2017, 2018, 2019, 2020]):
        unnested_y = unnested.loc[unnested['year'] == year]
        inner = {}
        for teamid in nba_team['id']:
            team_df = unnested_y.loc[unnested_y['team_id'] == teamid] ## drop added columns
            inner.update({teamid: team_df})
        folded.update({year:inner})
    return(folded)
