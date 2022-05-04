import games_pull


url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-elo/nbaallelo.csv"
five38rate = pd.read_csv(url, index_col=0)


five38rate= five38rate.replace({'NOH': 'NOP',
                               'NOK': 'NOP',
                               'NOH': 'NOP',
                               'NJN': 'BKN',
                               'SEA': 'OKC',
                               'VAN': 'MEM',
                               'KCK': 'SAC',
                               'SDC': 'LAC',
                               'WSB': 'WAS',
                               'PHO': 'PHX'}, regex=True)




games15 = five38rate[five38rate['year_id'] == 2008].sort_values('date_game')

def get_ELO_15(team_abb):
    games15team = games15[games15['team_id'] == team_abb]
    if games15team.empty:
        teamlltime = five38rate[five38rate['team_id'] == team_abb]
        rating = teamlltime.iloc[-1,:]['elo_n']
    else:
        rating = games15team.iloc[-1,:]['elo_n']
    return(rating)

## define function to track elo over time and create ongoing dataframe

## ELO Rating # FIX INTER_SEASON CHANGES
def get_ELO_track():
    AllSeasonGames = pd.DataFrame(NBAgamesC).iloc[::-1].reset_index(drop = 1) ## In correct order
    CurrSeason = AllSeasonGames.iloc[0,]['SEASON_ID_H']
    
    ELO_track = pd.DataFrame() ## Creating starting ELOs from the fivethirtyeight 1982 data
    for team in nba_team['abbreviation']:
        ELO_start = {'Team': team,
                     'ELO': get_ELO_15(team),
                     'Date': AllSeasonGames.iloc[0,]['GAME_DATE_H'],
                     'Season': CurrSeason}
        ELO_track = pd.concat([ELO_track, pd.DataFrame(ELO_start, index=[0])])
    date = AllSeasonGames.iloc[0,]['GAME_DATE_H']
    end_date = AllSeasonGames.iloc[-1,]['GAME_DATE_H']
    delta = dt.timedelta(days=1)
    seasonindex = 1
    ELO_track = ELO_track.reset_index(drop = 1)
    
    while date <= end_date:
        ThisDayGames = pd.DataFrame(AllSeasonGames[AllSeasonGames['GAME_DATE_H'] == date]).reset_index(drop = 1)
        if ThisDayGames.empty:
            date += delta
            continue
        if ThisDayGames[['SEASON_ID_H']].iloc[0,][0] != CurrSeason: 
            for team in np.array(nba_team['abbreviation']):
                old_ELO = ELO_track[ELO_track['Team'] == team][['ELO']].iloc[-1,:]
                new_ELO = (.75 * old_ELO) + (.25 * 1505)
                newELOadd = {'Team': team,
                             'ELO': new_ELO,
                             'Date': date - delta,
                             'Season': ThisDayGames[['SEASON_ID_H']].iloc[0,][0]
                             }
            CurrSeason = ThisDayGames[['SEASON_ID_H']].iloc[0,][0]
            print('New Season', 'Season', seasonindex, 'Complete', '. Start', CurrSeason)
            seasonindex+=1
        for index in range(len(ThisDayGames)):
            game = ThisDayGames.iloc[index,:]
            gamedate = game[['GAME_DATE_H']][0]
            gamescore_diff = abs(int(game['PTS_H'])-int(game['PTS_A']))
            
            team1_abb = game[['MATCHUP_H']][0][0:3]
            team2_abb = game[['MATCHUP_H']][0][-3:]
            team1 = nba_team.loc[nba_team['abbreviation'] == team1_abb]['full_name'].values[0]
            team2 = nba_team.loc[nba_team['abbreviation'] == team2_abb]['full_name'].values[0]
            team1id = nba_team.loc[nba_team['abbreviation'] == team1_abb]['id'].values[0]
            team2id = nba_team.loc[nba_team['abbreviation'] == team2_abb]['id'].values[0]
            
            
            t1W = game['WL_H'] 
            
            t1_ELO = ELO_track[ELO_track['Team'] == team1_abb].iloc[-1,:][['ELO']]
            t2_ELO = ELO_track[ELO_track['Team'] == team2_abb].iloc[-1,:][['ELO']]
    
            
            ## ELO FORMULA
            if game['WL_H'] == 1:
                S_teamT1 = 1
                S_teamT2 = 0
            else:
                S_teamT1 = 0
                S_teamT2 = 1
            
            if S_teamT1 == 1:
                ELO_diff = (t1_ELO+100) - t2_ELO ## 100 extra points for home team
            else:
                ELO_diff = t2_ELO - (t1_ELO+100)
                
            E_teamT1 = 1/(1 + 10**((t2_ELO-(t1_ELO+100))/400)) ## 100 extra points for home team
            E_teamT2 = 1/(1 + 10**(((t1_ELO+100)-t2_ELO)/400))
    
            kt1 = 20*( (gamescore_diff+3)**0.8 / (7.5 + 0.006*ELO_diff))
            kt2 = 20*( (gamescore_diff+3)**0.8 / (7.5 + 0.006*ELO_diff))
            # Compute new ELOs
            t1_ELO_new = kt1*(S_teamT1 - E_teamT1) + t1_ELO
            t2_ELO_new = kt2*(S_teamT2 - E_teamT2) + t2_ELO
            
            ELOt1_append = {'Team': team1_abb,
             'ELO': t1_ELO_new,
             'Date': gamedate,
                           'Season': CurrSeason}
            ELOt2_append = {'Team': team2_abb,
             'ELO': t2_ELO_new,
             'Date': gamedate,
                           'Season': CurrSeason}
            addtoELO = pd.concat([pd.DataFrame(ELOt1_append), pd.DataFrame(ELOt2_append)], axis = 0)
            ELO_track = pd.concat([ELO_track, addtoELO]).reset_index(drop = 1)
        date += delta
    return(ELO_track)


ELO_track = get_ELO_track() 
ELO_track['Date'] = pd.to_datetime(ELO_track['Date'])

Track_LAL = ELO_track[(ELO_track['Team'] == 'LAL')]
Track_GSW = ELO_track[(ELO_track['Team'] == 'GSW')]
Track_CHI = ELO_track[(ELO_track['Team'] == 'CHI')]

# multiple line plots
plt.plot( 'Date', 'ELO', data= Track_LAL, color='skyblue', label = 'LAL')
plt.plot( 'Date', 'ELO', data= Track_GSW, color='black', label = 'GSW')
plt.plot( 'Date', 'ELO', data= Track_CHI, color='red', label = 'CHI')

plt.title('ELO Since 2000 for NBA Teams')
plt.legend()
# show graph
plt.show()

def get_pre_elo(team_abb, gamedate, NBAgamesC, ELO_track):
    import warnings
    warnings.simplefilter("ignore", UserWarning)
    
    team = NBAgamesC[NBAgamesC['MATCHUP_H'].str.contains(team_abb) | NBAgamesC['MATCHUP_A'].str.contains(team_abb)]
    teambeforedate = team[team['GAME_DATE_H'] < gamedate]
    priorgamedate = pd.DataFrame(teambeforedate['GAME_DATE_H']).iloc[0,:][0]
    pre_elo = ELO_track[(ELO_track['Date'] ==  priorgamedate) & (ELO_track['Team'] ==  team_abb)]['ELO']
    return(int(pre_elo))


