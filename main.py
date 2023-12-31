from typing import Optional
import pandas as pd
import numpy as np
from fastapi import FastAPI
from starlette.responses import FileResponse 

app = FastAPI()


@app.get("/")
async def root():
    return FileResponse('APIData/index.html')

@app.get("/userdata/")
async def userdata(user_id: str):

    ## Step 1. Import General User Items dataframe and filter using user id input
    path = 'APIData/'
    fn_userItems = path + 'df_userItems.csv'
    df_userItems = pd.read_csv(fn_userItems)
    df_userItems_i = df_userItems[df_userItems['user_id']==user_id]

    # Check if user id is valid
    if len(df_userItems_i) == 0:
        cantidad = 0
        cantidadItems = 0
    else:
        ## Step 2. Save number of items
        cantidadItems = df_userItems_i['items_count'].values[0]

        # Check if the user has items
        if cantidadItems == 0:
            cantidad = 0
        else:
            ## Step 3. Import Specific User Items dataframe
            steam_id = df_userItems_i['steam_id'].values[0]
            pathItems = path + 'ItemsData/'
            fni = pathItems + 'itemsData_' + str(steam_id) + '.csv'
            dfi = pd.read_csv(fni)

            ## Step 4. Import Steam Games Dataframe
            fn_steamGames = path + 'df_steamGames.csv'
            df_steamGames = pd.read_csv(fn_steamGames)

            ## Step 5. Calculate money spent iterating over item ids and looking for them in Steam Games Dataframe
            I_IDS = dfi['item_id'].values
            cantidad = 0
            for i in I_IDS:
                try:
                    p = df_steamGames[df_steamGames['id']==i]['price'].values[0]
                except:
                    p = 0 # If game is not found, assign price of 0
                cantidad += p
        
    ## Step 6. Import General User Reviews Data
    fnReviews = path + 'df_reviews.csv'
    dfReviews = pd.read_csv(fnReviews)
    dfReviews_i = dfReviews[dfReviews['user_id']==user_id]

    # Check if user id is valid
    if len(dfReviews_i) == 0:
        r = 0
    else:
        steam_id = dfReviews_i['steam_id'].values[0]
        ## Step 7. Import Specific User Reviews Data
        fnReviews_i = path + 'ReviewsData/revData_' + str(steam_id) + '.csv'
        try:
            dfReviews_i = pd.read_csv(fnReviews_i)

            ## Step 8. Calculate recommendation percentage
            n = len(dfReviews_i)
            s = sum(dfReviews_i['recommend'])
            r = n/s*100
        except:
            r=0
    
    return {"cantidad":cantidad.item(),"items":cantidadItems.item(),"recper":r}

@app.get("/countreviews/")
async def countreviews(date1: str, date2: str):
    date1 = pd.to_datetime(date1)
    date2 = pd.to_datetime(date2)

    fn_reviews_r = 'APIData/df_reviews_r.csv'
    df_reviews_r = pd.read_csv(fn_reviews_r)

    df_reviews_r_betweenDates = df_reviews_r[pd.to_datetime(df_reviews_r['posted']).between(date1,date2)]
    n = len(df_reviews_r_betweenDates)

    users = len(df_reviews_r_betweenDates.groupby('steam_id').count())

    if n > 0:
        r = sum(df_reviews_r_betweenDates['recommend'])
        p = r/n*100
    else:
        p=0

    return {"UserCount":users,"RecPercentage":p} 

@app.get("/genre/")
async def genre(genre: str):
    path = 'APIData/'
    fn_genresRank = path + 'genresRank.csv'
    df_genresRank = pd.read_csv(fn_genresRank)

    genreRow = df_genresRank[df_genresRank['genre'] == genre]
    if len(genreRow) == 0:
        return('Not a genre (case sensitive)')
    else:
        rank = genreRow.index[0] + 1
        return {"rank": rank.item()}

@app.get("/userforgenre/")
async def userforgenre(genre: str):
    path = 'APIData/GenresData/'
    fn_genreRank = path + 'genreData_' + genre + '.csv'
    try:
        df_genreRank = pd.read_csv(fn_genreRank)
        top5 = df_genreRank.loc[0:4,'steam_id'].values
        fn_userItems = 'APIData/df_userItems.csv'
        df_userItems = pd.read_csv(fn_userItems)

        df_top5 = df_userItems[df_userItems['steam_id'].isin(top5)].drop(columns=['items_count','steam_id'])
        
        if len(df_genreRank) == 0:
            return('No Playing time')
        else:
            return {"top5": df_top5}
    except:
        return('Not a genre (case sensitive)')

@app.get("/developer/")
async def developer(dev:str):
    fn_steamGames = 'APIData/df_steamGames.csv'
    df_steamGames = pd.read_csv(fn_steamGames)
    df_steamGames_dev = df_steamGames[df_steamGames['developer'] == dev]
    n = len(df_steamGames_dev)
    if n == 0:
        return('No games found of this developer')
    else:
        df_steamGames_dev.loc[:,'release_date'] = pd.to_datetime(df_steamGames_dev['release_date']).dt.year
        year = df_steamGames_dev.groupby('release_date').count().index.values
        count  = df_steamGames_dev.groupby('release_date').count()['price'].values
        df_steamGames_dev.loc[:,'price'] = (df_steamGames_dev['price'] == 0)
        free = df_steamGames_dev.groupby('release_date').sum()['price'].values
        per = (free/count*100).astype(str)
        df_ret = pd.DataFrame({'year':year.tolist(),'count':count.tolist(),'free_content':per})
        df_ret['free_content']= df_ret['free_content'].str.slice(0,4)
        df_ret['free_content'] = (df_ret['free_content'].values + '%').tolist()
        return {dev:df_ret.to_dict()}

@app.get('/sentiment_analysis/')
async def sentiment_analysis(year:int):
    fn_steamGames = 'APIData/df_steamGames.csv'
    df_steamGames = pd.read_csv(fn_steamGames)
    df_steamGames['release_date'] = pd.to_datetime(df_steamGames['release_date']).dt.year
    ids = df_steamGames[df_steamGames['release_date']==year]['id'].values

    fn_reviews_r = 'APIData/df_reviews_r.csv'
    df_reviews_r = pd.read_csv(fn_reviews_r)
    df_reviews_r_ids = df_reviews_r[df_reviews_r['item_id'].isin(ids)]
    sa_count = df_reviews_r_ids.groupby('sentiment_analysis').count()['item_id'].values
    sa_index = df_reviews_r_ids.groupby('sentiment_analysis').count().index.values

    try: 
        sa_index = np.char.replace(np.char.replace(np.char.replace(sa_index.astype(str),'0','Negative'),'1','Neutral'),'2','Positive')
    except:
        pass
    rdic = {'Negative':0,'Neutral':0,'Positive':0}
    for i in range(len(sa_index)):
        rdic[sa_index[i]] = sa_count[i].item()
    
    return rdic

@app.get('/recomendacion_juego/')
async def recomendacion_juego(item_id:int):
    fn_steamGames = 'APIData/df_steamGames.csv'
    df_steamGames = pd.read_csv(fn_steamGames)

    df_gamesNameId = df_steamGames[['id','app_name']]
    i = df_gamesNameId[df_gamesNameId['id'] == float(item_id)].index[0]
    name = df_gamesNameId[df_gamesNameId['id'] == float(item_id)]['app_name'].values[0]

    fn_top5 = 'APIData/MLData/df_top5.csv'
    df_top5 = pd.read_csv(fn_top5)
    df_top5_i = df_top5.loc[:,str(i)].values

    rec_names = df_gamesNameId.loc[df_top5_i,'app_name'].values

    return {"Based on game:":name,"We recommend:":list(rec_names)}
