# gen_crime_score fetches a scaled city crime rate from the
#   database and translates that value to a 1-5 crime score
def gen_crime_score(db_conn, city):
    """
    gen_crime_score fetches a scaled city crime rate from the
    database and translates that value to a 1-5 crime score
    """
    ret_val = {"score": None, "error": "no score available"}

    # Query the database
    sql = "SELECT combined_scaled_rate FROM cityspire_crime WHERE city_code = %s"
    try:
      cursor      = db_conn.cursor()      # construct a database cursor
      cursor.execute(sql, (city,))        # execute the sql query
      city_scl    = cursor.fetchone()     # fetch the query results
      cursor.close()                      # close cursor

    except (Exception, psycopg2.Error) as error:
      ret_val["error"] = f"error fetching crime score data for city: {city} - {error}"
      return ret_val

    # Was the city found?
    if city_scl == None:
      # no results returned from the query - crime score not found
      ret_val["error"] = f"city: {city} not found"
      return ret_val

    # Highest scaled crime score -> worst crime rating
    if city_scl[0] >= 0.80:
      ret_val["score"] = 1
      return ret_val
    
    if city_scl[0] >= 0.60:
      ret_val["score"] = 2
      return ret_val

    if city_scl[0] >= 0.40:
      ret_val["score"] = 3
      return ret_val

    if city_scl[0] >= 0.20:
      ret_val["score"] = 4
      return ret_val

    # Lowest scaled crime score -> best crime rating
    ret_val["score"] = 5
    return ret_val

def gen_walk_score(db_conn, city):
    """
    gen_walk_score fetches a city walkability rating from the
    database and translates that value to a 1-5 walkability score
    """
    ret_val = {"score": None, "error": "no score available"}

    # Query the database
    sql = "SELECT walk_score FROM cityspire_wlk_scr WHERE city_code = %s"
    try:
      cursor      = db_conn.cursor()      # construct a database cursor
      cursor.execute(sql, (city,))        # execute the sql query
      wlk_scr_100 = cursor.fetchone()     # fetch the query results
      cursor.close()                      # close cursor

    except (Exception, psycopg2.Error) as error:
      ret_val["error"] = f"error the walkability score for city: {city} - {error}"
      return ret_val

    # Was the city found?
    if wlk_scr_100 == None:
      # no results returned from the query - raw walk score not found
      ret_val["error"] = f"walkability score for city: {city} not found"
      return ret_val

    # Highest walkability score -> best walking city
    wlk_scr_1 = wlk_scr_100[0]/100.0
    if wlk_scr_1 >= 0.80:
      ret_val["score"] = 5
      return ret_val
    
    if wlk_scr_1 >= 0.60:
      ret_val["score"] = 4
      return ret_val

    if wlk_scr_1 >= 0.40:
      ret_val["score"] = 3
      return ret_val

    if wlk_scr_1 >= 0.20:
      ret_val["score"] = 2
      return ret_val

    # Lowest walkability score -> worst walking city
    ret_val["score"] = 1
    return ret_val

# generates a rent_score based on quantiles of all rent rates
def gen_rent_score(db_conn, city):
    '''
    gets rent data from the database about a city and returns 
    a score from 1-5 based on the quantiles of all cities' rent data
    '''
    ret_val = {"score": None,
               "avg_rent": 0,
               "error": "no score available"}
    
    # query the database
    try:
        cursor = db_conn.cursor()
        sql = 'SELECT "Dec Avg Rent" FROM cityspire_rent WHERE "city_code"= %s;'
        cursor.execute(sql, (city,))
        avg_rent = cursor.fetchone()[0]
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        ret_val['Error'] = f"error fetching rent data for city: {city} - {error}"
        return ret_val

    # return error if there was no data found
    if avg_rent == None:
        ret_val['Error'] = f'{city} average rent not found'
        return ret_val
    ret_val['avg_rent'] = avg_rent    
    
    # generate the score
    buckets = [ 742. , 1203.6, 1364. , 1535.6, 1721.2, 2993. ]
    if buckets[0] <= avg_rent <= buckets[1]:
        # best score/lowest rent
        ret_val["score"] = 5
    elif avg_rent <= buckets[2]:
        ret_val["score"] = 4
    elif avg_rent <= buckets[3]:
        ret_val["score"] = 3
    elif avg_rent <= buckets[4]:
        ret_val["score"] = 2
    elif avg_rent <= buckets[5]:
        # worst score/highest rent
        ret_val["score"] = 1
    return ret_val    

def gen_aq_score(db_conn, city):
    '''
    gets air quality data from database for a city and generates a score
    from 1-5 based of the air quality data for all other cities
    '''
    ret_val = {"score": None, "error": "no score available"}
    
    # Query the database for the passed city code
    sql = 'SELECT "Combined Total" FROM cityspire_air_quality WHERE "city_code" =%s'
    try:
      cursor      = db_conn.cursor()      # construct a database cursor
      cursor.execute(sql, (city,))        # execute the sql query
      combined_aq    = cursor.fetchone()[0]     # fetch the query results
      cursor.close()

    except (Exception, psycopg2.Error) as error:
      ret_val["error"] = f"error fetching the air quality score for city: {city} - {error}"
      raise HTTPException(status_code=500, detail=ret_val)

    # Was the city found?
    if combined_aq == 0:
      # no results returned from the query - quality of life crime score not found
      ret_val["error"] = f"air quality score for city: {city} not found"
      raise HTTPException(status_code=404, detail=ret_val)    
    
    buckets = [ 7.08867508, 10.12566503, 10.89508254, 11.95768491, 12.90177913,
                16.86715543]
    if buckets[0] <= combined_aq <= buckets[1]:
        ret_val["score"] = 5
    elif combined_aq <= buckets[2]:
        ret_val["score"] = 4
    elif combined_aq <= buckets[3]:
        ret_val["score"] = 3
    elif combined_aq <= buckets[4]:
        ret_val["score"] = 2
    elif combined_aq <= buckets[5]:
        ret_val["score"] = 1
    return ret_val

def calc_wghtd_city_score(scores: dict, weights:dict):
  """
  calc_wghtd_city_score calculates a weighted average of 
  crime, walkability, air quality, and rent scores given
  the user's preferred ranking or weighting (1-10) for 
  each livability dimension
  """
  # calculate a weighted average score
  numerator = float(scores["crime"])*float(weights["crime"]) + \
                float(scores["walk"])*float(weights["walk"]) + \
                float(scores["air"])*float(weights["air"])   + \
                float(scores["rent"])*float(weights["rent"])

  denominator = float(weights["crime"]) + \
                float(weights["walk"])  + \
                float(weights["air"])   + \
                float(weights["rent"])

  wgt_avg = numerator / denominator
  print(f"calculated weighted average is {wgt_avg}")

  # check for extreme values
  if wgt_avg < 1.0:
    wgt_avg = 1.0

  if wgt_avg > 5.0:
    wgt_avg = 5.0

  wgt_avg = round(wgt_avg, 1)
  print(f"returning: {wgt_avg}")
  return wgt_avg
  