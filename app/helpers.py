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
def gen_rent_score(avg_rent):
    '''
    gets rent data from the database about a city and returns 
    a score from 1-5 based on the quantiles of all cities' rent data
    '''
    buckets = [ 742. , 1203.6, 1364. , 1535.6, 1721.2, 2993. ]
    if buckets[0] <= avg_rent <= buckets[1]:
        score = 5
    elif avg_rent <= buckets[2]:
        score = 4
    elif avg_rent <= buckets[3]:
        score = 3
    elif avg_rent <= buckets[4]:
        score = 2
    elif avg_rent <= buckets[5]:
        score = 1
    return score    

def gen_aq_score(combined_aq):
    '''
    gets air quality data from database for a city and generates a score
    from 1-5 based of the air quality data for all other cities
    '''
    buckets = [ 7.08867508, 10.12566503, 10.89508254, 11.95768491, 12.90177913,
                16.86715543]
    if buckets[0] <= combined_aq <= buckets[1]:
        score = 5
    elif combined_aq <= buckets[2]:
        score = 4
    elif combined_aq <= buckets[3]:
        score = 3
    elif combined_aq <= buckets[4]:
        score = 2
    elif combined_aq <= buckets[5]:
        score = 1
    return score