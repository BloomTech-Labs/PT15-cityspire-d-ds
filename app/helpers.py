from app.config import city_score_weights

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

def gen_city_weight(crime=0, walk=0, air=0, rent=0):
  """
  gen_city_weight returns a dict of float city score weights
  that sum to 1.0 (given passed integer weight values: 0-10)

  example return value:
    {"crime": 0.4, "walk": 0.3, "air": 0.1, "rent": 0.2}
  """
  # Validate inbound parameters (do parameter values sum to 10)
  wght_sum = crime + walk + air + rent
  if wght_sum != 10:
    # weight values do not sum to 10, use default weights
    return city_score_weights.weights_city

  # Convert weights from integers to float values: 0.0 <= wgt <= 1.0
  ret_dict = {}
  ret_dict["crime"] = round(float(crime/10), 4)
  ret_dict["walk"]  = round(float(walk/10), 4)
  ret_dict["air"]   = round(float(air/10), 4)
  ret_dict["rent"]  = 1.0 - (ret_dict["crime"] - ret_dict["walk"] - ret_dict["air"])

  return ret_dict

def calc_wghtd_city_score(crime: int, walk:int, air:int, rent: int, weights: dict):
  """
  calc_wghtd_city_score calculates a weighted average of 
  crime, walkability, air quality, and rent scores give a set
  of weights
  """
  # calculate a weighted score
  tmp_scr = float(crime)*weights["crime"] + \
            float(walk)*weights["walk"]   + \
            float(air)*weights["air"]     + \
            float(rent)*weights["rent"]

  # check for extreme values
  if tmp_scr < 0.0:
    return 0.0

  if tmp_scr > 5.0:
    return 5.0

  return round(tmp_scr, 1)
  