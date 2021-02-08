"""Machine learning functions"""

from fastapi import APIRouter, HTTPException
from sqlalchemy.sql import text
import psycopg2
from psycopg2.extras import RealDictCursor
from random import randint

from app.db import get_db
from app.dbsession import DBSession
from app.helpers import gen_crime_score, get_rent_score

router = APIRouter()

# Create a database session object
db_sess = DBSession()
# Connect to the database
db_conn_attempt = db_sess.connect()

# Determine if any connection errors occurred
if db_conn_attempt["error"] == None:
    # no errors connecting, assign the connection object for use
    db_conn = db_conn_attempt["value"]

    # fetch set of officially supported cities and save in memory
    sql = "SELECT id, city, state, city_code FROM cityspire_cities WHERE active = 'yes'"
    try:
      dict_cur      = db_conn.cursor(cursor_factory=RealDictCursor)
      dict_cur.execute(sql)                 # execute the sql query
      store_cities  = dict_cur.fetchall()   # fetch the query results
      dict_cur.close()

    except (Exception, psycopg2.Error) as error:
      store_cities = []
      print("ERROR: error fetching array of supported cities; see: {err_str}".format(err_str=db_conn_attempt["error"]))

else:
    # a connection error has occurred
    print("ERROR: error attempting to connect to the database: {err_str}".format(err_str=db_conn_attempt["error"]))

@router.get('/db_test')
async def db_test():
    """
    db_test tests the db session object's connection 
    to the Postgres database
    """
    return store_cities

@router.get('/cities')
async def cities():
    """
    cities returns a json array of supported cities (active = 'yes')

    Example Response:
    ```
    [
      {
        "id": 1,
        "city": "New York City",
        "state": "NY",
        "city_code": "New_York_City"
      },
      {
        "id": 2,
        "city": "Los Angeles",
        "state": "CA",
        "city_code": "Los_Angeles"
      },
      {
        "id": 3,
        "city": "Chicago",
        "state": "IL",
        "city_code": "Chicago"
      }
    ]
    ```
    """
    # Do we have a list of supported cities?
    if len(store_cities) == 0:
      # list of supported cities is 0 - an error has occurred
      ret_dict = {"msg": "no supported cities found"}
      raise HTTPException(status_code=500, detail=ret_dict)

    return store_cities

@router.get('/crime_scr/{city}')
async def get_crime_score(city: str):
    """
    get_crime_score returns a crime score (1-5) for the 
    passed city. 
      - 5: best crime score  (most reported crime)
      - 1: worst crime score (least reported crime)

    request:
      - GET `/crime_scr/<normalized city code>`

    examples:
      - GET `/crime_scr/St_Louis`
      - GET `/crime_scr/New_York`
      - GET `/crime_scr/Houston`

    return values:
      - "ok":    `True` (no errors found); `False` (errors found)
      - "error": error message
      - "score": `5` (best) to `1` (worst) score
    """
    # Define a response object
    ret_dict            = {}
    ret_dict["ok"]      = False
    ret_dict["msg"]     = ""
    ret_dict["error"]   = None
    ret_dict["score"]   = None

    # Validate the city parameter
    if len(city) == 0:
      # error: missing city parameter
      ret_dict["error"] = "missing city parameter"
      raise HTTPException(status_code=400, detail=ret_dict)

    # Query the database
    sql = "SELECT combined_scaled_rate FROM cityspire_crime WHERE city_code = %s"
    try:
      cursor      = db_conn.cursor()      # construct a database cursor
      cursor.execute(sql, (city,))        # execute the sql query
      city_scl    = cursor.fetchone()     # fetch the query results
      cursor.close()                      # close cursor

    except (Exception, psycopg2.Error) as error:
      ret_dict["error"] = f"error fetching crime score data for city: {city} - {error}"
      return ret_dict

    # Was the city found?
    if city_scl == None:
      # no results returned from the query - crime score not found
      ret_dict["error"] = f"city: {city} not found"
      raise HTTPException(status_code=404, detail=ret_dict)
    
    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} crime score"
    ret_dict["score"]   = gen_crime_score(city_scl[0])
    return ret_dict

@router.get('/rent_rate/{city}')
async def get_rent_rate(city: str):
  '''
  Takes in a city and return avg_rent and score (1-5):
  avg_rent: the raw average rent for that city
  score: 1 = expensive city rent
         5 = cheap city rent

  request:
    - GET `/rent_rate/<normalized city code>`

  examples:
    - GET `/rent_rate/St_Louis`
    - GET `/rent_rate/New_York_City`
    - GET `/rent_rate/Houston`  
  returns
  {
    - msg: '{City} Average Rent',
    - avg_rent: the raw average rent
    - score: 5(best/cheapest)-1(worst/most expensive)
  }
  '''
  if len(city) == 0:
      # raise error if city is missing
      raise HTTPException(status_code=400, detail="missing city parameter")

  # set up the return dictionary
  ret_dict = {}
  ret_dict['msg'] = f'{city} Average Rent'
  ret_dict['avg_rent'] = None
  ret_dict['score'] = 0

  # query the database
  try:
    cursor = db_conn.cursor()
    sql = 'SELECT "Dec Avg Rent" FROM cityspire_rent WHERE "city_code"= %s;'
    cursor.execute(sql, (city,))
    avg_rent = cursor.fetchone()
  except (Exception, psycopg2.Error) as error:
    ret_dict['Error'] = f"error fetching rent data for city: {city} - {error}"
    return ret_dict

  # return error if there was no data found
  if avg_rent == None:
    ret_dict['Error'] = f'{city} average rent not found'
    return ret_dict
  else:
    ret_dict['avg_rent'] = avg_rent[0]
    ret_dict['score'] = get_rent_score(avg_rent[0])
  
  # not sure if it's neccessary to close the cursor?
  cursor.close()
  return ret_dict

@router.get('/population_data/{city}')
async def get_population_data(city: str):
  '''
  Returns population data for the city passed in
  e.g.
  {
    msg: 'City Population
    population: 123456
  }
  '''
  if len(city) == 0:
    # raise error if city is missing
    raise HTTPException(status_code=400, detail="missing city parameter")

  # set up the return dictionary
  ret_dict = {}
  ret_dict['msg'] = f'{city} Population'
  ret_dict['population'] = None

  # query the database
  try:
    cursor = db_conn.cursor()
    sql = 'SELECT "population" FROM cityspire_cities WHERE "city_code" = %s;'
    cursor.execute(sql, (city,))
    population = cursor.fetchone()
  except (Exception, psycopg2.Error) as error:
    ret_dict['Error'] = f"error fetching population data for city: {city} - {error}"
    return ret_dict

  # return error if there was no data found
  if population == None:
    ret_dict['Error'] = f'{city} population data not found'
    return ret_dict
  else:
    ret_dict['population'] = population[0]
  
  cursor.close()
  return ret_dict

@router.get('/walk_scr/{city}')
async def get_walk_scr(city: str):
    """
    get_walk_scr returns a walkability score (1-5) for the 
    passed city. 
      - 5: best walkability score  (most "walkable")
      - 1: worst walkability score (least "walkable")

    request:
      - GET `/walk_scr/<normalized city name>`

    examples:
      - GET `/walk_scr/St_Louis`
      - GET `/walk_scr/New_York_City`
      - GET `/walk_scr/Houston`

    return values:
      - "ok":    `True` (no errors found); `False` (errors found)
      - "error": error message
      - "score": `5` (best) to `1` (worst) score
    """
    # Define a response object
    ret_dict            = {}
    ret_dict["ok"]      = False
    ret_dict["msg"]     = ""
    ret_dict["error"]   = None
    ret_dict["score"]   = None

    # Validate the city parameter
    if len(city) == 0:
      # error: missing city parameter
      ret_dict["msg"] = "missing city parameter"
      raise HTTPException(status_code=400, detail=ret_dict)

    # Query the database
    sql = "SELECT walk_score FROM cityspire_wlk_scr WHERE city_code = %s"
    try:
      cursor      = db_conn.cursor()      # construct a database cursor
      cursor.execute(sql, (city,))        # execute the sql query
      wlk_scr_100 = cursor.fetchone()     # fetch the query results
      cursor.close()                      # close cursor

    except (Exception, psycopg2.Error) as error:
      ret_dict["error"] = f"error the walkability scorefor city: {city} - {error}"
      return ret_dict

    # Was the city found?
    if wlk_scr_100 == None:
      # no results returned from the query - raw walk score not found
      ret_dict["error"] = f"walkability score for city: {city} not found"
      raise HTTPException(status_code=404, detail=ret_dict)
    
    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} walkability score"
    ret_dict["score"]   = gen_crime_score(float(wlk_scr_100[0])/100.0)
    return ret_dict

@router.get('/city_scr/{city}')
async def get_city_scr(city: str):
    """
    NOTE: CURRENTLY ROUTE RETURNS MOCK DATA

    city_scr returns an overall city quality of life score (1-5)
    for the passed city. 
      - 5: best quality of life score
      - 1: worst quality of life score

    request:
      - GET `/city_scr/<normalized city name>`

    examples:
      - GET `/city_scr/St_Louis`
      - GET `/city_scr/New_York_City`
      - GET `/city_scr/Houston`

    return values:
      - "ok":    `True` (no errors found); `False` (errors found)
      - "error": error message
      - "score": `5` (best) to `1` (worst) score
    """
    # Define a response object
    ret_dict            = {}
    ret_dict["ok"]      = False
    ret_dict["msg"]     = ""
    ret_dict["error"]   = None
    ret_dict["score"]   = None

    # Validate the city parameter
    if len(city) == 0:
      # error: missing city parameter
      ret_dict["error"] = "missing city parameter"
      raise HTTPException(status_code=400, detail=ret_dict)

    # Query the database for the passed city code
    sql = "SELECT COUNT(*) FROM cityspire_cities WHERE city_code = %s"
    try:
        cursor      = db_conn.cursor()      # construct a database cursor
        cursor.execute(sql, (city,))        # execute the sql query
        city_val    = cursor.fetchone()     # fetch the query results
        cursor.close()

    except (Exception, psycopg2.Error) as error:
        ret_dict["error"] = f"error fetching the overall quality of life score for city: {city} - {error}"
        raise HTTPException(status_code=500, detail=ret_dict)

    # Was the city found?
    if city_val[0] == 0:
        # no results returned from the query - quality of life crime score not found
        ret_dict["error"] = f"quality of life score for city: {city} not found"
        raise HTTPException(status_code=404, detail=ret_dict)
    
    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} quality of life score"
    ret_dict["score"]   = randint(1, 5)
    return ret_dict

@router.get('/air_qual_scr/{city}')
async def get_air_qual_scr(city: str):
    """
    NOTE: CURRENTLY ROUTE RETURNS MOCK DATA

    city_scr returns the air quality score (1-5)
    for the passed city. 
      - 5: best air quality score
      - 1: worst air quality score

    request:
      - GET `/air_qual_scr/<normalized city name>`

    examples:
      - GET `/air_qual_scr/St_Louis`
      - GET `/air_qual_scr/New_York_City`
      - GET `/air_qual_scr/Houston`

    return values:
      - "ok":    `True` (no errors found); `False` (errors found)
      - "error": error message
      - "score": `5` (best) to `1` (worst) score
    """
    # Define a response object
    ret_dict            = {}
    ret_dict["ok"]      = False
    ret_dict["msg"]     = ""
    ret_dict["error"]   = None
    ret_dict["score"]   = None

    # Validate the city parameter
    if len(city) == 0:
      # error: missing city parameter
      ret_dict["error"] = "missing city parameter"
      raise HTTPException(status_code=400, detail=ret_dict)

    # Query the database for the passed city code
    sql = "SELECT COUNT(*) FROM cityspire_cities WHERE city_code = %s"
    try:
      cursor      = db_conn.cursor()      # construct a database cursor
      cursor.execute(sql, (city,))        # execute the sql query
      city_val    = cursor.fetchone()     # fetch the query results
      cursor.close()

    except (Exception, psycopg2.Error) as error:
      ret_dict["error"] = f"error fetching the air quality score for city: {city} - {error}"
      raise HTTPException(status_code=500, detail=ret_dict)

    # Was the city found?
    if city_val[0] == 0:
      # no results returned from the query - quality of life crime score not found
      ret_dict["error"] = f"air quality score for city: {city} not found"
      raise HTTPException(status_code=404, detail=ret_dict)
    
    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} air quality score score"
    ret_dict["score"]   = randint(1, 5)
    return ret_dict
