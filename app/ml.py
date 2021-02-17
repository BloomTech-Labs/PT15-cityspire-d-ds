"""Machine learning functions"""

from fastapi import APIRouter, HTTPException
from sqlalchemy.sql import text
import psycopg2
from psycopg2.extras import RealDictCursor
from random import randint

from app.db import get_db
from app.dbsession import DBSession
from app.helpers import gen_crime_score, gen_rent_score, gen_aq_score, gen_walk_score
from app.helpers import calc_wghtd_city_score

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
    to the Postgres database and returns connection 
    information as a json document
    """
    return db_sess.test_connection()

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
      - GET `/crime_scr/New_York_City`
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

    # Generate the crime score
    crime_score = gen_crime_score(db_conn, city)

    # Any errors generating a crime score?
    if crime_score["score"] == None:
      ret_dict["error"] = crime_score["error"]
      raise HTTPException(status_code=400, detail=ret_dict)

    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} crime score"
    ret_dict["score"]   = crime_score["score"]
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
  # set up the return dictionary
  ret_dict = {}
  ret_dict['msg'] = f'{city} Average Rent'
  ret_dict['avg_rent'] = None
  ret_dict['score'] = 0

  if len(city) == 0:
      # raise error if city is missing
      raise HTTPException(status_code=400, detail="missing city parameter")
  
  # Generate the rent score
  rent_score = gen_rent_score(db_conn, city)

  # Any errors generating a score?
  if rent_score["score"] == None:
    ret_dict["error"] = rent_score["error"]
    raise HTTPException(status_code=400, detail=ret_dict)

  ret_dict['avg_rent'] = rent_score['avg_rent']
  ret_dict['score'] = rent_score['score']
  
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

    # Generate the walkablity score
    walk_score = gen_walk_score(db_conn, city)

    # Any errors generating a walk score?
    if walk_score["score"] == None:
      ret_dict["error"] = walk_score["error"]
      raise HTTPException(status_code=400, detail=ret_dict)
    
    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} walkability score"
    ret_dict["score"]   = walk_score["score"]

    return ret_dict

@router.get('/city_scr/{city}')
async def get_city_scr(city: str, crime: int=5, walk: int=5, air: int=5, rent: int=5):
    """
    city_scr returns an overall city quality of life score (1.0-5.0)
    for the passed city. 
      - 5.0: best quality of life score
      - 1.0: worst quality of life score

    the city_scr is a weighted average of multiple livablity scores including
    - crime
    - rent
    - walkability
    - air quality

    request:
      - GET `/city_scr/<normalized city name>`
      - Querystring parameters
        -  crime: integer 0-10 (default value = 5)
        -  walk: integer 0-10 (default value = 5)
        -  air: integer 0-10 (default value = 5)
        -  rent: integer 0-10 (default value = 5)

    examples:
      - GET `/city_scr/St_Louis?crime=8&walk=4&air=4&rent=9`
      - GET `/city_scr/New_York_City?crime=7&walk=10&air=4&rent=8`
      - GET `/city_scr/Houston?crime=4&walk=2&air=5&rent=9`

    return values:
      - "ok":    `True` (no errors found); `False` (errors found)
      - "error": error message
      - "score": `5.0` (best) to `1.0` (worst) score
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
    
    # Calculate the individual component city scores
    intrm_crime = gen_crime_score(db_conn, city)
    if intrm_crime["score"] == None:
      # error calculating the crime score
      ret_dict["error"] = intrm_crime["error"]
      raise HTTPException(status_code=500, detail=ret_dict)

    intrm_walk = gen_walk_score(db_conn, city)
    if intrm_walk["score"] == None:
      # error calculating the walkability score
      ret_dict["error"] = intrm_walk["error"]
      raise HTTPException(status_code=500, detail=ret_dict)

    intrm_air = gen_aq_score(db_conn, city)
    if intrm_air["score"] == None:
      # error calculating the air quality score
      ret_dict["error"] = intrm_air["error"]
      raise HTTPException(status_code=500, detail=ret_dict)

    intrm_rent = gen_rent_score(db_conn, city)
    if intrm_rent["score"] == None:
      # error calculating the rent score
      ret_dict["error"] = intrm_rent["error"]
      raise HTTPException(status_code=500, detail=ret_dict)

    # Construct a city score dict/map
    score_dict = {
      "crime": intrm_crime["score"],
      "walk": intrm_walk["score"],
      "air": intrm_air["score"],
      "rent": intrm_rent["score"]
    }
    # Construct a user weighting dict/map
    usr_weight_dict = {
      "crime": crime,
      "walk": walk,
      "air": air,
      "rent": rent
    }

    # Calculate the user's weighted average of the underlying city scores
    wght_score = calc_wghtd_city_score(score_dict, usr_weight_dict)

    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} quality of life score"
    ret_dict["score"]   = wght_score
    return ret_dict

@router.get('/air_qual_scr/{city}')
async def get_air_qual_scr(city: str):
    """

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
      ret_dict['error']: 'missing city parameter'

    # Generate the rent score
    aq_score = gen_aq_score(db_conn, city)

    # Any errors generating a score?
    if aq_score["score"] == None:
      ret_dict["error"] = aq_score["error"]
      raise HTTPException(status_code=400, detail=ret_dict)    
    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} air quality score"
    ret_dict["score"]   = aq_score['score']
    return ret_dict
