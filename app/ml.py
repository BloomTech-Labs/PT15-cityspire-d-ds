"""Machine learning functions"""

from fastapi import APIRouter, HTTPException
from sqlalchemy.sql import text
import psycopg2
from psycopg2.extras import RealDictCursor

from app.db import get_db
from app.dbsession import DBSession
from app.helpers import gen_crime_score

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
    """
    # Do we have a list of supported cities?
    if len(store_cities) == 0:
      # list of supported cities is 0 - an error has occurred
      raise HTTPException(status_code=500, detail=f"no supported cities found")
      return []

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
    # Validate the city parameter
    if len(city) == 0:
        # error: missing city parameter
        raise HTTPException(status_code=400, detail="missing city parameter")

    # Define a response object
    ret_dict            = {}
    ret_dict["ok"]      = False
    ret_dict["msg"]     = ""
    ret_dict["error"]   = None
    ret_dict["score"]   = -1

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
        raise HTTPException(status_code=404, detail=f"city: {city} not found")
    
    # Return results
    ret_dict["ok"]      = True
    ret_dict["error"]   = None
    ret_dict["msg"]     = f"{city} crime score"
    ret_dict["score"]   = gen_crime_score(city_scl[0])
    return ret_dict

@router.get('/rent_rate/{city}')
async def get_rent_rate(city: str):
  '''
  Takes in a city as its parameter and returns a dictionary:
  e.g:
  {
    msg: 'avg city rent',
    avg_rent: 2100
  }
  '''
  if len(city) == 0:
      # raise error if city is missing
      raise HTTPException(status_code=400, detail="missing city parameter")

  # set up the return dictionary
  ret_dict = {}
  ret_dict['msg'] = f'{city} Average Rent'
  ret_dict['avg_rent'] = None

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