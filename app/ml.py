"""Machine learning functions"""

from fastapi import APIRouter, HTTPException
from sqlalchemy.sql import text
import psycopg2

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
else:
    # a connection error has occurred
    log.error("error attempting to connect to the database: {err_str}".format(err_str=db_conn_attempt["error"]))

@router.get('/db_test')
async def db_test():
    """
    db_test tests the db session object's connection 
    to the Postgres database
    """
    return db_sess.test_connection()

@router.get('/crime_scr/{city}')
async def get_crime_score(city: str):
    """
    get_crime_score returns a crime score (1-5) for the 
    passed city. 
      - 5: best crime score  (most reported crime)
      - 1: worst crime score (least reported crime)

    request:
      - GET `/crime_scr/<normalized city name>`

    examples:
      - GET `/crime_scr/st_louis`
      - GET `/crime_scr/new_york`
      - GET `/crime_scr/houston`

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
    sql = "SELECT combined_scaled_rate FROM cityspire_crime WHERE city_norm = %s"
    try:
        cursor      = db_conn.cursor()      # construct a database cursor
        cursor.execute(sql, (city,))        # execute the sql query
        city_scl    = cursor.fetchone()     # fetch the query results

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
