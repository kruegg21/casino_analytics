import helper
import json
import psycopg2
from sqlalchemy import create_engine
from vizandmapping import get_data_from_nl_query
from generateresponsefromrequest import get_intent_entity_from_watson
from rulebasedquery import translation_dictionary

# Local database
# DATABASE_USER = 'test'
# DATABASE_NAME = 'playlogs'
# DATABASE_DOMAIN = 'tiger@localhost'
# DATABASE_TABLE = 'logs'

# Read password from external file
with open('passwords.json') as data_file:
    data = json.load(data_file)

DATABASE_HOST = 'soft-feijoa.db.elephantsql.com'
DATABASE_PORT = '5432'
DATABASE_NAME = 'ohdimqey'
DATABASE_USER = 'ohdimqey'
DATABASE_PASSWORD = data['DATABASE_PASSWORD']

# Connect to database
database_string = 'postgres://{}:{}@{}:{}/{}'.format(DATABASE_USER,
                                                     DATABASE_PASSWORD,
                                                     DATABASE_HOST,
                                                     DATABASE_PORT,
                                                     DATABASE_NAME)
engine = create_engine(database_string)

def get_database_schema(cur, table):
    '''
    Used to get column names for a table
    Input:
        cur -- psycopg2 cursor
    Output:
        list of strings of column names of table
    '''
    cur.execute("""SELECT column_name FROM information_schema.columns
                    WHERE table_name = '{}';""".format(DATABASE_TABLE))
    rows = cur.fetchall()
    return [element[0] for element in rows]

@helper.timeit
def make_materialized_view(engine, time_period, factors, table_name):
    '''
    Generalized method to create materialized view tables for faster SQL lookup
    Input:
        engine -- sqlalchemy cursor
        factors (list) -- list of strings of factors we want to group by in our
                   materialized view
        time_period (string) -- string of time period we want to aggregate on
        table_name (string) -- string of table we are building views from
    Output:
        None
    '''
    # Connect to engine
    connection = engine.connect()

    # Alphabetize factors
    factors.sort()

    factor_string = ''
    title_string = time_period + '_factored_by'
    for factor in factors:
        factor_string += ', '
        factor_string += factor
        title_string += '_'
        title_string += factor

    # Drop table if exists
    SQL_string = """DROP MATERIALIZEZD VIEW IF EXISTS {};""".format(title_string)
    connection.execute(SQL_string)

    # Build materialized view
    print "Creating materialized view: {}".format(title_string)
    SQL_string =  """CREATE MATERIALIZED VIEW IF NOT EXISTS {} AS
                      SELECT SUM(amountbet - amountwon) AS revenue,
                             COUNT(*) AS popularity, date_trunc('{}', {}.tmstmp) AS {}{}
                      FROM {}
                      GROUP BY date_trunc('{}', {}.tmstmp){};""".format(title_string,
                                                                        time_period,
                                                                        table_name,
                                                                        time_period,
                                                                        factor_string,
                                                                        table_name,
                                                                        time_period,
                                                                        table_name,
                                                                        factor_string)
    print "SQL command to build materialized view: {}".format(SQL_string)
    result = connection.execute(SQL_string)


if __name__ == "__main__":
    make_materialized_view(engine, 'day', ['bank', 'clublevel', 'zone', 'area'], 'full_logs')
    make_materialized_view(engine, 'minute', ['bank', 'clublevel', 'zone', 'area'], 'full_logs')

    make_materialized_view(engine, 'hour', ['bank', 'clublevel', 'zone', 'area'], 'logs')
