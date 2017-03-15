import helper
import psycopg2
from vizandmapping import get_data_from_nl_query
from generateresponsefromrequest import get_intent_entity_from_watson

# Local database
DATABASE_USER = 'test'
DATABASE_NAME = 'playlogs'
DATABASE_DOMAIN = 'tiger@localhost'
DATABASE_TABLE = 'logs'

def get_database_schema(cur):
    '''
    Input:
        cur -- psycopg2 cursor
    Output:
        list of strings of column names of table
    '''
    cur.execute("""SELECT column_name FROM information_schema.columns
                    WHERE table_name = '{}';""".format(DATABASE_TABLE))
    rows = cur.fetchall()
    return [element[0] for element in rows]

def make_materialized_view(cur):
    cur.execute("""CREATE MATERIALIZED VIEW revenue_hourly2 AS SELECT SUM(amountbet - amountwon) AS revenue, date_trunc('hour', logs.tmstmp), manufacturer, zone, area, bank, stand, clublevel AS hour FROM logs GROUP BY 2, 3, 4, 5 ,6 ,7, 8;""")
    # cur.execute("""CREATE MATERIALIZED VIEW revenue_hourly AS
    #                SELECT SUM(amountbet - amountwon) AS revenue, date_trunc('hour', logs.tmstmp) AS hour FROM logs GROUP BY 2;""")

if __name__ == "__main__":
    # Connect to database
    try:
        conn=psycopg2.connect("dbname='{}' user='{}'".format(DATABASE_NAME, DATABASE_USER))
    except:
        print "I am unable to connect to the database."
    cur = conn.cursor()

    # Local database
    DATABASE_USER = 'test'
    DATABASE_NAME = 'playlogs'
    DATABASE_DOMAIN = 'tiger@localhost'
    DATABASE_TABLE = 'logs'

    engine = helper.connect_to_database(DATABASE_USER,
                                        DATABASE_DOMAIN,
                                        DATABASE_NAME)

    # get_database_schema(cur)
    # make_materialized_view(cur)

    df = helper.get_sql_data("""SELECT * FROM revenue_hourly2 LIMIT 10;""", engine)

    # Close communication with the PostgreSQL database server
    cur.close()
    # Commit the changes
    conn.commit()




    # Test queries
    # Transform JSON Watson conversations response to query parameters object
    # nl_query = 'daily revenue '
    # df = get_intent_entity_from_watson(nl_query)
    #
    # print df.head()
