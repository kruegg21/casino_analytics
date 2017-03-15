import psycopg2

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
        list of strings of column names of database
    '''
    cur.execute("""SELECT column_name FROM information_schema.columns
                    WHERE table_name = '{}';""".format(DATABASE_TABLE))
    rows = cur.fetchall()
    return [element[0] for element in rows]


def make_materialized_view(cur):
    cur.execute("""CREATE MATERIALIZED VIEW revenue_hourly AS
                   SELECT SUM(amountbet - amountwon) AS revenue, date_trunc('hour', logs.tmstmp) AS hour FROM logs GROUP BY 2;""")


if __name__ == "__main__":
    # Connect to database
    try:
        conn = psycopg2.connect(
            "dbname='{}' user='{}'".format(DATABASE_NAME, DATABASE_USER))
    except:
        print "I am unable to connect to the database."
    cur = conn.cursor()

    get_database_schema(cur)
    make_materialized_view(cur)

    # close communication with the PostgreSQL database server
    cur.close()
    # commit the changes
    conn.commit()
