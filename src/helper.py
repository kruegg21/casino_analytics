from sqlalchemy import create_engine
import pandas as pd
import time

# Timing function
def timeit(method):
    """
    Timing wrapper
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print 'Running %r took %2.4f sec\n' % \
              (method.__name__, te-ts)
        return result
    return timed

@timeit
def connect_to_database(user, domain, name):
    engine = create_engine('postgresql://{}:{}/{}'.format(user, domain, name))
    return engine

@timeit
def get_sql_data(query, engine):
    return pd.read_sql_query(query, con = engine)
